/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "glean/rocksdb/database-impl.h"
#include "glean/rocksdb/container-impl.h"

#include "glean/rts/timer.h"

namespace facebook {
namespace glean {
namespace rocks {
namespace impl {

using namespace rts;

const char* admin_names[] = {
    "NEXT_ID",
    "VERSION",
    "STARTING_ID",
    "FIRST_UNIT_ID",
    "NEXT_UNIT_ID",
    "ORPHAN_FACTS",
};

namespace {

template <typename T, typename F>
T initAdminValue(
    ContainerImpl& container_,
    AdminId id,
    T def,
    bool write,
    F&& notFound) {
  auto current = readAdminValue<T>(container_, id);
  if (current.hasValue()) {
    return *current;
  } else {
    notFound();
    if (write) {
      binary::Output key;
      key.fixed(id);
      binary::Output value;
      value.fixed(def);
      Txn txn = container_.txn_write();
      txn.put(
          container_.family(Family::admin),
          slice(key),
          slice(value));
      txn.commit();
    }
    return def;
  }
}

} // namespace

DatabaseImpl::DatabaseImpl(
    ContainerImpl c,
    Id start,
    UsetId first_unit_id_,
    int64_t version)
    : container_(std::move(c)) {
  starting_id = Id::fromWord(initAdminValue(
      container_,
      AdminId::STARTING_ID,
      start.toWord(),
      container_.mode == Mode::Create,
      [] {}));

  next_id = Id::fromWord(initAdminValue(
      container_,
      AdminId::NEXT_ID,
      start.toWord(),
      container_.mode == Mode::Create,
      [mode = container_.mode] {
        if (mode != Mode::Create) {
          rts::error("corrupt database - missing NEXT_ID");
        }
      }));

  first_unit_id = initAdminValue(
      container_,
      AdminId::FIRST_UNIT_ID,
      first_unit_id_,
      container_.mode == Mode::Create,
      [] {
        // TODO: later this should be an error, for now we have to be
        // able to open old DBs.
      });
  VLOG(1) << folly::sformat("first_unit_id: {}", first_unit_id);

  next_uset_id = initAdminValue(
      container_,
      AdminId::NEXT_UNIT_ID,
      first_unit_id,
      container_.mode == Mode::Create,
      [] {
        // TODO: later this should be an error, for now we have to be
        // able to open old DBs.
      });
  VLOG(1) << folly::sformat("next_uset_id: {}", next_uset_id);

  db_version = initAdminValue(
      container_,
      AdminId::VERSION,
      version,
      container_.mode == Mode::Create,
      [] {});

  if (db_version != version) {
    rts::error("unexpected database version {}", db_version);
  }

  stats_.set(loadStats());

  if (container_.mode != Mode::ReadOnly) {
    // These are only used when writing
    ownership_unit_counters = loadOwnershipUnitCounters();
    ownership_derived_counters = loadOwnershipDerivedCounters();

    // We only need usets_ for writable DBs, and it takes time and
    // memory to load them so omit this for ReadOnly DBs.
    usets_ = loadOwnershipSets();
  }

  // Enable the fact owner cache when the DB is read-only
  if (container_.mode == Mode::ReadOnly) {
    cacheOwnership();
  }
}

rts::PredicateStats DatabaseImpl::loadStats() {
  container_.requireOpen();
  rts::PredicateStats stats;
  Txn txn = container_.txn_read();
  Cursor cur = txn.cursor(container_.family(Family::stats));

  for (cur.seek_first(); cur.valid(); cur.next()) {
    binary::Input key(byteRange(cur.key()));
    stats[key.fixed<Pid>()] = fromSlice<MemoryStats>(cur.value());
    assert(key.empty());
  }
  return stats;
}

Id DatabaseImpl::idByKey(Pid type, folly::ByteRange key) {
  if (count(type).high() == 0) {
    return Id::invalid();
  }

  container_.requireOpen();
  binary::Output k;
  k.fixed(type);
  k.put(key);
  Txn txn = container_.txn_read();
  MDB_val out;
  if (!txn.get(container_.family(Family::keys), slice(k), out)) {
    return Id::invalid();
  } else {
    binary::Input value = input(out);
    auto id = value.fixed<Id>();
    assert(value.empty());
    return id;
  }
}

Pid DatabaseImpl::typeById(Id id) {
  container_.requireOpen();
  MDB_val val;
  Txn txn = container_.txn_read();
  if (lookupById(txn, id, val)) {
    return input(val).packed<Pid>();
  } else {
    return Pid::invalid();
  }
}

namespace {

rts::Fact::Ref decomposeFact(Id id, const MDB_val& data) {
  auto inp = input(data);
  const auto ty = inp.packed<Pid>();
  const auto key_size = inp.packed<uint32_t>();
  return rts::Fact::Ref{id, ty, rts::Fact::Clause::from(inp.bytes(), key_size)};
}

} // namespace

bool DatabaseImpl::factById(Id id, std::function<void(Pid, Fact::Clause)> f) {
  container_.requireOpen();
  MDB_val val;
  Txn txn = container_.txn_read();
  if (lookupById(txn, id, val)) {
    auto ref = decomposeFact(id, val);
    f(ref.type, ref.clause);
    return true;
  } else {
    return false;
  }
}

bool DatabaseImpl::lookupById(Txn &txn, Id id, MDB_val& val) {
  if (id < startingId() || id >= firstFreeId()) {
    return false;
  }
  binary::Output key;
  key.nat(id.toWord());
  return txn.get(container_.family(Family::entities), slice(key), val);
}

namespace {

struct SeekIterator final : rts::FactIterator {
  SeekIterator(
      folly::ByteRange start,
      size_t prefix_size,
      Pid type,
      DatabaseImpl* db)
      : txn_(db->container_.txn_read()),
        iter_(txn_.cursor(db->container_.family(Family::keys))),
        upper_bound_(
            binary::lexicographicallyNext({start.data(), prefix_size})),
        type_(type),
        db_(db) {
    assert(prefix_size <= start.size());
    iter_.seek_key(slice(start));
  }

  void next() override {
    iter_.next();
  }

  Fact::Ref get(Demand demand) override {
    if (iter_.valid()) {
      if (memcmp(iter_.key().mv_data, upper_bound_.data(), upper_bound_.size()) >= 0) {
          return Fact::Ref::invalid();
      }
      auto key = input(iter_.key());
      [[maybe_unused]] auto ty = key.fixed<Pid>();
      assert(ty == type_);
      auto value = input(iter_.value());
      auto id = value.fixed<Id>();
      assert(value.empty());

      if (demand == KeyOnly) {
        return Fact::Ref{id, type_, Fact::Clause::fromKey(key.bytes())};
      } else {
        [[maybe_unused]] auto found = db_->lookupById(txn_, id, slice_);
        assert(found);
        return decomposeFact(id, slice_);
      }
    } else {
      return Fact::Ref::invalid();
    }
  }

  std::optional<Id> lower_bound() override {
    return std::nullopt;
  }
  std::optional<Id> upper_bound() override {
    return std::nullopt;
  }

  std::vector<unsigned char> upper_bound_;
  const Pid type_;
  Txn txn_;
  Cursor iter_;
  DatabaseImpl* db_;
  MDB_val slice_;
};

} // namespace

std::unique_ptr<rts::FactIterator>
DatabaseImpl::seek(Pid type, folly::ByteRange start, size_t prefix_size) {
  assert(prefix_size <= start.size());
  if (count(type).high() == 0) {
    return std::make_unique<EmptyIterator>();
  }

  container_.requireOpen();
  binary::Output out;
  out.fixed(type);
  const auto type_size = out.size();
  out.put(start);
  return std::make_unique<SeekIterator>(
      out.bytes(), type_size + prefix_size, type, this);
}

std::unique_ptr<rts::FactIterator> DatabaseImpl::seekWithinSection(
    Pid type,
    folly::ByteRange start,
    size_t prefix_size,
    Id from,
    Id upto) {
  if (upto <= startingId() || firstFreeId() <= from) {
    return std::make_unique<EmptyIterator>();
  }

  return Section(this, from, upto).seek(type, start, prefix_size);
}

namespace {

template<typename Direction>
struct EnumerateIterator final : rts::FactIterator {
  static std::vector<char> encode(Id id) {
    std::vector<char> v(rts::MAX_NAT_SIZE);
    const auto n =
        rts::storeNat(reinterpret_cast<unsigned char*>(v.data()), id.toWord());
    v.resize(n);
    return v;
  }

  explicit EnumerateIterator(Id start, Id bound, DatabaseImpl* db)
      : bound_(bound),
        txn_(db->container_.txn_read()),
        iter_(txn_.cursor(db->container_.family(Family::entities))) {
    auto st = encode(start);
    iter_.seek_key({st.size(), st.data()});
  }

  void next() override {
    iter_.seek_op(Direction::next);
  }

  Fact::Ref get(Demand /*unused*/) override {
    if (iter_.valid()) {
        Id id = Id::fromWord(loadTrustedNat(
                                 reinterpret_cast<const unsigned char*>(
                                   iter_.key().mv_data))
                               .first);
        if (Direction::inside(id, bound_)) {
            return decomposeFact(id,iter_.value());
        }
    }
    return Fact::Ref::invalid();
  }

  std::optional<Id> lower_bound() override {
    return std::nullopt;
  }
  std::optional<Id> upper_bound() override {
    return std::nullopt;
  }

  Id bound_;
  Txn txn_;
  Cursor iter_;
};

struct Forward {
  static std::pair<Id, Id>
  bounds(Id from, Id upto, Id starting_id, Id next_id) {
    if (from >= next_id || (upto && upto <= starting_id)) {
      return {Id::invalid(), Id::invalid()};
    } else {
      return {
          std::max(from, starting_id),
          upto && upto <= next_id ? upto : next_id};
    }
  }

  static bool inside(Id cur, Id bound) { return cur < bound; }
  static inline constexpr auto next = MDB_NEXT;
};

struct Backward {
  static std::pair<Id, Id>
  bounds(Id from, Id downto, Id starting_id, Id next_id) {
    if (downto >= next_id || (from && from <= starting_id)) {
      return {Id::invalid(), Id::invalid()};
    } else {
      return {
          (from && from <= next_id ? from : next_id) - 1,
          std::max(downto, starting_id)};
    }
  }

  static bool inside(Id cur, Id bound) { return cur > bound; }
  static inline constexpr auto next = MDB_PREV;
};

} // namespace

template <typename Direction>
std::unique_ptr<rts::FactIterator>
makeEnumerateIterator(DatabaseImpl* db, Id from, Id to) {
  db->container_.requireOpen();
  const auto [start, bound] =
      Direction::bounds(from, to, db->startingId(), db->firstFreeId());
  if (!start) {
    return std::make_unique<rts::EmptyIterator>();
  } else {
    return std::make_unique<EnumerateIterator<Direction>>(start, bound, db);
  }
}

std::unique_ptr<rts::FactIterator> DatabaseImpl::enumerate(Id from, Id upto) {
  return makeEnumerateIterator<Forward>(this, from, upto);
}

std::unique_ptr<rts::FactIterator> DatabaseImpl::enumerateBack(
    Id from,
    Id downto) {
  return makeEnumerateIterator<Backward>(this, from, downto);
}

void DatabaseImpl::commit(rts::FactSet& facts) {
  container_.requireOpen();

  if (facts.empty()) {
    return;
  }

  if (facts.startingId() < next_id) {
    rts::error(
        "batch inserted out of sequence ({} < {})",
        facts.startingId(),
        next_id);
  }

  Txn txn = container_.txn_write();

  // NOTE: We do *not* support concurrent writes so we don't need to protect
  // stats_ here because nothing should be able to replace it while we're
  // running
  const auto& old_stats = stats_.unprotected();
  PredicateStats new_stats(old_stats);

  for (auto iter = facts.enumerate(); auto fact = iter->get(); iter->next()) {
    assert(fact.id >= next_id);

    uint64_t mem = 0;
    auto put = [&](auto family, const auto& key, const auto& value) {
      txn.put(family, key, value);
      mem += key.size();
      mem += value.size();
    };

    {
      binary::Output k;
      k.nat(fact.id.toWord());
      binary::Output v;
      v.packed(fact.type);
      v.packed(fact.clause.key_size);
      v.put({fact.clause.data, fact.clause.size()});

      txn.put(container_.family(Family::entities), slice(k), slice(v));
    }

    {
      binary::Output k;
      k.fixed(fact.type);
      k.put(fact.key());
      binary::Output v;
      v.fixed(fact.id);

      txn.put(container_.family(Family::keys), slice(k), slice(v));
    }

    new_stats[fact.type] += MemoryStats::one(mem);
  }

  const auto first_free_id = facts.firstFreeId();
  auto tmp1 = AdminId::NEXT_ID;
  auto tmp2 = first_free_id;
  txn.put(
      container_.family(Family::admin),
      toSlice(tmp1),
      toSlice(tmp2));

  for (const auto& x : new_stats) {
    if (x.second != old_stats.get(x.first)) {
      auto tmp = x.first.toWord();
      txn.put(
          container_.family(Family::stats),
          toSlice(tmp),
          toSlice(x.second));
    }
  }

  txn.commit();
  next_id = first_free_id;

  stats_.set(std::move(new_stats));
}

} // namespace impl
} // namespace rocks
} // namespace glean
} // namespace facebook
