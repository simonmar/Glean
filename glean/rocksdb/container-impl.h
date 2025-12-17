/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "glean/rocksdb/rocksdb.h"
#include "glean/rocksdb/util.h"

namespace facebook {
namespace glean {
namespace rocks {
namespace impl {

struct Family {
 private:
  Family(const char* n, unsigned int flags_, bool keep_ = true)
      : index(families.size()),
        name(n),
        flags(flags_),
        keep(keep_) {
    families.push_back(this);
  }

  Family(const Family&) = delete;
  Family& operator=(const Family&) = delete;

  static std::vector<const Family*> families;

 public:
  size_t index;
  const char* name;
  unsigned int flags;

  // Whether to keep this column family after the DB is complete. If
  // keep = false, then the contents of the column family will be
  // deleted before compaction.
  bool keep = true;

  static const Family admin;
  static const Family entities;
  static const Family keys;
  static const Family stats;
  static const Family meta;
  static const Family ownershipUnits;
  static const Family ownershipUnitIds;
  static const Family ownershipRaw;
  static const Family ownershipDerivedRaw;
  static const Family ownershipSets;
  static const Family factOwners;
  static const Family factOwnerPages;

  static size_t count() {
    return families.size();
  }

  static const Family* FOLLY_NULLABLE family(const std::string& name) {
    for (auto p : families) {
      if (name == p->name) {
        return p;
      }
    }
    return nullptr;
  }

  static const Family* FOLLY_NULLABLE family(size_t i) {
    return i < families.size() ? families[i] : nullptr;
  }
};

struct Cursor {
    Cursor() : cursor(nullptr, &mdb_cursor_close) {}
    Cursor(MDB_txn *txn, MDB_dbi dbi) :
        cursor(nullptr, &mdb_cursor_close) {
      MDB_cursor *c;
      check(mdb_cursor_open(txn, dbi, &c));
      cursor.reset(c);
    }

    bool seek_first() { return seek_op(MDB_FIRST); }

    bool seek_last() { return seek_op(MDB_LAST); }

    bool seek_op(MDB_cursor_op op) {
      int s = mdb_cursor_get(ptr(), &key_, &value_, op);
      if (s == MDB_SUCCESS) {
          // needed?
          check(mdb_cursor_get(ptr(), &key_, &value_, MDB_GET_CURRENT));
          valid_ = true;
      }
      else if (s == MDB_NOTFOUND) {
          valid_ = false;
      }
      else {
          check(s);
      }
      return valid_;
    }

    bool seek_key(MDB_val key) {
      key_ = key;
      int s = mdb_cursor_get(ptr(), &key_, &value_, MDB_SET_RANGE);
      if (s == MDB_SUCCESS) {
          // needed?
          check(mdb_cursor_get(ptr(), &key_, &value_, MDB_GET_CURRENT));
          valid_ = true;
      }
      else if (s == MDB_NOTFOUND) {
          valid_ = false;
      }
      else {
          check(s);
      }
      return valid_;
    }

    bool next() { return seek_op(MDB_NEXT); }

    bool valid() { return valid_; }

    MDB_val& key() { return key_; }
    MDB_val& value() { return value_; }

    MDB_cursor *ptr() {
        return cursor.get();
    }

  private:
    std::unique_ptr<MDB_cursor, decltype(&mdb_cursor_close)> cursor;
    MDB_val key_, value_;
    bool valid_;

};

struct Txn {
    Txn(MDB_env *env, unsigned int flags = 0) : txn(nullptr, &mdb_txn_abort) {
        MDB_txn* t;
        check(mdb_txn_begin(env, NULL, flags, &t));
        txn.reset(t);
    }

    bool get(MDB_dbi db, MDB_val k, MDB_val& v) {
        int s = mdb_get(txn.get(), db, &k, &v);
        if (s == MDB_SUCCESS) {
            return true;
        } else if (s == MDB_NOTFOUND) {
            return false;
        } else {
            check(s);
            return false;
        }
    }

    void put(MDB_dbi db, MDB_val k, MDB_val v) {
        check(mdb_put(txn.get(), db, &k, &v, 0));
    }

    Cursor cursor(MDB_dbi dbi) {
        return Cursor(txn.get(), dbi);
    }

    MDB_txn* ptr() {
        return txn.get();
    }

    void commit() {
        if (txn) {
            check(mdb_txn_commit(txn.release()));
        }
    }

  private:
    std::unique_ptr<MDB_txn, decltype(&mdb_txn_abort)> txn;
};

struct ContainerImpl final : Container {
  Mode mode;
  std::unique_ptr<MDB_env, decltype(&mdb_env_close)> db;
  std::vector<MDB_dbi> families;

  ContainerImpl(
      const std::string& path,
      Mode m,
      bool cache_index_and_filter_blocks,
      folly::Optional<std::shared_ptr<Cache>> cache);

  ContainerImpl(const ContainerImpl&) = delete;
  ContainerImpl(ContainerImpl&& other) = default;
  ContainerImpl& operator=(const ContainerImpl&) = delete;
  ContainerImpl& operator=(ContainerImpl&&) = delete;

  ~ContainerImpl() override {
    close();
  }

  void close() noexcept override;

  void requireOpen() const;

  void backup(const std::string& path) override;
  std::unique_ptr<Database>
      openDatabase(Id start, rts::UsetId first_unit_id, int32_t version) &&
      override;

  void writeData(folly::ByteRange key, folly::ByteRange value) override;

  bool readData(folly::ByteRange key, std::function<void(folly::ByteRange)> f)
      override;

  void optimize(bool compact) override;

  MDB_dbi family(const Family& family) const;

  Txn txn_write() {
      return Txn(db.get());
  };

  Txn txn_read() {
      return Txn(db.get(), MDB_RDONLY);
  };
};

} // namespace impl
} // namespace rocks
} // namespace glean
} // namespace facebook
