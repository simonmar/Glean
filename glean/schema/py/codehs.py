# @generated
# To regenerate this file run fbcode//glean/schema/gen/sync
from typing import Optional, Tuple, Union, List, Dict, TypeVar
from thrift.py3 import Struct
from enum import Enum
import ast
from glean.schema.py.glean_schema_predicate import GleanSchemaPredicate, angle_for, R, Just, InnerGleanSchemaPredicate
from glean.schema.py.hs import *


from glean.schema.code_hs.types import (
    Entity,
)




class CodeHsEntity(InnerGleanSchemaPredicate):
  @staticmethod
  def build_angle(__env: Dict[str, R], definition: ast.Expr, function_: ast.Expr, class_: ast.Expr) -> Tuple[str, Struct]:
    return f"code.hs.Entity.2 {{ { ', '.join(filter(lambda x: x != '', [angle_for(__env, definition, 'definition'), angle_for(__env, function_, 'function_'), angle_for(__env, class_, 'class_')])) or '_' } }}", Entity

  @staticmethod
  def angle_query_definition(*, definition: Optional["HsDefinition"] = None) -> "CodeHsEntity":
    raise Exception("this function can only be called from @angle_query")

  @staticmethod
  def angle_query_function_(*, function_: Optional["HsFunctionDefinition"] = None) -> "CodeHsEntity":
    raise Exception("this function can only be called from @angle_query")

  @staticmethod
  def angle_query_class_(*, class_: Optional["HsClass"] = None) -> "CodeHsEntity":
    raise Exception("this function can only be called from @angle_query")





