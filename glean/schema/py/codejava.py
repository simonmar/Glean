# @generated
# To regenerate this file run fbcode//glean/schema/gen/sync
from typing import Optional, Tuple, Union, List, Dict, TypeVar
from thrift.py3 import Struct
from enum import Enum
import ast
from glean.schema.py.glean_schema_predicate import GleanSchemaPredicate, angle_for, R, Just, InnerGleanSchemaPredicate
from glean.schema.py.java import *


from glean.schema.code_java.types import (
    Annotations,
    Entity,
)




class CodeJavaAnnotations(InnerGleanSchemaPredicate):
  @staticmethod
  def build_angle(__env: Dict[str, R], annotations: ast.Expr) -> Tuple[str, Struct]:
    return f"code.java.Annotations.5 {{ { ', '.join(filter(lambda x: x != '', [angle_for(__env, annotations, 'annotations')])) or '_' } }}", Annotations

  @staticmethod
  def angle_query_annotations(*, annotations: Optional[List["JavaAnnotation"]] = None) -> "CodeJavaAnnotations":
    raise Exception("this function can only be called from @angle_query")




class CodeJavaEntity(InnerGleanSchemaPredicate):
  @staticmethod
  def build_angle(__env: Dict[str, R], class_: ast.Expr, definition_: ast.Expr) -> Tuple[str, Struct]:
    return f"code.java.Entity.5 {{ { ', '.join(filter(lambda x: x != '', [angle_for(__env, class_, 'class_'), angle_for(__env, definition_, 'definition_')])) or '_' } }}", Entity

  @staticmethod
  def angle_query_class_(*, class_: Optional["JavaClassDeclaration"] = None) -> "CodeJavaEntity":
    raise Exception("this function can only be called from @angle_query")

  @staticmethod
  def angle_query_definition_(*, definition_: Optional["JavaDefinition"] = None) -> "CodeJavaEntity":
    raise Exception("this function can only be called from @angle_query")





