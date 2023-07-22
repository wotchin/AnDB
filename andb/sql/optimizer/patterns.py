from andb.sql.parser.ast.create import CreateTable, CreateIndex
from andb.executor.operator.logical import *

from .base import Pattern


# class CreateIndexPattern(Pattern):
#     chain = (CreateIndex,)
#
#
# class CreateTablePattern(Pattern):
#     chain = (CreateTable,)


class UtilityPattern(Pattern):
    chain = (UtilityOperator,)
