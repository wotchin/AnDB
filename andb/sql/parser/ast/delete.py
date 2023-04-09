from .base import ASTNode


class Delete(ASTNode):
    def __init__(self, table, where):
        super().__init__()
        self.table = table
        self.where = where

