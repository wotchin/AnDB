from .base import ASTNode


class Update(ASTNode):
    def __init__(self, table, columns, where):
        super().__init__()
        self.table = table
        # dict type: key is column name, value is set value
        self.columns = columns
        self.where = where
