from .base import ASTNode


class Insert(ASTNode):
    def __init__(self, table, columns, from_values=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.table = table
        self.columns = columns

        self.from_values = from_values


