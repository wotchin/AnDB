from .base import ASTNode


class Insert(ASTNode):
    def __init__(self, table, columns, from_select=None, values=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.table = table
        self.columns = columns

        self.from_select = from_select
        self.values = values


