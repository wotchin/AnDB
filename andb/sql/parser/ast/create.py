from .base import ASTNode


class CreateTable(ASTNode):
    def __init__(self, name, columns,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.columns = columns


class CreateIndex(ASTNode):
    def __init__(self, name, table_name, columns, index_type=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.table_name = table_name
        self.columns = columns
        self.index_type = index_type
