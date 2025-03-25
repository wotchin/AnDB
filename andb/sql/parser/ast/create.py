from andb.sql.parser.ast.drop import DropTable
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


class CreateMemoryTable(CreateTable):
    def __init__(self, name, columns, temporary=True, *args, **kwargs):
        super().__init__(name, columns, *args, **kwargs)
        self.temporary = temporary
        self.__dict__.update(kwargs)



class DropMemoryTable(DropTable):
    def __init__(self, name, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
