from .base import ASTNode
from andb.errno.errors import InitializationStageError

class Identifier(ASTNode):
    def __init__(self, parts=None, ):
        super().__init__()
        self.parts = parts
        self.items = parts.split('.')
        if len(self.items) > 2:
            raise InitializationStageError(f"syntax error: '{parts}'.")
        if len(self.items) == 1:
            self.items.insert(0, None)
