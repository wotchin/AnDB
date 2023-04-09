from .base import ASTNode


class Identifier(ASTNode):
    def __init__(self, parts=None, ):
        super().__init__()
        self.parts = parts

