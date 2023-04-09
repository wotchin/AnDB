from .base import ASTNode


class Tuple(ASTNode):
    def __init__(self, items):
        super().__init__()
        self.items = items


class Star(ASTNode):
    def __init__(self):
        super().__init__()


class Constant(ASTNode):
    def __init__(self, value):
        super().__init__()
        self.value = value

