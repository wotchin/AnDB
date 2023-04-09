from .base import ASTNode


class AlterTable(ASTNode):
    def __init__(self, target, arg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target = target
        self.arg = arg
