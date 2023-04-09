from .base import ASTNode


class Select(ASTNode):
    def __init__(self, targets, distinct=False, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.targets = targets
        self.distinct = distinct

