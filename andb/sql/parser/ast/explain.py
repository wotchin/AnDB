from .base import ASTNode


class Explain(ASTNode):
    def __init__(self, target,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target = target

