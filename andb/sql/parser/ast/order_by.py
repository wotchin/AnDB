from .base import ASTNode


class OrderBy(ASTNode):
    def __init__(self, column, direction, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.attr = column
        self.direction = direction
