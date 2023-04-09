from .base import ASTNode


class Union(ASTNode):
    def __init__(self, left, right, unique=True,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.left = left
        self.right = right
        self.unique = unique

        if self.alias:
            self.parentheses = True

