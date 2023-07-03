from .base import ASTNode


class Select(ASTNode):
    def __init__(self, targets, distinct=False, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.targets = targets
        self.distinct = distinct
        self.from_table = None
        self.where = None
        self.group_by = None
        self.having = None
        self.order_by = None
        self.limit = None
        self.offset = None
