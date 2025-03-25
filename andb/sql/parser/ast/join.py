from .base import ASTNode


class JoinType:
    LEFT_JOIN = 'LEFT JOIN'
    RIGHT_JOIN = 'RIGHT JOIN'
    INNER_JOIN = 'INNER JOIN'
    FULL_JOIN = 'FULL JOIN'
    CROSS_JOIN = 'CROSS JOIN'
    OUTER_JOIN = 'OUTER JOIN'


class Join(ASTNode):
    def __init__(self, left, right, join_type, condition=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.join_type = join_type
        self.condition = condition
        self.implicit = kwargs.get('implicit', False)

class BinaryTreeNode:
    def __init__(self):
        self.left = None
        self.right = None
        self.value = None
