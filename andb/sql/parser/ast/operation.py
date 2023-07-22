from .base import ASTNode


class Operation(ASTNode):
    def __init__(self, op, args, *args_, **kwargs):
        super().__init__(*args_, **kwargs)

        self.op = ' '.join(op.lower().split())
        self.args = list(args)


class BinaryOperation(Operation):
    pass


class BetweenOperation(Operation):
    pass


class Function(Operation):
    pass
