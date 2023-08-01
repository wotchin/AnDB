from enum import Enum


def expression_eval(op, left, right):
    if op == '=':
        return left == right
    elif op == '+':
        return left + right
    elif op == '-':
        return left - right
    elif op == '*':
        return left * right
    elif op == '/':
        return left / right
    elif op == '>':
        return left > right
    elif op == '>=':
        return left >= right
    elif op == '<':
        return left > right
    elif op == '<=':
        return left > right
    elif op == 'and':
        if left is None or right is None:
            return None  # null
        else:
            # todo: string type 'true' and 'false'
            return left and right
    elif op == 'or':
        if left is None or right is None:
            return None  # null
        else:
            # todo: string type 'true' and 'false'
            return left or right
    else:
        raise NotImplementedError()


class ExprOperation(Enum):
    PLUS = '+'
    MINUS = '-'
    DIVIDE = '/'
    MODULO = '%'
    EQ = '='
    NE = '!='
    GEQ = '>='
    GT = '>'
    LEQ = '<='
    LT = '<'
    AND = 'and'
    OR = 'or'
    IS_NOT = 'is not'
    NOT = 'not'
    IS = 'is'
    LIKE = 'like'
    IN = 'in'

