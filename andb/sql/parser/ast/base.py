

class ASTNode:
    def __init__(self, alias=None, parentheses=False, **kwargs):
        self.alias = alias
        self.parentheses = parentheses

    def __repr__(self):
        hidden_fields = ('alias', 'parentheses')
        fields = []
        for k, v in self.__dict__.items():
            if k.startswith('_') or k in hidden_fields:
                continue
            fields.append(f'{k}={v}')
        fields = ' '.join(fields)
        if fields:
            return f'<{self.__class__.__name__} {fields}>'
        return f'<{self.__class__.__name__}>'
