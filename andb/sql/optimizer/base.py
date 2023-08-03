
class Pattern:
    chain = ()


class BaseTransformation:

    @staticmethod
    def match(ast) -> bool:
        pass

    @staticmethod
    def on_transform(ast):
        pass


class BaseImplementation:
    @classmethod
    def get_pattern(cls):
        pass

    @classmethod
    def match(cls, operator) -> bool:
        pass

    @classmethod
    def on_implement(cls, old_operator):
        pass


