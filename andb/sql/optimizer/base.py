
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
        # chain = cls.get_pattern().chain
        #
        # def dfs(i, node):
        #     if node is None:
        #         return False
        #
        #     if i == len(chain):
        #         return True
        #
        #     if not isinstance(node, chain[i]):
        #         return False
        #
        #     result = False
        #     for child in operator.children:
        #         result = result or dfs(i + 1, child)
        #     return result
        #
        # return dfs(0, operator)

    @classmethod
    def on_implement(cls, old_operator):
        pass


