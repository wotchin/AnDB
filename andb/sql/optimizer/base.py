
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
    def __init__(self):
        pass

    def get_pattern(self):
        pass

    def match(self, operator) -> bool:
        chain = self.get_pattern().chain

        def dfs(i, node):
            if node is None:
                return False

            if i == len(chain):
                return True

            if not isinstance(node, chain[i]):
                return False

            result = False
            for child in operator.children:
                result = result or dfs(i + 1, child)
            return result

        return dfs(0, operator)

    def on_implement(self, old_operator):
        pass


