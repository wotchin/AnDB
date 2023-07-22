from .base import BaseImplementation
from .patterns import *


class UtilityImplementation(BaseImplementation):
    def get_pattern(self):
        return UtilityPattern

    def match(self, operator) -> bool:
        return isinstance(operator, self.get_pattern().chain[0])

    def on_implement(self, old_operator):
        return old_operator.physical_operator


_all_implementations = [impl() for impl in BaseImplementation.__subclasses__()]


def andb_logical_plan_implement(logical_plan):
    for impl in _all_implementations:
        if impl.match(logical_plan):
            return impl.on_implement(logical_plan)
    return logical_plan
