from abc import ABCMeta, abstractmethod


class BaseCache(metaclass=ABCMeta):
    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def put(self, key, value, pinned):
        pass

    @staticmethod
    def pop(self, key):
        pass

    @abstractmethod
    def pin(self, key):
        pass

    @abstractmethod
    def unpin(self, key):
        pass

    @abstractmethod
    def get_evicted_list(self):
        pass

    def __init__(self, capacity):
        self.capacity = capacity
