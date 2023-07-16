from andb.common.replacement.base import BaseCache
from andb.errno.errors import BufferOverflow


class Node:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None
        self.pinned = False


class LRUCache(BaseCache):
    def __init__(self, capacity):
        super().__init__(capacity)
        self.cache = {}
        self.evicted = []
        # dummy nodes
        self.head = Node(None, None)
        self.tail = Node(None, None)
        self.head.next = self.tail
        self.tail.prev = self.head

    def clear(self):
        self.cache.clear()
        self.evicted.clear()
        node = self.head.next
        while node is not self.tail:
            self._remove(node)
            node = node.next

    def get(self, key):
        if key in self.cache:
            node = self.cache[key]
            self._remove(node)
            self._add(node)
            return node.value
        return None

    def put(self, key, value, pinned=False):
        if key in self.cache:
            self._remove(self.cache[key])
        node = Node(key, value)
        node.pinned = pinned
        self.cache[key] = node
        self._add(node)
        if len(self.cache) > self.capacity:
            node = self.head.next
            while node and node.pinned:
                node = node.next
            if node is self.tail:
                # remove inserted one
                self._remove(self.cache[key])
                del self.cache[key]
                raise BufferOverflow('All buffers are pinned, no room to put.')
            self._remove(node)
            # add to evicted list
            self.evicted.append(node)
            del self.cache[node.key]

    def pop(self, key):
        if key in self.cache:
            node = self.cache[key]
            self._remove(node)
            del self.cache[key]
            return node.value
        return None

    def pin(self, key):
        if key not in self.cache:
            return
        self.cache[key].pinned = True

    def unpin(self, key):
        if key not in self.cache:
            return
        self.cache[key].pinned = False

    def get_evicted_list(self):
        return self.evicted

    def _remove(self, node):
        prev_node = node.prev
        next_node = node.next
        prev_node.next = next_node
        next_node.prev = prev_node

    def _add(self, node):
        prev_node = self.tail.prev
        prev_node.next = node
        node.prev = prev_node
        node.next = self.tail
        self.tail.prev = node

    def items(self):
        node = self.head.next
        while node is not self.tail:
            yield node.value
            node = node.next

    def keys(self):
        return self.cache.keys()

    def __iter__(self):
        return self.items()
