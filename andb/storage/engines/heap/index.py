import struct

# Constants for struct packing/unpacking
INT_FORMAT = 'i'
LONG_FORMAT = 'q'

PAGE_FREE_SPACE_FORMAT = '<I'
PAGE_TUPLE_COUNT_FORMAT = '<I'

# Constants for indexing
INDEX_PAGE_SIZE = 4096
INDEX_NODE_HEADER_SIZE = 12
INDEX_LEAF_HEADER_SIZE = 8
INDEX_KEY_FORMAT = 'i'
INDEX_POINTER_FORMAT = 'q'
INDEX_KEY_SIZE = struct.calcsize(INDEX_KEY_FORMAT)
INDEX_POINTER_SIZE = struct.calcsize(INDEX_POINTER_FORMAT)
INDEX_NODE_MAX_KEYS = (INDEX_PAGE_SIZE - INDEX_NODE_HEADER_SIZE) // (INDEX_KEY_SIZE + INDEX_POINTER_SIZE)
INDEX_LEAF_MAX_KEYS = (INDEX_PAGE_SIZE - INDEX_LEAF_HEADER_SIZE) // (INDEX_KEY_SIZE + INDEX_POINTER_SIZE)

# Constants
MID_KEY_INDEX = 1
MIN_KEYS = 2
MAX_KEYS = 4


class IndexPage:
    def __init__(self, page_id, parent_page_id, is_leaf=False):
        self.page_id = page_id
        self.parent_page_id = parent_page_id
        self.is_leaf = is_leaf
        self.keys = []
        self.pointers = []

    def add_key_pointer(self, key, pointer):
        self.keys.append(key)
        self.pointers.append(pointer)

    def to_bytes(self):
        data = struct.pack(INT_FORMAT, self.page_id)
        data += struct.pack(INT_FORMAT, self.parent_page_id)
        data += struct.pack(INT_FORMAT, 1 if self.is_leaf else 0)
        data += struct.pack(PAGE_FREE_SPACE_FORMAT, INDEX_PAGE_SIZE - len(data))
        for i in range(len(self.keys)):
            data += struct.pack(INDEX_KEY_FORMAT, self.keys[i])
            data += struct.pack(INDEX_POINTER_FORMAT, self.pointers[i])
        return data

    @classmethod
    def from_bytes(cls, data):
        page_id, parent_page_id, is_leaf, free_space = struct.unpack_from(
            f'{INT_FORMAT}{INT_FORMAT}{INT_FORMAT}{PAGE_FREE_SPACE_FORMAT}', data)
        page = cls(page_id, parent_page_id, True if is_leaf == 1 else False)
        offset = INDEX_NODE_HEADER_SIZE if is_leaf == 0 else INDEX_LEAF_HEADER_SIZE
        while offset < INDEX_PAGE_SIZE - free_space:
            key, pointer = struct.unpack_from(f'{INDEX_KEY_FORMAT}{INDEX_POINTER_FORMAT}', data, offset)
            page.add_key_pointer(key, pointer)
            offset += INDEX_KEY_SIZE + INDEX_POINTER_SIZE
        return page


class BPlusTreeIndex:
    def __init__(self):
        self.root = None

    def insert(self, key, page_id, slot_offset):
        if self.root is None:
            self.root = BPlusTreeNode(is_leaf=True)
        self.root.insert(key, page_id, slot_offset)

    def search(self, key):
        if self.root is None:
            return None, None
        return self.root.search(key)

    def delete(self, key):
        if self.root is None:
            return
        self.root.delete(key)


class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.keys = []
        self.children = []
        self.is_leaf = is_leaf

    def insert(self, key, page_id, slot_offset):
        if self.is_leaf:
            self._insert_in_leaf(key, page_id, slot_offset)
        else:
            index = self._find_insert_index(key)
            child = self.children[index]
            child.insert(key, page_id, slot_offset)
            if child.is_overflow():
                self._split_child(index)

    def search(self, key):
        if self.is_leaf:
            return self._search_in_leaf(key)
        else:
            index = self._find_search_index(key)
            child = self.children[index]
            return child.search(key)

    def delete(self, key):
        if self.is_leaf:
            self._delete_in_leaf(key)
        else:
            index = self._find_search_index(key)
            child = self.children[index]
            child.delete(key)
            if child.is_underflow():
                self._balance_child(index)

    def is_overflow(self):
        return len(self.keys) == MAX_KEYS

    def is_underflow(self):
        return len(self.keys) < MIN_KEYS

    def _insert_in_leaf(self, key, page_id, slot_offset):
        index = self._find_insert_index(key)
        self.keys.insert(index, key)
        self.children.insert(index, (page_id, slot_offset))

    def _search_in_leaf(self, key):
        index = self._find_search_index(key)
        if index < len(self.keys) and self.keys[index] == key:
            return self.children[index]
        return None, None

    def _delete_in_leaf(self, key):
        index = self._find_search_index(key)
        if index < len(self.keys) and self.keys[index] == key:
            del self.keys[index]
            del self.children[index]

    def _split_child(self, index):
        child = self.children[index]
        new_child = BPlusTreeNode(is_leaf=child.is_leaf)
        self.keys.insert(index, child.keys[MID_KEY_INDEX])
        self.children.insert(index + 1, new_child)
        new_child.keys = child.keys[MID_KEY_INDEX + 1:]
        child.keys = child.keys[:MID_KEY_INDEX + 1]
        if not child.is_leaf:
            new_child.children = child.children[MID_KEY_INDEX + 1:]
            child.children = child.children[:MID_KEY_INDEX + 1]

    def _balance_child(self, index):
        if index > 0:
            # Try borrowing from the left sibling
            left_sibling = self.children[index - 1]
            if len(left_sibling.keys) > MIN_KEYS:
                self._borrow_from_left_sibling(index)
                return

        if index < len(self.children) - 1:
            # Try borrowing from the right sibling
            right_sibling = self.children[index + 1]
            if len(right_sibling.keys) > MIN_KEYS:
                self._borrow_from_right_sibling(index)
                return

        # Merge with siblings
        if index > 0:
            # Merge with left sibling
            self._merge_with_left_sibling(index)
        else:
            # Merge with right sibling
            self._merge_with_right_sibling(index)

    def _borrow_from_left_sibling(self, index):
        child = self.children[index]
        left_sibling = self.children[index - 1]

        child.keys.insert(0, self.keys[index - 1])
        self.keys[index - 1] = left_sibling.keys.pop()
        if not child.is_leaf:
            child.children.insert(0, left_sibling.children.pop())

    def _borrow_from_right_sibling(self, index):
        child = self.children[index]
        right_sibling = self.children[index + 1]

        child.keys.append(self.keys[index])
        self.keys[index] = right_sibling.keys.pop(0)
        if not child.is_leaf:
            child.children.append(right_sibling.children.pop(0))

    def _merge_with_left_sibling(self, index):
        child = self.children[index]
        left_sibling = self.children[index - 1]

        left_sibling.keys.append(self.keys.pop(index - 1))
        left_sibling.keys.extend(child.keys)
        if not child.is_leaf:
            left_sibling.children.extend(child.children)

        del self.children[index]

    def _merge_with_right_sibling(self, index):
        child = self.children[index]
        right_sibling = self.children[index + 1]

        child.keys.append(self.keys.pop(index))
        child.keys.extend(right_sibling.keys)
        if not child.is_leaf:
            child.children.extend(right_sibling.children)

        del self.children[index + 1]

    def _find_insert_index(self, key):
        index = 0
        while index < len(self.keys) and key > self.keys[index]:
            index += 1
        return index

    def _find_search_index(self, key):
        index = 0
        while index < len(self.keys) and key >= self.keys[index]:
            index += 1
        return index - 1


class IndexManager:
    def __init__(self):
        self.page_directory = {}

    def create_index(self, index_name):
        # TODO: Implement the creation of an index
        pass

    def drop_index(self, index_name):
        # TODO: Implement the dropping of an index
        pass

    def search(self, index_name, key):
        # TODO: Implement the search operation in the index
        pass

    def insert(self, index_name, key, pointer):
        # TODO: Implement the insertion operation in the index
        pass

    def delete(self, index_name, key):
        # TODO: Implement the deletion operation in the index
        pass
