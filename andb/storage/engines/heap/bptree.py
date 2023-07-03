import abc

from andb.common.cstructure import CStructure, Integer4Field
from andb.storage.common.page import Page, INVALID_ITEM_ID, INVALID_BYTES, PAGE_SIZE, PageHeader
from andb.constants.strings import LITTLE_END

INDEX_PAGE_FLAG_LEAF = 0b01
INDEX_PAGE_FLAG_NOT_LEAF = 0b00

INVALID_PAGE_NO = 0xffffffff


class TuplePointer(CStructure):
    pageno = Integer4Field(unsigned=True)
    tid = Integer4Field(unsigned=True)

    def __init__(self, pageno=-1, tid=-1):
        self.pageno = pageno
        self.tid = tid

    def __repr__(self):
        return '<pageno: {}, tid: {}>'.format(self.pageno, self.tid)


class BPlusNode:
    STATE_NORMAL = 0
    STATE_UNLOADED = 1
    STATE_ALLOCATED = 2

    def __init__(self):
        self.lsn = 0
        self.keys = []
        self.children = []
        self._page_no = None
        self._state = self.STATE_NORMAL

    def get_pageno(self):
        assert self._page_no is not None
        return self._page_no

    def set_pageno(self, pageno):
        self._page_no = pageno

    def set_state(self, state):
        self._state = state

    @property
    def state(self):
        return self._state

    @abc.abstractmethod
    def pack(self):
        pass

    @abc.abstractmethod
    def load_factor(self):
        pass


class InternalNode(BPlusNode):
    def __init__(self):
        super().__init__()

    def pack(self):
        page = Page.allocate(lsn=self.lsn)
        page.header.flags = (self.get_pageno() << 1)
        page.header.flags |= INDEX_PAGE_FLAG_NOT_LEAF
        page.header.checksum = 0
        for i in range(0, len(self.keys)):
            # structure:
            # child pageno (4bytes), key (variable length)
            data = (int.to_bytes(self.children[i].get_pageno(), 4, LITTLE_END) +
                    self.keys[i])
            rv = page.insert(self.lsn, data)
            assert rv != INVALID_ITEM_ID
        # Since the number of children is always one more than the
        # number of keys, we have to traverse one more time.
        if len(self.children) == (len(self.keys) + 1):
            # avoid both empty
            data = int.to_bytes(self.children[-1].get_pageno(), 4, LITTLE_END)
            rv = page.insert(self.lsn, data)
            assert rv != INVALID_ITEM_ID
        return page.pack()

    @staticmethod
    def unpack(data) -> 'InternalNode':
        page = Page.unpack(data)
        assert (page.header.flags & INDEX_PAGE_FLAG_NOT_LEAF) == INDEX_PAGE_FLAG_NOT_LEAF
        pageno = page.header.flags >> 1
        node = InternalNode()
        node.set_pageno(pageno)
        node.lsn = page.header.lsn
        for i in range(len(page.item_ids)):
            data = page.select(i)
            assert data != INVALID_BYTES
            child_pageno = int.from_bytes(data[:4], LITTLE_END)
            key = data[4:]
            # Since the number of children is always one more than the
            # number of keys, we have to traverse one more time. But the last
            # key is empty.
            if len(key) > 0:
                node.keys.append(key)
            child_node = BPlusNode()
            child_node.set_pageno(child_pageno)
            child_node.set_state(BPlusNode.STATE_UNLOADED)

            # Delete the field keys since prevent caller from accessing this unloading node.
            # If caller iterates keys, it will raise an AttributeError.
            del child_node.keys

            node.children.append(child_node)

        return node

    def load_factor(self):
        total_size = PAGE_SIZE - PageHeader.size()
        used_size = 0
        child_field_size = 4
        for k in self.keys:
            used_size += len(k) + child_field_size
        return used_size / total_size


class LeafNode(BPlusNode):
    def __init__(self):
        super().__init__()
        self.key_value_pairs = []
        self.next_leaf = None
        self.high_key = float('inf')  # Initialize high key as positive infinity

    def pack(self):
        page = Page.allocate(lsn=self.lsn)
        page.header.flags = (self.get_pageno() << 1)
        page.header.flags |= INDEX_PAGE_FLAG_LEAF
        page.header.checksum = 0
        if self.next_leaf:
            page.header.reserved = self.next_leaf.get_pageno()
        for i in range(0, len(self.keys)):
            # structure:
            # value_length (4bytes), value (variable length), key (variable length)
            value_data = bytearray()
            for pointer in self.key_value_pairs[i]:
                value_data += pointer.pack()
            data = (int.to_bytes(len(value_data), 4, LITTLE_END) +
                    value_data +
                    self.keys[i])

            rv = page.insert(self.lsn, data)
            assert rv != INVALID_ITEM_ID
        return page.pack()

    @staticmethod
    def unpack(data) -> 'LeafNode':
        page = Page.unpack(data)
        assert (page.header.flags & INDEX_PAGE_FLAG_LEAF) == INDEX_PAGE_FLAG_LEAF
        pageno = page.header.flags >> 1
        node = LeafNode()
        node.set_pageno(pageno)
        node.lsn = page.header.lsn
        if page.header.reserved != INVALID_PAGE_NO:
            node.next_leaf = LeafNode()
            node.next_leaf.set_state(LeafNode.STATE_UNLOADED)
            node.next_leaf.set_pageno(page.header.reserved)

        tuple_point_size = TuplePointer.size()
        for i in range(len(page.item_ids)):
            data = page.select(i)
            assert data != INVALID_BYTES
            value_length = int.from_bytes(data[:4], LITTLE_END)
            value_data = data[4: 4 + value_length]
            key = data[4 + value_length:]
            node.keys.append(key)
            values = []
            for j in range(len(value_data) // tuple_point_size):
                p = TuplePointer()
                p.unpack(value_data[(j * tuple_point_size): ((j + 1) * tuple_point_size)])
                values.append(p)
            node.key_value_pairs.append(values)
        return node

    def load_factor(self):
        total_size = PAGE_SIZE - PageHeader.size()
        used_size = 0
        value_length_field_size = 4
        for i, k in enumerate(self.keys):
            used_size += len(k) + value_length_field_size
            used_size += len(self.key_value_pairs[i]) * TuplePointer.size()
        return used_size / total_size



def create_node(serialized_node):
    # Get header first, then determine this node is leaf or not.
    header = PageHeader()
    header.unpack(serialized_node[:header.size()])
    if (header.flags & INDEX_PAGE_FLAG_LEAF) == INDEX_PAGE_FLAG_LEAF:
        node = LeafNode.unpack(serialized_node)
    else:
        node = InternalNode.unpack(serialized_node)
    return node


class BPlusTree:
    def __init__(self, root_node=None):
        self._next_pageno = 0
        if root_node:
            self.root = root_node
        else:
            self.root = self._allocate_node(is_leaf=True)

    def insert(self, lsn, key, value):
        node = self._find_leaf_node(key)
        node.lsn = lsn
        index = self._find_index(node, key)
        if key in node.keys:
            # Key already exists, append the value to the existing key
            # todo: detect if we can hold so many values
            node.key_value_pairs[index].append(value)
        else:
            node.keys.insert(index, key)
            node.key_value_pairs.insert(index, [value])
            if self._need_to_split(node):
                self._split(lsn, node)

    def delete(self, lsn, key):
        node = self._find_leaf_node(key)
        node.lsn = lsn
        if key in node.keys:
            index = node.keys.index(key)
            node.key_value_pairs.pop(index)
            node.keys.pop(index)

    def search(self, key):
        node = self._find_leaf_node(key)
        if key in node.keys:
            index = node.keys.index(key)
            return node.key_value_pairs[index]
        else:
            return []

    def search_range(self, start_key, end_key):
        """Include the value of start_key but not end_key."""
        result = []
        node = self._find_leaf_node(start_key)
        index = self._find_index(node, start_key)

        while node is not None:
            for i in range(index, len(node.keys)):
                key = node.keys[i]
                if key < end_key:
                    result.append(node.key_value_pairs[i])
                else:
                    return result
            node = node.next_leaf
            if node and node.state == LeafNode.STATE_UNLOADED:
                node = self.load_page(node.get_pageno())
            index = 0

        return result

    def load_page(self, pageno):
        raise NotImplementedError

    def _allocate_pageno(self):
        """Should override this method while using disk-based B+ tree."""
        current_pageno = self._next_pageno
        self._next_pageno += 1
        return current_pageno

    def _allocate_node(self, is_leaf):
        if is_leaf:
            node = LeafNode()
        else:
            node = InternalNode()
        node.set_pageno(self._allocate_pageno())
        return node

    def _find_leaf_node(self, key):
        current_node = self.root
        while True:
            if current_node.state == current_node.STATE_UNLOADED:
                current_node = self.load_page(current_node.get_pageno())
            if not isinstance(current_node, InternalNode):
                break
            index = self._find_index(current_node, key)
            current_node = current_node.children[index]
        # Maybe, the key is just in a gap between two nodes.
        # if that, we should get the next leaf node.
        if (len(current_node.keys) > 0 and (current_node.keys[-1] < key)
                and current_node.next_leaf):
            current_node = current_node.next_leaf
            if current_node.state == current_node.STATE_UNLOADED:
                current_node = self.load_page(current_node.get_pageno())
        return current_node

    def _split(self, lsn, node):
        mid = len(node.keys) // 2
        new_node = self._allocate_node(is_leaf=True)
        new_node.lsn = lsn
        new_node.keys = node.keys[mid:]
        new_node.key_value_pairs = node.key_value_pairs[mid:]
        new_node.high_key = node.high_key

        node.keys = node.keys[:mid]
        node.key_value_pairs = node.key_value_pairs[:mid]
        node.high_key = new_node.keys[0]  # Update the high key of the left node

        if node.next_leaf is not None:
            new_node.next_leaf = node.next_leaf
        node.next_leaf = new_node
        if node is self.root:
            parent = self._allocate_node(is_leaf=False)
            parent.lsn = lsn
            parent.keys.append(new_node.keys[0])
            parent.children.append(node)
            parent.children.append(new_node)
            self.root = parent
        else:
            parent = self._find_parent(node)
            index = self._find_index(parent, node.keys[0])
            parent.keys.insert(index, new_node.keys[0])
            parent.children.insert(index + 1, new_node)
            if self._need_to_split(parent):
                self._split(lsn, parent)

    def _need_to_split(self, node):
        """This method is just for demonstrating. We should override it according
        the size of data or other rules."""
        max_load_factor = 0.5
        return node.load_factor() > max_load_factor

    def _find_parent(self, node):
        current_node = self.root
        while True:
            if node in current_node.children:
                return current_node
            current_node = current_node.children[self._find_index(current_node, node.keys[0])]

    @staticmethod
    def _find_index(node, key):
        keys = node.keys
        for i, k in enumerate(keys):
            if key <= k:
                return i
        return len(keys)

    def serialize(self) -> bytes:
        nodes = []
        queue = [self.root]
        # traverse the tree
        while queue:
            node = queue.pop(0)

            if isinstance(node, InternalNode):
                queue.extend(node.children)

            nodes.append(node)
        # todo: can be streaming using yield
        nodes.sort(key=lambda n: n.get_pageno())

        # assert as below
        for i in range(len(nodes)):
            assert nodes[i].get_pageno() == i

        serialized_tree = bytearray()
        for node in nodes:
            serialized_tree += node.pack()
        # set the first field to present the root page number
        return (int.to_bytes(self.root.get_pageno(), 4, LITTLE_END) +
                bytes(serialized_tree))

    @classmethod
    def deserialize(cls, serialized_tree) -> 'BPlusTree':
        root_pageno = int.from_bytes(serialized_tree[:4], LITTLE_END)
        page_bytes = serialized_tree[4:]

        def get_node_by_idx(i):
            return page_bytes[(i * PAGE_SIZE): ((i + 1) * PAGE_SIZE)]

        root_data = get_node_by_idx(root_pageno)
        tree = cls(create_node(root_data))
        return tree

    def __repr__(self):
        return self.stringify(load_page=False)

    def stringify(self, load_page=True):
        lines = []
        self._stringify_node(self.root, depth=0, lines=lines, load_page=load_page)
        return '\n'.join(lines)

    def _stringify_node(self, node, depth=0, lines: list = None, load_page=False):
        if node is None:
            return ''

        if load_page and node.STATE_UNLOADED == LeafNode.STATE_UNLOADED:
            node = self.load_page(node.get_pageno())

        indent = "  " * depth
        if isinstance(node, InternalNode):
            node_type = "Internal"
        else:
            node_type = "Leaf"

        if node.state == LeafNode.STATE_UNLOADED:
            keys = 'unloading'
        else:
            keys = ", ".join(str(key) for key in node.keys)
        lines.append(f"{indent}{node_type} Node<{node.get_pageno()}>: Keys=[{keys}]")

        if isinstance(node, LeafNode):
            if node.state == LeafNode.STATE_UNLOADED:
                values = 'unloading'
            else:
                values = ", ".join(str(pair) for pair in node.key_value_pairs)
            lines.append(f"{indent}  Values=[{values}]")
        else:
            for child in node.children:
                self._stringify_node(child, depth + 1, lines, load_page)
