from andb.constants.values import PAGE_SIZE
from andb.storage.engines.heap.bptree import BPlusTree, TuplePointer, create_node


def test_bplus_tree():
    lsn = 0

    def next_lsn():
        nonlocal lsn
        lsn += 1
        return lsn

    tree = BPlusTree()
    tree.insert(next_lsn(), 5, "apple")
    tree.insert(next_lsn(), 3, "banana")
    tree.insert(next_lsn(), 5, "orange")
    assert tree.search(5) == ['apple', 'orange']
    assert tree.search_range(0, 3) == []
    assert (tree.search_range(3, 5)) == [['banana']]
    assert (tree.search_range(5, 10)) == [['apple', 'orange']]

    tree.insert(next_lsn(), 5, "pear0")
    tree.insert(next_lsn(), 5, "pear1")
    tree.insert(next_lsn(), 5, "pear2")
    tree.insert(next_lsn(), 5, "pear3")
    tree.insert(next_lsn(), 5, "pear4")
    tree.insert(next_lsn(), 5, "pear5")
    assert (tree.search_range(0, 10)) == [['banana'],
                                          ['apple', 'orange', 'pear0', 'pear1', 'pear2', 'pear3', 'pear4', 'pear5']]

    assert str(tree).strip() == """\
Leaf Node<0>: Keys=[3, 5]
  Values=[['banana'], ['apple', 'orange', 'pear0', 'pear1', 'pear2', 'pear3', 'pear4', 'pear5']]"""

    assert tree.search(5) == ['apple', 'orange', 'pear0', 'pear1', 'pear2', 'pear3', 'pear4', 'pear5']
    assert tree.search(6) == []

    tree.delete(next_lsn(), 5)
    assert tree.search(5) == []
    assert (tree.search_range(0, 10)) == [['banana']]

    tree.insert(next_lsn(), 2, 'second')
    tree.insert(next_lsn(), 4, 'fourth')
    tree.insert(next_lsn(), 5, 'fifth')
    tree.insert(next_lsn(), 6, 'sixth')

    assert tree.search(2) == ['second']
    assert tree.search(6) == ['sixth']
    assert tree.search(5) == ['fifth']
    assert tree.search(4) == ['fourth']
    assert (tree.search_range(0, 10)) == [['second'], ['banana'], ['fourth'], ['fifth'], ['sixth']]


def test_bplus_tree_page():
    lsn = 0

    def next_lsn():
        nonlocal lsn
        lsn += 1
        return lsn

    tree = BPlusTree()
    tree.insert(next_lsn(), b'hello', TuplePointer(1, 1))
    tree.insert(next_lsn(), b'world', TuplePointer(2, 1))
    assert (tree.search(b'hello')) == [TuplePointer(1, 1)]
    assert (tree.search(b'h')) == []
    assert (tree.search(b'world')) == [TuplePointer(2, 1)]

    tree.insert(next_lsn(), b'aaa', TuplePointer(1, 2))
    tree.insert(next_lsn(), b'aaa', TuplePointer(1, 3))
    tree.insert(next_lsn(), b'aaa', TuplePointer(1, 2))
    assert str(tree.search(b'aaa')) == '[<pageno: 1, tid: 2>, <pageno: 1, tid: 3>, <pageno: 1, tid: 2>]'

    tree.insert(next_lsn(), b'aab', TuplePointer(1, 4))
    tree.insert(next_lsn(), b'aac', TuplePointer(1, 5))
    tree.insert(next_lsn(), b'aad', TuplePointer(1, 6))
    assert str(tree.search_range(b'a', b'b')) == '[[<pageno: 1, tid: 2>, <pageno: 1, tid: 3>, <pageno: 1, tid: 2>], ' \
                                                 '[<pageno: 1, tid: 4>], [<pageno: 1, tid: 5>], [<pageno: 1, tid: 6>]]'
    b = tree.serialize()
    assert (len(b) // PAGE_SIZE) == 3

    page_bytes = b[4:]

    class DiskBasedBPlusTree(BPlusTree):
        def load_page(self, pageno):
            return create_node(page_bytes[(pageno * PAGE_SIZE): ((pageno + 1) * PAGE_SIZE)])

    tree_stringified = str(tree)
    tree2 = DiskBasedBPlusTree.deserialize(b)

    assert (tree2.search(b'hello')) == tree.search(b'hello')
    assert (tree2.search(b'h')) == tree.search(b'h')
    assert (tree2.search(b'world')) == tree.search(b'world')

    assert (tree2.search(b'aaa')) == tree.search(b'aaa')

    assert (tree2.search_range(b'a', b'b')) == tree.search_range(b'a', b'b')

    tree2_stringified = tree2.stringify()
    assert tree_stringified == tree2_stringified

    assert (tree2.search(b'aac')) == tree.search(b'aac')