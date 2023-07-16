from andb.errno.errors import BufferOverflow
from andb.common.replacement import LRUCache


def test_lrucache():
    cache = LRUCache(2)

    cache.put(1, 'a')
    cache.put(2, 'b', pinned=True)  # 'b' is marked as unpinnable

    assert (cache.get(1)) == 'a'  # Output: 'a'
    assert (cache.get(2)) == 'b'  # Output: 'b'

    cache.put(3, 'c')  # Evicts 'a' since it is the only pinnable node available

    assert (cache.get(1)) is None  # not found, as it was evicted
    assert (cache.get(2)) == 'b'  # Output: 'b'
    assert (cache.get(3)) == 'c'  # Output: 'c'
    cache.put(3, 'b')
    assert (cache.get(3)) == 'b'
    cache.put(3, 'd', pinned=True)
    assert (cache.get(3)) == 'd'
    try:
        cache.put(4, '4', pinned=True)
    except BufferOverflow:
        pass
    else:
        raise AssertionError()

    try:
        cache.put(5, '5', pinned=True)
    except BufferOverflow:
        pass
    else:
        raise AssertionError()

    assert list(cache) == ['b', 'd']
    cache.unpin(3)
    cache.put(4, '4')
    assert list(cache) == ['b', '4']

    assert len(list(cache.items())) > 0
    cache.clear()
    assert len(list(cache.items())) == 0


test_lrucache()
