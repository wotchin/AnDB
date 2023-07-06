import functools


def hook_function(function, prefunction=None, postfunction=None):
    # nothing to do
    if not prefunction and (not postfunction):
        return function

    @functools.wraps(function)
    def run(*args, **kwargs):
        if prefunction:
            prefunction(*args, **kwargs)
        rv = function(*args, **kwargs)
        if postfunction:
            postfunction(*args, **kwargs)
        return rv

    return run


def is_array_like(a):
    return (isinstance(a, list) or isinstance(a, tuple)
            or isinstance(a, set))


def insert_bytearray(raw, i, new):
    for start in range(i, i + len(new)):
        raw.insert(start, new[start - i])


def bytes_to_hex(b):
    h = list()
    for i, ch in enumerate(b):
        if i % 8 == 0:
            h.append('\n')
        h.append('%02x' % ch)
    return ' '.join(h)


def get_the_nearest_two_power_number(value):
    # keep 1 bit to test overflow
    rv = value & 0xefffffff
    rv |= (rv >> 1)
    rv |= (rv >> 2)
    rv |= (rv >> 4)
    rv |= (rv >> 8)
    rv |= (rv >> 16)
    rv += 1
    if rv > 0xefffffff:
        rv >>= 1
    return rv


def memoize(func):
    """a generic cache, which won't cache unhashable types."""
    memtbl = {}

    @functools.wraps(func)
    def wrapper(*args):
        if args in memtbl:
            return memtbl[args]
        else:
            rv = func(*args)
            memtbl[args] = rv
            return rv

    return wrapper
