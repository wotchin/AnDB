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

