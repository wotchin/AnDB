def rot(x, k):
    return (x << k) | (x >> (32 - k))


def mix(a, b, c):
    a -= c
    a ^= rot(c, 4)
    c += b
    b -= a
    b ^= rot(a, 6)
    a += c
    c -= b
    c ^= rot(b, 8)
    b += a
    a -= c
    a ^= rot(c, 16)
    c += b
    b -= a
    b ^= rot(a, 19)
    a += c
    c -= b
    c ^= rot(b, 4)
    b += a
    return a, b, c


def final(a, b, c):
    c ^= b
    c -= rot(b, 14)
    a ^= c
    a -= rot(c, 11)
    b ^= a
    b -= rot(a, 25)
    c ^= b
    c -= rot(b, 16)
    a ^= c
    a -= rot(c, 4)
    b ^= a
    b -= rot(a, 14)
    c ^= b
    c -= rot(b, 24)
    return a, b, c


def hash_bytes(k, keylen):
    a = b = c = 0x9e3779b9 + keylen + 3923095
    length = keylen

    # check for word-aligned
    if isinstance(k, memoryview) and (k.itemsize == 1):
        ka = k.cast('I')
        while length >= 12:
            a += ka[0]
            b += ka[1]
            c += ka[2]
            a, b, c = mix(a, b, c)
            ka = ka[3:]
            length -= 12

        k = ka.cast('B')
        remaining = length
        if remaining >= 8:
            c += (k[7] << 24)
        if remaining >= 7:
            c += (k[6] << 16)
        if remaining >= 6:
            c += (k[5] << 8)
        if remaining >= 5:
            c += k[4]
        if remaining >= 4:
            a += (k[3] << 24)
        if remaining >= 3:
            a += (k[2] << 16)
        if remaining >= 2:
            a += (k[1] << 8)
        if remaining >= 1:
            a += k[0]

    else:
        while length >= 12:
            a += (k[0] + (k[1] << 8) + (k[2] << 16) + (k[3] << 24))
            b += (k[4] + (k[5] << 8) + (k[6] << 16) + (k[7] << 24))
            c += (k[8] + (k[9] << 8) + (k[10] << 16) + (k[11] << 24))
            a, b, c = mix(a, b, c)
            k = k[12:]
            length -= 12

        remaining = length
        if remaining >= 8:
            c += (k[7] << 24)
        if remaining >= 7:
            c += (k[6] << 16)
        if remaining >= 6:
            c += (k[5] << 8)
        if remaining >= 5:
            c += k[4]
        if remaining >= 4:
            a += (k[3] << 24)
        if remaining >= 3:
            a += (k[2] << 16)
        if remaining >= 2:
            a += (k[1] << 8)
        if remaining >= 1:
            a += k[0]

    a, b, c = final(a, b, c)
    # truncate
    return c & 0xFFFFFFFF


def hash_any(v):
    pass


def hash_int(v: int, length):
    key = int.to_bytes(v, length, 'little')
    return hash_bytes(key, len(key))


def test_hash_bytes():
    cases = ((b'Hello, World!', 2645077949),
             (b'andb', 870259952),
             (b'12345', 1858864134))
    for key, expected in cases:
        result = hash_bytes(key, len(key))
        assert result == expected


def test_hash_int():
    assert (hash_int(1, 4)) == 1627631432
    assert (hash_int(92, 4)) == 857227344
    try:
        hash_int(0xffffffffff, 4)
    except OverflowError:
        pass
    else:
        raise
    assert (hash_int(0xffffffff, 4)) == 1462718007
    assert (hash_int(0xffffffff, 8)) == 302020952


test_hash_bytes()
test_hash_int()


def hash_float():
    return None


def hash_bool():
    return None


def hash_string():
    return None

