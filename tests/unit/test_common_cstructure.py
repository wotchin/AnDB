from andb.common.cstructure import Integer4Field
from andb.common.cstructure import CharField
from andb.common.cstructure import Float4Field
from andb.common.cstructure import CStructure
from andb.common.utils import bytes_to_hex


class TestStructure(CStructure):
    id = Integer4Field()
    name = CharField(num=8)
    salary = Float4Field()
    gender = CharField()
    address = CharField(num=10)


def test_cstructure():
    base = TestStructure()
    base.id = 1
    base.name = b'xiaoming'
    base.salary = 10240.10
    base.gender = 1
    base.address = b'ABC City, xxxxxxxxxxxxx'

    buff = base.pack()
    assert bytes_to_hex(buff) == """
 01 00 00 00 78 69 61 6f 
 6d 69 6e 67 66 00 20 46 
 01 00 41 42 43 20 43 69 
 74 79 2c 20"""

    new_one = TestStructure()
    new_one.unpack(buff)
    assert new_one.id == 1
    assert new_one.name == b'xiaoming'
    assert (new_one.salary - 10240.10) <= 0.1
    assert new_one.gender == 1
    # truncation
    assert new_one.address == b'ABC City, '
    assert base.size() % 4 == 0

