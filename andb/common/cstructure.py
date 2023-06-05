import struct
from collections import OrderedDict

CTYPE_BIG_ENDIAN = '>'
CTYPE_LITTLE_ENDIAN = '<'

CTYPE_TYPE_CHAR = 'c'
CTYPE_TYPE_CHAR_ARRAY = 's'
CTYPE_TYPE_SHORT = 'H'
CTYPE_TYPE_INT4 = 'i'
CTYPE_TYPE_UINT4 = 'I'
CTYPE_TYPE_INT8 = 'q'
CTYPE_TYPE_UINT8 = 'Q'
CTYPE_TYPE_FLOAT4 = 'f'
CTYPE_TYPE_FLOAT8 = 'd'


class Field:
    def __init__(self, name, ctype, num, default):
        self.name = name
        self.ctype = ctype
        self.pretty_ctype = None
        self.num = num
        self.default = default

        assert ctype
        assert self.num >= 1

    def __repr__(self):
        if self.num == 1:
            return '<%s %s>' % (
                self.pretty_ctype or self.ctype, self.name)
        else:
            return '<%s %s[%d]>' % (
                self.pretty_ctype or self.ctype, self.name, self.num
            )


class CharField(Field):
    def __init__(self, name=None, num=1):
        if num > 1:
            # default value is an empty char array
            super(CharField, self).__init__(name, CTYPE_TYPE_CHAR_ARRAY, num, bytes(num))
        else:
            # short type is easier than char for only one element
            super(CharField, self).__init__(name, CTYPE_TYPE_SHORT, num, 0)
        self.pretty_ctype = 'char'


class Integer4Field(Field):
    def __init__(self, name=None, unsigned=False, num=1):
        if unsigned:
            super(Integer4Field, self).__init__(name, CTYPE_TYPE_UINT4, num, 0)
            self.pretty_ctype = 'unsigned int'
        else:
            super(Integer4Field, self).__init__(name, CTYPE_TYPE_INT4, num, 0)
            self.pretty_ctype = 'int'


class Integer8Field(Field):
    def __init__(self, name=None, unsigned=False, num=1):
        if unsigned:
            super(Integer8Field, self).__init__(name, CTYPE_TYPE_UINT8, num, 0)
            self.pretty_ctype = 'unsigned long long'
        else:
            super(Integer8Field, self).__init__(name, CTYPE_TYPE_INT8, num, 0)
            self.pretty_ctype = 'long long'


class Float4Field(Field):
    def __init__(self, name=None, num=1):
        super(Float4Field, self).__init__(name, CTYPE_TYPE_FLOAT4, num, 0.)
        self.pretty_ctype = 'float'


class Float8Field(Field):
    def __init__(self, name=None, num=1):
        super(Float8Field, self).__init__(name, CTYPE_TYPE_FLOAT8, num, 0.)
        self.pretty_ctype = 'double'


class StructureMeta(type):
    def __new__(cls, name, bases, attrs):
        if name == 'CStructure':
            return type.__new__(cls, name, bases, attrs)

        # should be in order
        mappings = OrderedDict()
        format_ = list()
        for k, v in attrs.items():
            if isinstance(v, CStructure):
                raise NotImplementedError('Not supported to set non-primitive data types yet.')
            if isinstance(v, Field):
                # set key name as the default name
                if not v.name:
                    v.name = k
                mappings[k] = v
                # this is an array
                if v.num > 1:
                    format_.append(str(v.num))
                format_.append(v.ctype)

        attrs['__mappings__'] = mappings
        attrs['__cformat__'] = CTYPE_LITTLE_ENDIAN + ''.join(format_)
        return type.__new__(cls, name, bases, attrs)


class CStructure(metaclass=StructureMeta):
    __mappings__ = None
    __cformat__ = None

    def pack(self):
        values = list()
        for k, field in self.__mappings__.items():
            real_value = getattr(self, k)
            # append default values if not set
            if isinstance(real_value, Field):
                if field.num > 1:
                    for _ in range(field.num):
                        values.append(field.default)
                else:
                    values.append(field.default)
            else:
                if field.num > 1:
                    if isinstance(real_value, list):
                        values.extend(real_value)
                    elif isinstance(real_value, bytes):
                        values.append(real_value)
                    else:
                        raise ValueError('%s should be a list or bytes.' % k)

                else:
                    values.append(real_value)
        try:
            return struct.pack(self.__cformat__, *values)
        except struct.error as e:
            raise struct.error("%s. format is '%s' and values are %s." % (
                e, self.__cformat__, values))

    def unpack(self, buffer):
        values = struct.unpack(self.__cformat__, buffer)
        if len(values) == 0:
            return

        i = 0
        for k, field in self.__mappings__.items():
            if field.num > 1 and not isinstance(field, CharField):
                array_ = values[i: i + field.num]
                i += field.num
                setattr(self, k, array_)
            else:
                value = values[i]
                setattr(self, k, value)
                i += 1

    def size(self):
        return struct.calcsize(self.__cformat__)

    def __eq__(self, other):
        if not isinstance(other, CStructure):
            return False
        return self.__dict__ == other.__dict__


def bytes_to_hex(b):
    h = list()
    for i, ch in enumerate(b):
        if i % 8 == 0:
            h.append('\n')
        h.append('%02x' % ch)
    return ' '.join(h)
