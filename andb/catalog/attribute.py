from ._base import CatalogTable, CatalogForm
from .type import _ANDB_TYPE, VarcharType


class AndbAttributeForm(CatalogForm):
    __fields__ = {
        'class_oid': 'bigint',
        'name': 'text',
        'type_oid': 'bigint',
        'length': 'integer',
        'num': 'integer',
        'notnull': 'boolean'
    }

    def __init__(self, class_oid, name, type_oid, length, num, notnull=False):
        self.class_oid = class_oid
        self.name = name
        self.type_oid = type_oid
        self.length = length
        self.num = num
        self.notnull = notnull

    def __lt__(self, other):
        if self.class_oid == other.class_oid:
            return self.num < other.num
        return self.class_oid < other.class_oid


class AndbAttributeTable(CatalogTable):
    __tablename__ = 'andb_attribute'

    def init(self):
        # todo: insert system catalog information?
        pass

    def get_table_fields(self, class_oid):
        return self.search(lambda r: r.class_oid == class_oid)

    def define_relation_fields(self, class_oid, fields):
        num = 0
        while num < len(fields):
            name, type_name, notnull = fields[num]
            # todo: atomic
            if type_name.startswith(VarcharType.type_name):
                # varchar is fixed length
                length = int(type_name.replace(VarcharType.type_name, ''))
                type_name = VarcharType.type_name
            else:
                length = _ANDB_TYPE.get_type_form(type_name).type_bytes
            self.insert(AndbAttributeForm(
                class_oid=class_oid,
                name=name,
                type_oid=_ANDB_TYPE.get_type_oid(type_name),
                length=length,
                num=num,
                notnull=notnull
            ))
            num += 1


_ANDB_ATTRIBUTE = AndbAttributeTable()
