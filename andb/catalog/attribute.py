from ._base import CatalogTable, CatalogTuple
from .type import ANDB_TYPE


class AndbAttributeTuple(CatalogTuple):
    __fields__ = {
        'class_oid': 'bigint',
        'name': 'text',
        'type_oid': 'bigint',
        'num': 'integer',
        'notnull': 'boolean'
    }

    def __init__(self, class_oid, name, type_oid, num, notnull=False):
        self.class_oid = class_oid
        self.name = name
        self.type_oid = type_oid
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

    def define_table_fields(self, class_oid, fields):
        num = 0
        while num < len(fields):
            name, type_name, notnull = fields[num]
            # todo: atomic
            self.insert(AndbAttributeTuple(
                class_oid=class_oid,
                name=name,
                type_oid=ANDB_TYPE.get_type_oid(type_name),
                num=num,
                notnull=notnull
            ))


