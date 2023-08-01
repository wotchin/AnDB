from ._base import CatalogTable, CatalogForm
from .attribute import _ANDB_ATTRIBUTE


class IndexType:
    Unknown = 0
    BTREE = 1


class AndbIndexForm(CatalogForm):
    __fields__ = {
        'oid': 'bigint',
        'name': 'text',
        'index_type': 'int',
        'table_oid': 'bigint',
        'index_num': 'int',
        'attr_num': 'int'
    }

    def __init__(self, oid, name, table_oid, index_num, attr_num, index_type=IndexType.BTREE):
        self.oid = oid
        self.name = name
        self.table_oid = table_oid
        self.index_num = index_num
        self.attr_num = attr_num
        self.index_type = index_type

    def __lt__(self, other):
        if self.oid == other.oid:
            return self.index_num < other.index_num
        return self.oid < other.oid


class AndbIndexTable(CatalogTable):
    __tablename__ = 'andb_index'

    def init(self):
        pass

    def get_index_forms(self, oid):
        return self.search(lambda r: r.oid == oid)

    def define_index_fields(self, name, index_oid, table_oid, table_attr_forms):
        num = 0
        while num < len(table_attr_forms):
            form = table_attr_forms[num]
            self.insert(AndbIndexForm(
                oid=index_oid, name=name, table_oid=table_oid,
                index_num=num, attr_num=form.num
            ))
            num += 1

    def get_attr_form_array(self, index_oid):
        # todo: implement a cache for index forms
        index_forms = self.search(lambda r: r.oid == index_oid)
        assert len(index_forms) > 0
        table_oid = index_forms[0].table_oid
        attr_forms = _ANDB_ATTRIBUTE.search(lambda r: r.class_oid == table_oid)

        # join with attr forms
        attr_form_array = []

        # todo: this is a basic nested loop join, we can use sort-merge join later
        for index_form in index_forms:
            for attr_form in attr_forms:
                if index_form.attr_num == attr_form.num:
                    attr_form_array.append(attr_form)

        return attr_form_array


_ANDB_INDEX = AndbIndexTable()
