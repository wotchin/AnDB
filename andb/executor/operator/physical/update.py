from andb.catalog.syscache import CATALOG_ANDB_INDEX
from andb.errno.errors import InitializationStageError
from andb.storage.engines.heap.bptree import TuplePointer
from andb.storage.engines.heap.relation import bt_update, hot_simple_update, open_relation, close_relation, bt_delete, \
    bt_simple_insert
from andb.storage.lock import rlock
from andb.executor.operator.physical.select import Scan
from andb.catalog.oid import INVALID_OID

from ..logical import Condition, TableColumn
from .select import Filter
from .base import PhysicalOperator


class UpdatePhysicalOperator(PhysicalOperator):
    def __init__(self, table_oid, scan_operator: Scan, attr_num_value_pair: dict):
        super().__init__('Update')
        self.startup_cost = 0
        self.total_cost = 1
        self.startup_elapsed = 0
        self.total_elapsed = 0

        self.table_oid = table_oid
        self.relation = None
        self.index_relations = None
        self.modify_index_relations = None
        self.index_form_array = CATALOG_ANDB_INDEX.search(lambda r: r.table_oid == self.table_oid)

        # can use index scan :)
        self.scan = scan_operator
        self.attr_num_value_pair = attr_num_value_pair

    def _need_to_modify_key(self, index_attrs):
        for table_attr in self.attr_num_value_pair:
            for index_attr in index_attrs:
                if table_attr.num == index_attr.attr_num:
                    return True
        return False

    def get_args(self):
        return ('table_name', self.relation.name), ('table_oid', self.table_oid) + super().get_args()

    def open(self):
        super().open()

        self.relation = open_relation(self.table_oid, rlock.ROW_EXCLUSIVE_LOCK)
        if not self.relation:
            raise InitializationStageError(f'cannot open relation {self.table_oid} for update.')

        self.index_relations = {}  # e.g., {relation: [form0, form1, ...]}
        self.modify_index_relations = set()
        for form in self.index_form_array:
            relation = open_relation(form.oid, rlock.ROW_EXCLUSIVE_LOCK)
            if not relation:
                raise InitializationStageError(f'cannot get the relation using oid {form.oid}.')

            if relation not in self.index_relations:
                self.index_relations[relation] = []
            self.index_relations[relation].append(form)

        for relation in self.index_relations:
            if self._need_to_modify_key(self.index_relations[relation]):
                self.modify_index_relations.add(relation)

        self.scan.open()

    def next(self):
        for tuple_ in self.scan.next():
            pageno, tid = self.scan.get_cursor()
            # update both heap table and indexes
            new_tuple = list(tuple_)
            for attr_num, new_value in self.attr_num_value_pair.items():
                new_tuple[attr_num] = new_value
            new_pageno, new_tid = hot_simple_update(relation=self.relation, pageno=pageno, tid=tid,
                                                    python_tuple=new_tuple)
            new_tuple_pointer = TuplePointer(new_pageno, new_tid)
            for index_relation in self.index_relations:
                index_forms = self.index_relations[index_relation]

                key = [tuple_[form.attr_num] for form in index_forms]
                if index_relation not in self.modify_index_relations:
                    bt_update(index_relation, key, new_tuple_pointer)
                else:
                    bt_delete(index_relation, key)
                    new_key = [new_tuple[form.attr_num] for form in index_forms]
                    bt_simple_insert(index_relation, new_key, new_tuple_pointer)
            yield

    def close(self):
        close_relation(self.table_oid, rlock.ROW_EXCLUSIVE_LOCK)
        for relation in self.index_relations:
            close_relation(relation.oid, rlock.ROW_EXCLUSIVE_LOCK)

        self.scan.close()
        super().close()
