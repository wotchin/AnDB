from andb.catalog.syscache import CATALOG_ANDB_INDEX
from andb.errno.errors import InitializationStageError
from andb.storage.engines.heap.bptree import TuplePointer
from andb.storage.engines.heap.relation import hot_simple_insert, bt_simple_insert, open_relation, close_relation
from andb.storage.lock import rlock
from .base import PhysicalOperator


class InsertPhysicalOperator(PhysicalOperator):
    def __init__(self, table_oid, python_tuples=None, select=None):
        super().__init__('Insert')
        self.startup_cost = 0
        self.total_cost = 1
        self.startup_elapsed = 0
        self.total_elapsed = 0

        self.table_oid = table_oid
        self.index_form_array = CATALOG_ANDB_INDEX.search(lambda r: r.table_oid == self.table_oid)
        self.relation = None
        self.index_relations = None
        self.select = select
        self.python_tuples = python_tuples

    def get_args(self):
        return ('table_name', self.relation.name), ('table_oid', self.table_oid) + super().get_args()

    def open(self):
        super().open()

        self.relation = open_relation(self.table_oid, rlock.ROW_EXCLUSIVE_LOCK)
        if not self.relation:
            raise InitializationStageError(f'cannot get the relation using oid {self.table_oid}.')

        self.index_relations = {}  # e.g., {relation: [form0, form1, ...]}
        for form in self.index_form_array:
            relation = open_relation(form.oid, rlock.ROW_EXCLUSIVE_LOCK)
            if not relation:
                raise InitializationStageError(f'cannot get the relation using oid {form.oid}.')
            if relation not in self.index_relations:
                self.index_relations[relation] = []
            self.index_relations[relation].append(form)

    def next(self):
        if not self.python_tuples:
            # todo: fetch tuples from select clause
            pass

        for python_tuple in self.python_tuples:
            pageno, tid = hot_simple_insert(self.relation, python_tuple=python_tuple)
            for relation, form_array in self.index_relations.items():
                key = [python_tuple[form.attr_num] for form in form_array]
                bt_simple_insert(relation, key=key, tuple_pointer=TuplePointer(pageno, tid))
            # easy to count iterations
            yield

    def close(self):
        close_relation(self.table_oid, rlock.ROW_EXCLUSIVE_LOCK)
        for relation in self.index_relations:
            close_relation(relation.oid, rlock.ROW_EXCLUSIVE_LOCK)

        super().close()
