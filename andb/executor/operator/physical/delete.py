from andb.catalog.syscache import CATALOG_ANDB_INDEX
from andb.errno.errors import InitializationStageError
from andb.executor.operator.physical.select import Scan
from andb.runtime import session_vars
from andb.storage.engines.heap.relation import hot_simple_delete, bt_delete, open_relation, \
    close_relation
from andb.storage.engines.memory.table import memory_delete_batch, memory_get_row_id
from andb.storage.lock import rlock
from .base import PhysicalOperator


class DeletePhysicalOperator(PhysicalOperator):
    def __init__(self, table_oid, scan_operator: Scan):
        super().__init__('Insert')
        self.startup_cost = 0
        self.total_cost = 1
        self.startup_elapsed = 0
        self.total_elapsed = 0

        self.table_oid = table_oid
        self.index_form_array = CATALOG_ANDB_INDEX.search(lambda r: r.table_oid == self.table_oid)
        self.relation = None
        self.index_relations = None

        # can use index scan :)
        self.scan = scan_operator
        self.add_child(scan_operator)

    def get_args(self):
        return ('table_name', self.relation.name), ('table_oid', self.table_oid) + super().get_args()

    def open(self):
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

        self.scan.open()

    def next(self):
        for tuple_ in self.scan.next():
            pageno, tid = self.scan.get_cursor()
            # delete both heap table and indexes
            hot_simple_delete(self.relation, pageno, tid)
            for index_relation in self.index_relations:
                index_forms = self.index_relations[index_relation]
                key = [tuple_[form.attr_num] for form in index_forms]
                bt_delete(index_relation, key)
            yield

    def close(self):
        close_relation(self.table_oid, rlock.ROW_EXCLUSIVE_LOCK)
        for relation in self.index_relations:
            close_relation(relation.oid, rlock.ROW_EXCLUSIVE_LOCK)

        self.scan.close()


class DeleteMemoryTablePhysicalOperator(PhysicalOperator):
    def __init__(self, table_oid, scan_operator: Scan):
        super().__init__('DeleteMemoryTable')
        self.table_oid = table_oid
        self.database_oid = session_vars.get_session_value('database_oid')
        self.scan = scan_operator
        self.add_child(scan_operator)

    def open(self):
        super().open()
        self.scan.open()

    def next(self):
        to_be_deleted_rows = []
        for tuple_ in self.scan.next():
            row_id = memory_get_row_id(self.table_oid, self.database_oid, tuple_)
            to_be_deleted_rows.append(row_id)

        memory_delete_batch(self.table_oid, self.database_oid, to_be_deleted_rows)

        for row_id in to_be_deleted_rows:
            yield

    def close(self):
        self.scan.close()
        super().close()
