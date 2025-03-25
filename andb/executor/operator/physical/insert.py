from andb.catalog.syscache import CATALOG_ANDB_INDEX, CATALOG_ANDB_ATTRIBUTE
from andb.errno.errors import InitializationStageError
from andb.storage.engines.heap.bptree import TuplePointer
from andb.storage.engines.heap.relation import hot_simple_insert, bt_simple_insert, open_relation, close_relation
from andb.storage.engines.memory.table import memory_insert
from andb.storage.lock import rlock
from andb.runtime import session_vars
from .base import PhysicalOperator


class InsertPhysicalOperator(PhysicalOperator):
    def __init__(self, table_oid, python_tuples, column_idxs, is_select):
        super().__init__('Insert')
        self.startup_cost = 0
        self.total_cost = 1
        self.startup_elapsed = 0
        self.total_elapsed = 0

        self.table_oid = table_oid
        self.index_form_array = CATALOG_ANDB_INDEX.search(lambda r: r.table_oid == self.table_oid)
        self.relation = None
        self.index_relations = None
        self.is_select = is_select
        self.from_values = python_tuples
        self.column_idxs = column_idxs

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
            
        if self.is_select:
            self.from_values.open()

    def next(self):
        insert_values = self.from_values
        if self.is_select:
            attr_forms = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == self.table_oid)
            attr_forms = {form.num: form for form in attr_forms}
            
            insert_values = []
            for tup in self.from_values.next():
                row = [None] * len(attr_forms)
                for i, attr_num in enumerate(self.column_idxs):
                    if (attr_forms[attr_num].notnull and tup[i] is None) or \
                       (attr_forms[attr_num].enum_vals is not None and tup[i] not in attr_forms[attr_num].enum_vals):
                        raise ValueError(f'Invalid value {tup[i]} for column {attr_forms[attr_num].name}')
                    row[attr_num] = tup[i]
                insert_values.append(row)
            
        for python_tuple in insert_values:
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
            
        if self.is_select:
            self.from_values.close()

        super().close()


class InsertMemoryTablePhysicalOperator(PhysicalOperator):
    def __init__(self, table_oid, python_tuples=None, column_idxs=None, is_select=False):
        super().__init__('InsertMemoryTable')
        self.table_oid = table_oid
        self.database_oid = session_vars.get_session_value('database_oid')
        self.from_values = python_tuples
        self.is_select = is_select
        self.column_idxs = column_idxs

    def open(self):
        super().open()
            
        if self.is_select:
            self.from_values.open()

    def next(self):
        
        if self.is_select:
            insert_values = []
            attr_forms = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == self.table_oid)
            attr_forms = {form.num: form for form in attr_forms}
            for tup in self.from_values.next():
                row = [None] * len(attr_forms)
                for i, attr_num in enumerate(self.column_idxs):
                    if (attr_forms[attr_num].notnull and tup[i] is None) or \
                       (attr_forms[attr_num].enum_vals is not None and tup[i] not in attr_forms[attr_num].enum_vals):
                        raise ValueError(f'Invalid value {tup[i]} for column {attr_forms[attr_num].name}')
                    row[attr_num] = tup[i]
                insert_values.append(row)
        else:
            insert_values = self.from_values

        for python_tuple in insert_values:
            memory_insert(self.table_oid, self.database_oid, python_tuple)
            yield

    def close(self):
        if self.is_select:
            self.from_values.close()

        super().close()
