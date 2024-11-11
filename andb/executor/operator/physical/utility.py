from andb.catalog.oid import INVALID_OID
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_TYPE, CATALOG_ANDB_CLASS
from andb.errno.errors import RollbackError, DDLException
from andb.storage.engines.heap.relation import RelationKinds, bt_create_index_internal, \
    hot_create_table

from .base import PhysicalOperator


class CreateIndexOperator(PhysicalOperator):
    def __init__(self, index_name, table_name, fields, database_oid, index_type=None):
        super().__init__('CreateIndex')
        self.index_name = index_name
        self.table_name = table_name
        self.table_oid = INVALID_OID
        self.database_oid = database_oid
        self.fields = fields
        self.index_attr_form_array = []
        self.table_attr_form_array = None
        self.index_oid = INVALID_OID
        self.index_type = index_type

    def open(self):
        try:
            self.table_oid = CATALOG_ANDB_CLASS.get_relation_oid(self.table_name, self.database_oid,
                                                                 kind=RelationKinds.HEAP_TABLE)
        except RollbackError as e:
            raise DDLException(e)

        self.table_attr_form_array = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == self.table_oid)
        for field in self.fields:
            index_attr = None
            for attr in self.table_attr_form_array:
                if attr.name == field:
                    index_attr = attr
                    break
            if index_attr is None:
                raise DDLException(f'not found the field {field} in the table {self.table_name}.')
            self.index_attr_form_array.append(index_attr)

        self.total_cost = 1000  # todo: estimate cost of creating index

    def next(self):
        self.index_oid = bt_create_index_internal(index_name=self.index_name, table_oid=self.table_oid,
                                                  attr_form_array=self.table_attr_form_array,
                                                  index_attr_form_array=self.index_attr_form_array,
                                                  database_oid=self.database_oid)
        yield self.index_oid


class CreateTableOperator(PhysicalOperator):
    def __init__(self, table_name, fields, database_oid):
        super().__init__('CreateTable')
        self.table_name = table_name
        self.fields = fields
        self.database_oid = database_oid
        self.table_oid = INVALID_OID

    def open(self):
        calibrated_fields = []
        for fields in self.fields:
            if len(fields) < 2:
                raise DDLException('invalid table columns.')
            name, type_name = fields[0], fields[1]
            # todo: check name character
            if CATALOG_ANDB_TYPE.get_type_oid(type_name) == INVALID_OID:
                raise DDLException(f'invalid type {type_name}.')
            if len(fields) == 2:
                calibrated_fields.append([name, type_name, False])
            else:
                calibrated_fields.append(fields)
        self.fields = calibrated_fields

    def next(self):
        # allow to throw DDLException
        self.table_oid = hot_create_table(table_name=self.table_name, fields=self.fields,
                                          database_oid=self.database_oid)
        yield self.table_oid


class ExplainOperator(PhysicalOperator):

    @staticmethod
    def explain(operator, indent=''):
        output_lines = []
        name = operator.name
        output_lines.append(f"{indent} -> {name}")

        # add operator parameters
        params = []
        for name, value in operator.get_args():
            params.append(f"{name}: {value}")
        output_lines.append(f"{indent}    {', '.join(params)}")

        for i, child in enumerate(operator.children):
            child_indent = f'{indent}  '
            output_lines.extend(ExplainOperator.explain(child, child_indent))

        return output_lines

    def __init__(self, logical_plan):
        super().__init__('Explain')
        self.logical_plan = logical_plan
        self.physical_plan = None
        # todo: add grammar and syntax support, explain analyze ...
        self.analyze = False

    def open(self):
        # let the database generate physical plan
        # we can directly close the physical plan when don't use analyze
        self.physical_plan.open()
        if self.analyze:
            # we should perform the statement when analyzing
            list(self.physical_plan.next())
        self.physical_plan.close()

    def next(self):
        # onw row with two columns: logical plan and physical plan
        yield ('\n'.join(ExplainOperator.explain(self.logical_plan)),
               '\n'.join(ExplainOperator.explain(self.physical_plan)))

    def close(self):
        pass
