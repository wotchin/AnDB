import time

from prettytable import PrettyTable

from andb.catalog.attribute import AndbAttributeForm
from andb.catalog.oid import INVALID_OID, OID_TEMP_TABLE
from andb.sql.parser import CmdType


class ExecutionResult:
    def __init__(self, success=True, notice=None, warning=None, effect_rows=0, elapsed=0):
        self.success = success
        self.notice = notice
        self.warning = warning
        self.effect_rows = effect_rows
        self.elapsed = elapsed

    def __repr__(self):
        lines = []
        if self.notice:
            lines.append(f'NOTICE: {self.notice}')
        if self.warning:
            lines.append(f'WARNING: {self.warning}')
        lines.append(f'Effect Rows: {self.effect_rows}')
        lines.append(f'Elapsed Time: {self.elapsed}')
        return '\n'.join(lines)


class ExecuteResultTuple(ExecutionResult):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tuple = None


class ExecuteResultSet(ExecutionResult):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.attr_forms = []
        self.tuples = []

    def define_fields(self, fields):
        assert not self.attr_forms

        for i, field in enumerate(fields):
            name, type_oid, notnull = field
            self.attr_forms.append(
                AndbAttributeForm(
                    class_oid=OID_TEMP_TABLE, name=name, type_oid=type_oid, length=0, num=i, notnull=notnull
                )
            )

    def add_tuple(self, t):
        self.tuples.append(t)

    def rows(self):
        return len(self.tuples)
    
    def __repr__(self):
        if len(self.tuples) == 0:
            return super().__repr__()

        table = PrettyTable()
        attr_forms = self.attr_forms
        if not attr_forms:
            attr_forms = [
                AndbAttributeForm(class_oid=OID_TEMP_TABLE, name='undefined', type_oid=INVALID_OID, length=0, num=0, notnull=True)
                for _ in range(len(self.tuples[0]))
            ]

        table.field_names = [attr_form.name for attr_form in attr_forms]
        for t in self.tuples:
            table.add_row(t)
        return table.get_string() + '\n' + super().__repr__()


class ExecutionPortal:
    def __init__(self, query_string, cmd_type, plan_tree):
        self.query_string = query_string
        self.cmd_type = cmd_type
        self.plan_tree = plan_tree
        self.xid = 0
        self._results = None
        self.attr_forms = None  # target list
        self.init_elapsed = 0
        self.execute_elapsed = 0
        self.final_elapsed = 0

    def initialize(self):
        start_time = time.monotonic()
        self.plan_tree.open()
        self.init_elapsed = time.monotonic() - start_time

    def execute(self):
        start_time = time.monotonic()
        root = self.plan_tree
        self._results = list(root.next())
        self.execute_elapsed = time.monotonic() - start_time
        # todo: result type

    def finalize(self):
        start_time = time.monotonic()
        self.plan_tree.close()
        self.final_elapsed = time.monotonic() - start_time

    def results(self):
        # todo: result type
        total_elapsed = self.init_elapsed + self.execute_elapsed + self.final_elapsed
        if self.cmd_type == CmdType.CMD_UTILITY:
            return ExecutionResult(elapsed=total_elapsed)
        elif self.cmd_type == CmdType.CMD_SELECT:
            # todo: result set
            rv = ExecuteResultSet(elapsed=total_elapsed)
            rv.attr_forms = self.attr_forms
            rv.tuples = self._results
            rv.effect_rows = len(self._results)
            return rv
        elif self.cmd_type in (CmdType.CMD_INSERT,
                               CmdType.CMD_UPDATE,
                               CmdType.CMD_DELETE):
            return ExecutionResult(elapsed=total_elapsed, effect_rows=len(self._results))
        else:
            assert False

