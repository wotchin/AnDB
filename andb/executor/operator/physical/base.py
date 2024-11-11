import time


class PhysicalOperator:
    def __init__(self, name):
        self.name = name
        self.startup_cost = 0
        self.total_cost = 0
        self.startup_elapsed = 0
        self.total_elapsed = 0

        self.children = []
        # output target columns
        self.columns = None

        # instrument
        self.actual_rows = 0
        self.open_time = 0
        self.close_time = 0

    def get_args(self):
        return (('cost', f'{self.startup_cost}...{self.total_cost}'),
                ('actual_rows', self.actual_rows),
                ('elapsed', f'{self.close_time - self.open_time:.2f}'))

    def open(self):
        self.open_time = time.monotonic()

    def next(self):
        self.actual_rows += 1

    def close(self):
        self.close_time = time.monotonic()

    def add_child(self, operator):
        assert isinstance(operator, PhysicalOperator)
        self.children.append(operator)

    # alias
    @property
    def left_tree(self):
        assert len(self.children) <= 2
        if len(self.children) == 0:
            return None

        return self.children[0]

    @property
    def right_tree(self):
        assert len(self.children) <= 2
        if len(self.children) == 2:
            return self.children[1]

        return None

