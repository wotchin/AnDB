

class PhysicalOperator:
    def __init__(self, name):
        self.name = name
        self.startup_cost = 0
        self.total_cost = 0
        self.startup_elapsed = 0
        self.total_elapsed = 0
        self.args = {}
        self.children = []

    def open(self):
        pass

    def next(self):
        pass

    def close(self):
        pass

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

