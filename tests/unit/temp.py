def combine(columns, const_values):
    combinations = []

    def dfs(i, constructing):
        if i >= len(columns):
            combinations.append(constructing)
            return
        column = columns[i]
        column_values = const_values[column]
        for value in column_values:
            dfs(i + 1, constructing + [value])
    dfs(0, [])
    return combinations


col = ['a', 'b', 'c']
vals = {'a': [1, 2, 3], 'b': [4, 5], 'c': [1], 'd': []}

print(combine(col, vals))
print(combine([], {}))
