import pickle

def easy_tuple_serialize(python_tuple) -> bytes:
    return pickle.dumps(python_tuple)

def easy_tuple_deserialize(data) -> tuple:
    return pickle.loads(data)


