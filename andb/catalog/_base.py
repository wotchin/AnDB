import os
import pickle
from abc import ABC, abstractmethod

from andb.constants.filename import CATALOG_DIR


class CatalogForm(ABC):
    __fields__ = {}

    def _extract_field_values(self, o):
        values = []
        for name in self.__fields__:
            values.append(getattr(o, name))
        return tuple(values)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return (self._extract_field_values(self) ==
                self._extract_field_values(other))

    def __hash__(self):
        return hash(self._extract_field_values(self))

    @abstractmethod
    def __lt__(self, other):
        pass

    def fields(self):
        return self.__fields__


class CatalogTable:
    __tablename__ = 'undefined'

    def __init__(self):
        assert self.__tablename__ != CatalogTable.__tablename__, \
            'require modification for __tablename__'
        self.rows = []

    @abstractmethod
    def init(self):
        # only called once when the catalog table is created
        pass

    def insert(self, row):
        assert isinstance(row, CatalogForm)
        # todo: binary search
        self.rows.append(row)
        self.rows.sort()
        self.save()

    def delete(self, lambda_condition):
        assert callable(lambda_condition)
        i = 0
        while i < len(self.rows):
            if lambda_condition(self.rows[i]):
                self.rows.pop(i)
                i -= 1
            i += 1
        self.save()

    def search(self, lambda_condition):
        assert callable(lambda_condition)
        results = []
        for r in self.rows:
            if lambda_condition(r):
                results.append(r)
        return results

    def update(self, old, new):
        assert isinstance(old, CatalogForm)
        assert isinstance(new, CatalogForm)

        i = 0
        while i < len(self.rows):
            if self.rows[i] == old:
                self.rows[i] = new
                i -= 1
            i += 1
        self.rows.sort()
        self.save()

    def load(self):
        filename = os.path.join(CATALOG_DIR, self.__tablename__)
        data = bytearray()
        with open(filename, 'rb') as f:
            while True:
                buff = f.read(256)
                if not buff:
                    break
                data += buff
        self.rows = pickle.loads(data)

    def save(self):
        filename = os.path.join(CATALOG_DIR, self.__tablename__)
        f = open(filename, 'w+b')
        f.write(pickle.dumps(self.rows))
        os.fsync(f.fileno())
        f.close()
