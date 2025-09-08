from typing import Any, Dict, Iterable, Iterator, List, Optional, Callable

from ..storage.table import Table
from ..execution.sytem_catalog import SystemCatalog

Row = Dict[str, Any]

class Operator:
    def open(self) -> None:
        pass

    def next(self) -> Optional[Row]:
        raise NotImplementedError

    def close(self) -> None:
        pass

class SeqScan(Operator):
    def __init__(self, table: Table) -> None:
        self.table = table
        self._iter: Optional[Iterator[Row]] = None

    def open(self) -> None:
        self._iter = iter(self.table.scan())

    def next(self) -> Optional[Row]:
        assert self._iter is not None, "operator not opened"
        try:
            return next(self._iter)
        except StopIteration:
            return None

class Filter(Operator):
    def __init__(self, child: Operator, predicate: Callable[[Row], bool]) -> None:
        self.child = child
        self.predicate = predicate

    def open(self) -> None:
        self.child.open()

    def next(self) -> Optional[Row]:
        while True:
            row = self.child.next()
            if row is None:
                return None
            if self.predicate(row):
                return row

    def close(self) -> None:
        self.child.close()

class Project(Operator):
    def __init__(self, child: Operator, columns: List[str]) -> None:
        self.child = child
        self.columns = columns

    def open(self) -> None:
        self.child.open()

    def next(self) -> Optional[Row]:
        row = self.child.next()
        if row is None:
            return None
        return {col: row.get(col) for col in self.columns}

    def close(self) -> None:
        self.child.close()

class Insert(Operator):
    def __init__(self, table: Table, rows: Iterable[Row]) -> None:
        self.table = table
        self.rows = iter(rows)
        self._done = False

    def open(self) -> None:
        self._done = False

    def next(self) -> Optional[Row]:
        if self._done:
            return None
        count = 0
        for r in self.rows:
            self.table.insert(r)
            count += 1
        self._done = True
        return {"inserted": count}

    def close(self) -> None:
        pass

class CreateTable(Operator):
    def __init__(self, syscat: SystemCatalog, name: str, columns: List[tuple]) -> None:
        self.syscat = syscat
        self.name = name
        self.columns = columns
        self._done = False

    def open(self) -> None:
        self._done = False

    def next(self) -> Optional[Row]:
        if self._done:
            return None
        self.syscat.create_table(self.name, self.columns)
        self._done = True
        return {"created": self.name}

    def close(self) -> None:
        pass

class Delete(Operator):
    def __init__(self, table: Table, predicate: Optional[Callable[[Row], bool]]) -> None:
        self.table = table
        self.predicate = predicate
        self._done = False

    def open(self) -> None:
        self._done = False

    def next(self) -> Optional[Row]:
        if self._done:
            return None
        deleted = self.table.delete(self.predicate)
        self._done = True
        return {"deleted": deleted}

    def close(self) -> None:
        pass

# Helpers

def make_predicate(col: str, op: str, val: Any) -> Callable[[Row], bool]:
    if op == "EQ":
        return lambda r: r.get(col) == val
    if op == "NE":
        return lambda r: r.get(col) != val
    if op == "GT":
        return lambda r: (r.get(col) is not None) and (r.get(col) > val)
    if op == "LT":
        return lambda r: (r.get(col) is not None) and (r.get(col) < val)
    if op == "GE":
        return lambda r: (r.get(col) is not None) and (r.get(col) >= val)
    if op == "LE":
        return lambda r: (r.get(col) is not None) and (r.get(col) <= val)
    return lambda r: True
