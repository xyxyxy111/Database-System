from typing import Dict, List, Tuple

from ..storage.buffer_manager import BufferManager
from ..storage.disk_manager import DiskManager
from ..storage.table import Table

CATALOG_TABLE = "__catalog__"

class SystemCatalog:
    def __init__(self, db_path: str) -> None:
        self.disk = DiskManager(db_path)
        self.buffer = BufferManager(self.disk)
        self.tables: Dict[str, Table] = {}
        self.schemas: Dict[str, List[Tuple[str, str]]] = {}
        # 初始化目录表
        cat = self.get_table(CATALOG_TABLE)
        # 载入已存在的模式
        for row in cat.scan():
            tname = row.get("table")
            cols = row.get("columns") or []
            if isinstance(cols, list):
                self.schemas[tname] = [(c[0], c[1]) for c in cols]

    def get_table(self, name: str) -> Table:
        if name not in self.tables:
            self.tables[name] = Table(self.buffer, name)
        return self.tables[name]

    def create_table(self, name: str, columns: List[Tuple[str, str]]) -> None:
        if name in self.schemas:
            raise ValueError(f"table {name} already exists")
        # 注册 schema 并写入目录
        self.schemas[name] = columns
        cat = self.get_table(CATALOG_TABLE)
        cat.insert({"table": name, "columns": columns})

    def table_exists(self, name: str) -> bool:
        return name in self.schemas

    def get_schema(self, name: str) -> List[Tuple[str, str]]:
        return self.schemas.get(name, [])
