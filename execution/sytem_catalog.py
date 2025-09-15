import os
import json
from typing import Dict, List, Optional, Tuple


class SystemCatalog:
    def __init__(self, db_file: str) -> None:
        self.db_file = db_file
        self.catalog_file = db_file + ".catalog"
        self.tables: Dict[str, List[Tuple[str, str]]] = {}
        self._load_catalog()

    def _load_catalog(self) -> None:
        if os.path.exists(self.catalog_file):
            try:
                with open(self.catalog_file, 'r') as f:
                    self.tables = json.load(f)
            except:
                self.tables = {}

    def _save_catalog(self) -> None:
        with open(self.catalog_file, 'w') as f:
            json.dump(self.tables, f)

    def table_exists(self, name: str) -> bool:
        return name in self.tables

    def create_table(self, name: str, columns: List[Tuple[str, str]]) -> None:
        if self.table_exists(name):
            raise ValueError(f"Table '{name}' already exists")
        self.tables[name] = columns
        self._save_catalog()

    def get_schema(self, name: str) -> List[Tuple[str, str]]:
        if not self.table_exists(name):
            raise ValueError(f"Table '{name}' does not exist")
        return self.tables[name]

    def get_table(self, name: str) -> 'Table':
        from storage.table import Table
        if not self.table_exists(name):
            raise ValueError(f"Table '{name}' does not exist")
        return Table(name, self.get_schema(name))

    def drop_table(self, table_name: str) -> None:
        """从系统目录中删除表，并删除对应的数据文件"""
        if table_name in self.tables:
            # 从内存中删除表定义
            del self.tables[table_name]

            # 删除对应的数据文件
            data_file = f"data/{table_name}.dat"
            if os.path.exists(data_file):
                try:
                    os.remove(data_file)
                    print(f"Deleted data file: {data_file}")
                except OSError as e:
                    print(f"Warning: Could not delete data file {data_file}: {e}")

            # 保存更新后的目录
            self._save_catalog()
            print(f"Table '{table_name}' dropped successfully")
        else:
            print(f"Table '{table_name}' does not exist in catalog")