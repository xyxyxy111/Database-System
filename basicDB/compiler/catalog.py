"""
目录管理器
该模块主要用于维护数据库的元信息，例如表的定义、列属性等
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class ColumnInfo:
    """列的元数据"""

    name: str
    data_type: str
    size: Optional[int] = None
    is_nullable: bool = True

    def __repr__(self) -> str:
        return f"{self.name} {self.data_type}({self.size})" if self.size else f"{self.name} {self.data_type}"


@dataclass
class TableInfo:
    """单个表的定义信息"""

    name: str
    columns: List[ColumnInfo]

    def get_column(self, column_name: str) -> Optional[ColumnInfo]:
        """通过列名查找对应的列描述"""
        target = column_name.upper()
        for col in self.columns:
            if col.name.upper() == target:
                return col
        return None

    def get_column_names(self) -> List[str]:
        """返回当前表的所有列名"""
        return [col.name for col in self.columns]

    def __repr__(self) -> str:
        return f"TableInfo({self.name}, {self.columns})"


class Catalog:
    """数据库目录，用于记录表结构定义"""

    def __init__(self) -> None:
        self._tables: Dict[str, TableInfo] = {}

    def create_table(self, table_name: str, columns: List[ColumnInfo]) -> bool:
        """新建一张表，若已存在则返回 False"""
        key: str = table_name.upper()
        if key in self._tables:
            return False
        self._tables[key] = TableInfo(table_name, columns)
        return True

    def drop_table(self, table_name: str) -> bool:
        """删除指定表定义"""
        key: str = table_name.upper()
        if key not in self._tables:
            return False
        del self._tables[key]
        return True

    def table_exists(self, table_name: str) -> bool:
        """判断某个表是否存在"""
        return table_name.upper() in self._tables

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """判断某列是否存在"""
        tbl = self.get_table_info(table_name)
        return tbl.get_column(column_name) is not None if tbl else False

    def get_table_info(self, table_name: str) -> Optional[TableInfo]:
        """获取某表的结构描述"""
        return self._tables.get(table_name.upper())

    def get_column_info(self, table_name: str, column_name: str) -> Optional[ColumnInfo]:
        """获取列的定义信息"""
        tbl = self.get_table_info(table_name)
        return tbl.get_column(column_name) if tbl else None

    def get_tables(self) -> List[str]:
        """列出所有表名"""
        return [tbl.name for tbl in self._tables.values()]

    def validate_columns(self, table_name: str, column_names: List[str]) -> Tuple[bool, str]:
        """验证输入列是否在表中存在"""
        tbl = self.get_table_info(table_name)
        if not tbl:
            return False, f"Table '{table_name}' does not exist"

        available = {col.name.upper() for col in tbl.columns}
        for col in column_names:
            if col.upper() not in available:
                return False, f"Column '{col}' does not exist in table '{table_name}'"
        return True, ""

    def get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """获取列的数据类型"""
        col = self.get_column_info(table_name, column_name)
        return col.data_type if col else None

    def clear(self) -> None:
        """清除所有表定义"""
        self._tables.clear()

    def to_dict(self) -> Dict:
        """导出为字典格式，便于序列化"""
        return {
            tbl_name: {
                "name": tbl_info.name,
                "columns": [
                    {
                        "name": col.name,
                        "data_type": col.data_type,
                        "size": col.size,
                        "is_nullable": col.is_nullable,
                    }
                    for col in tbl_info.columns
                ],
            }
            for tbl_name, tbl_info in self._tables.items()
        }

    def from_dict(self, data: Dict) -> None:
        """从字典格式恢复表定义"""
        self._tables.clear()
        for tbl_name, tbl_data in data.items():
            cols: List[ColumnInfo] = [
                ColumnInfo(
                    name=col["name"],
                    data_type=col["data_type"],
                    size=col.get("size"),
                    is_nullable=col.get("is_nullable", True),
                )
                for col in tbl_data["columns"]
            ]
            self._tables[tbl_name] = TableInfo(tbl_data["name"], cols)

    def __repr__(self) -> str:
        return f"Catalog({list(self._tables.keys())})"
