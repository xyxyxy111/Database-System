from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class ColumnInfo:
    """字段元数据"""

    name: str
    data_type: str
    size: Optional[int] = None
    is_nullable: bool = True

    def __repr__(self):
        if self.size:
            return f"{self.name} {self.data_type}({self.size})"
        return f"{self.name} {self.data_type}"


@dataclass
class TableInfo:
    """表的元信息"""

    name: str
    columns: List[ColumnInfo]

    def get_column(self, column_name: str) -> Optional[ColumnInfo]:
        """根据字段名查找对应的列对象"""
        for col in self.columns:
            if col.name.upper() == column_name.upper():
                return col
        return None

    def get_column_names(self) -> List[str]:
        """返回该表所有字段名"""
        return [col.name for col in self.columns]

    def __repr__(self):
        return f"TableInfo({self.name}, {self.columns})"


class Catalog:
    """元信息目录管理器"""

    def __init__(self):
        self._tables: Dict[str, TableInfo] = {}

    def create_table(self, table_name: str, columns: List[ColumnInfo]) -> bool:
        """新增表定义"""
        key = table_name.upper()
        if key in self._tables:
            return False  # 已存在同名表
        self._tables[key] = TableInfo(table_name, columns)
        return True

    def drop_table(self, table_name: str) -> bool:
        """移除表定义"""
        key = table_name.upper()
        if key not in self._tables:
            return False
        del self._tables[key]
        return True

    def table_exists(self, table_name: str) -> bool:
        """判断某表是否在目录中"""
        return table_name.upper() in self._tables

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """检查某字段是否在指定表中"""
        table_info = self.get_table_info(table_name)
        return bool(table_info and table_info.get_column(column_name))

    def get_table_info(self, table_name: str) -> Optional[TableInfo]:
        """返回表的完整元信息"""
        return self._tables.get(table_name.upper())

    def get_column_info(
        self, table_name: str, column_name: str
    ) -> Optional[ColumnInfo]:
        """返回指定字段的定义"""
        table_info = self.get_table_info(table_name)
        return table_info.get_column(column_name) if table_info else None

    def get_all_tables(self) -> List[str]:
        """获取所有表名（逻辑名）"""
        return [tbl.name for tbl in self._tables.values()]

    def validate_columns(
        self, table_name: str, column_names: List[str]
    ) -> Tuple[bool, str]:
        """确认一组字段是否都存在于该表"""
        table_info = self.get_table_info(table_name)
        if not table_info:
            return False, f"Table '{table_name}' not found"

        valid_names = {col.name.upper() for col in table_info.columns}
        for col_name in column_names:
            if col_name.upper() not in valid_names:
                return (
                    False,
                    f"Column '{col_name}' not found in table '{table_name}'",
                )

        return True, ""

    def get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """获取字段类型"""
        col_info = self.get_column_info(table_name, column_name)
        return col_info.data_type if col_info else None

    def clear(self):
        """清空所有表定义"""
        self._tables.clear()

    def to_dict(self) -> Dict:
        """序列化为字典"""
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

    def from_dict(self, data: Dict):
        """根据字典数据恢复目录"""
        self._tables.clear()
        for tbl_name, tbl_data in data.items():
            cols = [
                ColumnInfo(
                    name=c["name"],
                    data_type=c["data_type"],
                    size=c.get("size"),
                    is_nullable=c.get("is_nullable", True),
                )
                for c in tbl_data["columns"]
            ]
            self._tables[tbl_name] = TableInfo(tbl_data["name"], cols)

    def __repr__(self):
        return f"Catalog({list(self._tables.keys())})"
