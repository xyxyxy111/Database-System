"""数据库目录管理器 - 管理数据库的元数据和表信息"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import json


@dataclass
class ColumnDefinition:
    """列定义"""
    name: str
    data_type: str
    nullable: bool = True
    default_value: Any = None
    primary_key: bool = False
    unique: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'name': self.name,
            'data_type': self.data_type,
            'nullable': self.nullable,
            'default_value': self.default_value,
            'primary_key': self.primary_key,
            'unique': self.unique
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnDefinition':
        """从字典创建"""
        return cls(
            name=data['name'],
            data_type=data['data_type'],
            nullable=data.get('nullable', True),
            default_value=data.get('default_value'),
            primary_key=data.get('primary_key', False),
            unique=data.get('unique', False)
        )


@dataclass
class TableDefinition:
    """表定义"""
    name: str
    columns: List[ColumnDefinition]
    created_at: str
    record_count: int = 0
    
    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """获取列定义"""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None
    
    def get_column_names(self) -> List[str]:
        """获取所有列名"""
        return [col.name for col in self.columns]
    
    def get_column_types(self) -> List[str]:
        """获取所有列类型"""
        return [col.data_type for col in self.columns]
    
    def get_primary_key_columns(self) -> List[str]:
        """获取主键列名"""
        return [col.name for col in self.columns if col.primary_key]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'name': self.name,
            'columns': [col.to_dict() for col in self.columns],
            'created_at': self.created_at,
            'record_count': self.record_count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableDefinition':
        """从字典创建"""
        columns = [ColumnDefinition.from_dict(col_data) 
                  for col_data in data['columns']]
        return cls(
            name=data['name'],
            columns=columns,
            created_at=data['created_at'],
            record_count=data.get('record_count', 0)
        )


class DatabaseCatalog:
    """数据库目录管理器"""
    
    def __init__(self):
        self.tables: Dict[str, TableDefinition] = {}
        self.sequences: Dict[str, int] = {}  # 自增序列
        self.indexes: Dict[str, List[str]] = {}  # 索引信息
        
    def create_table(self, table_def: TableDefinition) -> bool:
        """创建表"""
        table_name = table_def.name.lower()
        
        if table_name in self.tables:
            return False  # 表已存在
        
        # 验证列定义
        column_names = set()
        primary_keys = []
        
        for col in table_def.columns:
            # 检查重复列名
            col_name_lower = col.name.lower()
            if col_name_lower in column_names:
                return False  # 重复列名
            column_names.add(col_name_lower)
            
            # 收集主键列
            if col.primary_key:
                primary_keys.append(col.name)
        
        # 如果有主键，创建主键索引
        if primary_keys:
            index_name = f"pk_{table_name}"
            self.indexes[index_name] = primary_keys
        
        self.tables[table_name] = table_def
        return True
    
    def drop_table(self, table_name: str) -> bool:
        """删除表"""
        table_name = table_name.lower()
        
        if table_name not in self.tables:
            return False  # 表不存在
        
        # 删除相关索引
        indexes_to_remove = []
        for index_name, index_columns in self.indexes.items():
            if index_name.startswith(f"pk_{table_name}") or \
               index_name.startswith(f"idx_{table_name}_"):
                indexes_to_remove.append(index_name)
        
        for index_name in indexes_to_remove:
            del self.indexes[index_name]
        
        # 删除表定义
        del self.tables[table_name]
        return True
    
    def get_table(self, table_name: str) -> Optional[TableDefinition]:
        """获取表定义"""
        return self.tables.get(table_name.lower())
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name.lower() in self.tables
    
    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        return list(self.tables.keys())
    
    def update_record_count(self, table_name: str, count: int):
        """更新表的记录数"""
        table = self.get_table(table_name)
        if table:
            table.record_count = count
    
    def increment_record_count(self, table_name: str, increment: int = 1):
        """增加表的记录数"""
        table = self.get_table(table_name)
        if table:
            table.record_count += increment
    
    def validate_record(self, table_name: str, 
                       record_data: Dict[str, Any]) -> Tuple[bool, str]:
        """验证记录数据"""
        table = self.get_table(table_name)
        if not table:
            return False, f"表 '{table_name}' 不存在"
        
        # 检查所有必需的列
        for col in table.columns:
            if col.name not in record_data:
                if not col.nullable and col.default_value is None:
                    return False, f"列 '{col.name}' 不能为空"
                # 使用默认值
                if col.default_value is not None:
                    record_data[col.name] = col.default_value
        
        # 检查额外的列
        table_columns = {col.name.lower() for col in table.columns}
        for col_name in record_data.keys():
            if col_name.lower() not in table_columns:
                return False, f"未知列 '{col_name}'"
        
        # 数据类型验证
        for col in table.columns:
            if col.name in record_data:
                value = record_data[col.name]
                if not self._validate_data_type(value, col.data_type):
                    return False, (f"列 '{col.name}' 数据类型不匹配，"
                                 f"期望 {col.data_type}")
        
        return True, "验证通过"
    
    def _validate_data_type(self, value: Any, data_type: str) -> bool:
        """验证数据类型"""
        if value is None:
            return True  # NULL值总是有效的
        
        data_type = data_type.upper()
        
        if data_type == 'INT' or data_type == 'INTEGER':
            return isinstance(value, int)
        elif data_type == 'FLOAT' or data_type == 'REAL':
            return isinstance(value, (int, float))
        elif data_type in ['VARCHAR', 'TEXT', 'CHAR']:
            return isinstance(value, str)
        elif data_type == 'BOOLEAN':
            return isinstance(value, bool)
        else:
            return True  # 未知类型，允许通过
    
    def get_next_sequence_value(self, sequence_name: str) -> int:
        """获取序列的下一个值"""
        if sequence_name not in self.sequences:
            self.sequences[sequence_name] = 0
        
        self.sequences[sequence_name] += 1
        return self.sequences[sequence_name]
    
    def create_index(self, index_name: str, table_name: str, 
                    columns: List[str]) -> bool:
        """创建索引"""
        if index_name in self.indexes:
            return False  # 索引已存在
        
        table = self.get_table(table_name)
        if not table:
            return False  # 表不存在
        
        # 验证列是否存在
        table_columns = {col.name.lower() for col in table.columns}
        for col_name in columns:
            if col_name.lower() not in table_columns:
                return False  # 列不存在
        
        self.indexes[index_name] = columns
        return True
    
    def drop_index(self, index_name: str) -> bool:
        """删除索引"""
        if index_name not in self.indexes:
            return False  # 索引不存在
        
        del self.indexes[index_name]
        return True
    
    def get_indexes_for_table(self, table_name: str) -> List[Tuple[str, List[str]]]:
        """获取表的所有索引"""
        table_name = table_name.lower()
        table_indexes = []
        
        for index_name, columns in self.indexes.items():
            if (index_name.startswith(f"pk_{table_name}") or 
                index_name.startswith(f"idx_{table_name}_")):
                table_indexes.append((index_name, columns))
        
        return table_indexes
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return {
            'tables': {name: table.to_dict() 
                      for name, table in self.tables.items()},
            'sequences': self.sequences.copy(),
            'indexes': self.indexes.copy()
        }
    
    def from_dict(self, data: Dict[str, Any]):
        """从字典反序列化"""
        self.tables.clear()
        self.sequences.clear() 
        self.indexes.clear()
        
        # 恢复表定义
        if 'tables' in data:
            for name, table_data in data['tables'].items():
                self.tables[name] = TableDefinition.from_dict(table_data)
        
        # 恢复序列
        if 'sequences' in data:
            self.sequences.update(data['sequences'])
        
        # 恢复索引
        if 'indexes' in data:
            self.indexes.update(data['indexes'])
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取目录统计信息"""
        total_records = sum(table.record_count 
                           for table in self.tables.values())
        total_columns = sum(len(table.columns) 
                           for table in self.tables.values())
        
        return {
            'table_count': len(self.tables),
            'total_records': total_records,
            'total_columns': total_columns,
            'index_count': len(self.indexes),
            'sequence_count': len(self.sequences)
        }
    
    def __repr__(self):
        return f"DatabaseCatalog(tables={len(self.tables)}, indexes={len(self.indexes)})"


def main():
    """测试用例"""
    import datetime
    
    print("数据库目录管理器测试")
    print("=" * 30)
    
    # 创建目录管理器
    catalog = DatabaseCatalog()
    
    # 创建表定义
    user_columns = [
        ColumnDefinition("id", "INT", primary_key=True, nullable=False),
        ColumnDefinition("name", "VARCHAR", nullable=False),
        ColumnDefinition("email", "VARCHAR", unique=True),
        ColumnDefinition("age", "INT", default_value=0)
    ]
    
    user_table = TableDefinition(
        name="users",
        columns=user_columns,
        created_at=datetime.datetime.now().isoformat()
    )
    
    # 测试表创建
    print("1. 测试表创建...")
    success = catalog.create_table(user_table)
    print(f"   创建用户表: {'成功' if success else '失败'}")
    
    # 测试表查询
    print("2. 测试表查询...")
    table = catalog.get_table("users")
    if table:
        print(f"   表名: {table.name}")
        print(f"   列数: {len(table.columns)}")
        print(f"   主键列: {table.get_primary_key_columns()}")
    
    # 测试记录验证
    print("3. 测试记录验证...")
    test_record = {"id": 1, "name": "Alice", "email": "alice@example.com"}
    valid, message = catalog.validate_record("users", test_record.copy())
    print(f"   记录验证: {'通过' if valid else '失败'} - {message}")
    
    # 测试无效记录
    invalid_record = {"id": "not_int", "name": "Bob"}
    valid, message = catalog.validate_record("users", invalid_record.copy())
    print(f"   无效记录: {'通过' if valid else '失败'} - {message}")
    
    # 测试索引创建
    print("4. 测试索引管理...")
    success = catalog.create_index("idx_users_email", "users", ["email"])
    print(f"   创建邮箱索引: {'成功' if success else '失败'}")
    
    indexes = catalog.get_indexes_for_table("users")
    print(f"   用户表索引数: {len(indexes)}")
    
    # 测试统计信息
    print("5. 测试统计信息...")
    stats = catalog.get_statistics()
    print(f"   统计信息: {stats}")
    
    print("\n目录管理器测试完成!")


if __name__ == "__main__":
    main()
