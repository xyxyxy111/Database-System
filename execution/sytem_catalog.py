"""
系统目录模块 (System Catalog)
============================

本模块实现了数据库的系统目录功能。
系统目录是数据库的元数据存储，记录所有表的结构信息。

主要功能：
1. 表结构管理：存储和查询表的列定义
2. 表存在性检查：检查表是否存在
3. 表对象管理：创建和获取表对象
4. 持久化存储：将目录信息持久化到磁盘

目录表：
- 使用特殊表"__catalog__"存储元数据
- 启动时自动加载已存在的表结构
- 创建表时自动更新目录信息
"""

from typing import Dict, List, Tuple

from storage.buffer_manager import BufferManager
from storage.disk_manager import DiskManager
from storage.table import Table

# 目录表名常量
CATALOG_TABLE = "__catalog__"


class SystemCatalog:
    """
    系统目录类
    
    管理数据库的元数据信息，包括表结构和表对象。
    系统目录是数据库的核心组件，连接存储层和执行层。
    """
    
    def __init__(self, db_path: str) -> None:
        """
        初始化系统目录
        
        参数:
            db_path (str): 数据库文件路径
            
        功能:
            - 创建磁盘管理器和缓冲管理器
            - 初始化表对象缓存
            - 初始化模式缓存
            - 加载已存在的表结构
        """
        self.disk = DiskManager(db_path)           # 磁盘管理器
        self.buffer = BufferManager(self.disk)     # 缓冲管理器
        self.tables: Dict[str, Table] = {}         # 表对象缓存
        self.schemas: Dict[str, List[Tuple[str, str]]] = {}  # 模式缓存
        
        # 初始化目录表
        cat = self.get_table(CATALOG_TABLE)
        
        # 载入已存在的模式
        for row in cat.scan():
            tname = row.get("table")
            cols = row.get("columns") or []
            if isinstance(cols, list):
                # 将列定义转换为(列名, 类型)元组列表
                self.schemas[tname] = [(c[0], c[1]) for c in cols]

    def get_table(self, name: str) -> Table:
        """
        获取表对象
        
        如果表对象不存在，则创建新的表对象。
        使用缓存机制避免重复创建。
        
        参数:
            name (str): 表名
            
        返回:
            Table: 表对象
        """
        if name not in self.tables:
            self.tables[name] = Table(self.buffer, name)
        return self.tables[name]

    def create_table(self, name: str, columns: List[Tuple[str, str]]) -> None:
        """
        创建新表
        
        在系统目录中注册新表的结构信息。
        
        参数:
            name (str): 表名
            columns (List[Tuple[str, str]]): 列定义列表，每个元组为(列名, 类型)
            
        异常:
            ValueError: 当表已存在时抛出
        """
        # 检查表是否已存在
        if name in self.schemas:
            raise ValueError(f"table {name} already exists")
        
        # 注册模式到缓存
        self.schemas[name] = columns
        
        # 写入目录表
        cat = self.get_table(CATALOG_TABLE)
        cat.insert({"table": name, "columns": columns})

    def table_exists(self, name: str) -> bool:
        """
        检查表是否存在
        
        参数:
            name (str): 表名
            
        返回:
            bool: 表存在返回True，否则返回False
        """
        return name in self.schemas

    def get_schema(self, name: str) -> List[Tuple[str, str]]:
        """
        获取表的模式（列定义）
        
        参数:
            name (str): 表名
            
        返回:
            List[Tuple[str, str]]: 列定义列表，每个元组为(列名, 类型)
        """
        return self.schemas.get(name, [])
