"""
页存储模块(数据库的页式存储系统设计)
页是数据库存储的基本单位，每个页包含固定大小的数据。

主要功能：
1. 页的创建和管理
2. 数据的序列化和反序列化
3. 页容量的管理
4. 行的插入和检索

页格式：
- 固定大小4KB
- 使用Python pickle进行序列化
- 支持追加式插入和顺序扫描
- 包含页ID、表名和行数据
"""

import pickle
from typing import Any, Dict, List, Optional, Tuple

# 页大小常量：4KB
PAGE_SIZE = 4096


class Page:
    """
    页类
    表示数据库中的一个存储页。每个页包含：
    - page_id: 页的唯一标识符
    - table_name: 页所属的表名
    - rows: 页中存储的行数据列表

    页使用Python pickle进行序列化，确保数据可以持久化存储。
    """

    def __init__(self, page_id: int, table_name: str = "", rows: Optional[List[Dict[str, Any]]] = None) -> None:
        """
        初始化页
        参数:
            page_id (int): 页的唯一标识符
            table_name (str): 页所属的表名，默认为空字符串
            rows (Optional[List[Dict[str, Any]]]): 页中的行数据，默认为空列表
        """
        self.page_id = page_id        # 页ID
        self.table_name = table_name  # 表名
        self.rows: List[Dict[str, Any]] = rows or []  # 行数据列表

    def capacity_left(self) -> int:
        """
        计算页的剩余容量

        通过序列化当前数据来计算剩余可用空间。

        返回:
            int: 剩余字节数
        """
        # 构造要序列化的数据结构
        data = {
            "page_id": self.page_id,
            "table_name": self.table_name,
            "rows": self.rows
        }
        # 序列化并计算大小
        raw = pickle.dumps(data)
        return PAGE_SIZE - len(raw)

    def can_insert(self, row: Dict[str, Any]) -> bool:
        """
        检查是否可以插入新行

        通过模拟插入操作来检查页是否有足够空间。

        参数:
            row (Dict[str, Any]): 要插入的行数据

        返回:
            bool: 如果可以插入返回True，否则返回False
        """
        # 构造包含新行的数据结构
        data = {
            "page_id": self.page_id,
            "table_name": self.table_name,
            "rows": self.rows + [row]  # 添加新行
        }
        # 序列化并检查大小
        raw = pickle.dumps(data)
        return len(raw) <= PAGE_SIZE

    def insert_row(self, row: Dict[str, Any]) -> bool:
        """
        向页中插入一行数据

        参数:
            row (Dict[str, Any]): 要插入的行数据

        返回:
            bool: 插入成功返回True，否则返回False
        """
        # 检查是否可以插入
        if self.can_insert(row):
            self.rows.append(row)
            return True
        return False

    def get_rows(self) -> List[Dict[str, Any]]:
        """
        获取页中的所有行数据

        返回:
            List[Dict[str, Any]]: 行数据列表的副本
        """
        return list(self.rows)  # 返回副本，避免外部修改

    def to_bytes(self) -> bytes:
        """
        将页序列化为字节数组

        将页数据序列化为固定大小的字节数组，用于磁盘存储。
        如果数据超过页大小，会抛出异常。

        返回:
            bytes: 序列化后的字节数组，长度为PAGE_SIZE

        异常:
            ValueError: 当序列化数据超过页大小时抛出
        """
        # 构造要序列化的数据结构
        data = {
            "page_id": self.page_id,
            "table_name": self.table_name,
            "rows": self.rows
        }
        # 序列化
        raw = pickle.dumps(data)

        # 检查大小
        if len(raw) > PAGE_SIZE:
            raise ValueError(
                "Page overflow: serialized data exceeds PAGE_SIZE")

        # 用零字节填充到固定大小
        return raw.ljust(PAGE_SIZE, b"\x00")

    @staticmethod
    def from_bytes(raw: bytes) -> "Page":
        """
        从字节数组反序列化页

        从磁盘读取的字节数组反序列化为页对象。

        参数:
            raw (bytes): 要反序列化的字节数组

        返回:
            Page: 反序列化后的页对象
        """
        # 去除右侧的填充零字节
        payload = raw.rstrip(b"\x00")

        # 如果是空页（新分配的页）
        if not payload:
            return Page(page_id=-1, table_name="", rows=[])

        # 反序列化数据
        data = pickle.loads(payload)

        # 创建页对象
        page = Page(
            page_id=int(data.get("page_id", -1)),
            table_name=str(data.get("table_name", "")),
            rows=list(data.get("rows", []))
        )
        return page
