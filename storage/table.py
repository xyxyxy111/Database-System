"""
表存储模块 (Table Storage)
=========================

本模块实现了数据库的表存储功能。
表是数据库的基本存储单位，由多个页组成。

主要功能：
1. 表的创建和管理
2. 行的插入和删除
3. 表的顺序扫描
4. 页的管理和分配

表结构：
- 使用堆表结构（Heap Table）
- 页顺序追加，支持动态增长
- 每页属于特定表，通过table_name标识
- 页0作为超级页，存储元数据
"""

from typing import Any, Dict, Iterable, List, Optional, Callable

from storage.buffer_manager import BufferManager
from storage.page import Page


class Table:
    """
    表类
    
    实现堆表存储，支持行的插入、扫描和删除。
    表由多个页组成，页顺序追加，支持动态增长。
    """
    
    def __init__(self, buffer: BufferManager, name: str) -> None:
        """
        初始化表
        
        参数:
            buffer (BufferManager): 缓冲管理器
            name (str): 表名
            
        功能:
            - 初始化表的基本属性
            - 如果数据库为空，创建超级页（页0）
        """
        self.buffer = buffer  # 缓冲管理器
        self.name = name      # 表名
        
        # 如果数据库为空，创建超级页（页0）
        if self.buffer.disk.num_pages() == 0:
            page = self.buffer.new_page()  # 分配页0作为超级页
            page.table_name = "__meta__"
            self.buffer.flush_page(0)

    def _iter_data_pages(self) -> Iterable[int]:
        """
        迭代属于当前表的数据页
        
        返回:
            Iterable[int]: 属于当前表的页ID迭代器
            
        功能:
            - 扫描所有页（从页1开始，页0是超级页）
            - 只返回属于当前表的页
        """
        total = self.buffer.disk.num_pages()
        for pid in range(1, total):
            page = self.buffer.get_page(pid)
            if page.table_name == self.name:
                yield pid

    def insert(self, row: Dict[str, Any]) -> None:
        """
        向表中插入一行数据
        
        参数:
            row (Dict[str, Any]): 要插入的行数据
            
        异常:
            ValueError: 当行数据超过单页大小时抛出
            
        功能:
            - 尝试向最后一个属于当前表的页插入
            - 如果页已满，分配新页
            - 自动刷新页到磁盘
        """
        # 找到最后一个属于当前表的页
        last_page_id = None
        for pid in self._iter_data_pages():
            last_page_id = pid
        
        # 尝试向最后一个页插入
        if last_page_id is not None:
            page = self.buffer.get_page(last_page_id)
            if page.insert_row(row):
                # 插入成功，刷新页
                self.buffer.flush_page(page.page_id)
                return
        
        # 需要分配新页
        page = self.buffer.new_page()
        page.table_name = self.name
        
        if not page.insert_row(row):
            # 罕见情况：一行超过页大小
            raise ValueError("row too large for a single page")
        
        # 刷新新页到磁盘
        self.buffer.flush_page(page.page_id)

    def scan(self) -> Iterable[Dict[str, Any]]:
        """
        扫描表中的所有行
        
        返回:
            Iterable[Dict[str, Any]]: 行数据迭代器
            
        功能:
            - 顺序扫描属于当前表的所有页
            - 逐页返回行数据
            - 使用生成器模式，节省内存
        """
        for pid in self._iter_data_pages():
            page = self.buffer.get_page(pid)
            for r in page.get_rows():
                yield r

    def delete(self, predicate: Optional[Callable[[Dict[str, Any]], bool]]) -> int:
        """
        从表中删除满足条件的行
        
        参数:
            predicate (Optional[Callable[[Dict[str, Any]], bool]]): 删除条件，
                                                                   None表示删除所有行
            
        返回:
            int: 删除的行数
            
        功能:
            - 扫描所有属于当前表的页
            - 根据谓词函数过滤行
            - 重写包含删除行的页
            - 返回删除的总行数
        """
        deleted = 0
        
        for pid in self._iter_data_pages():
            page = self.buffer.get_page(pid)
            rows = page.get_rows()
            
            if predicate is None:
                # 删除所有行
                kept: List[Dict[str, Any]] = []
                deleted += len(rows)
            else:
                # 根据谓词函数过滤行
                kept = []
                for r in rows:
                    if predicate(r):
                        deleted += 1  # 标记为删除
                    else:
                        kept.append(r)  # 保留
            
            # 如果页内容发生变化，重写页
            if len(kept) != len(rows):
                new_page = Page(pid, self.name, kept)
                self.buffer.cache[pid] = new_page
                self.buffer.flush_page(pid)
        
        return deleted
