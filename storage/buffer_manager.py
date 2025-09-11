"""
缓冲管理器模块 (Buffer Manager)
==============================

本模块实现了数据库的缓冲管理功能。
缓冲管理器在内存中维护一个页缓存，减少磁盘I/O操作，提高数据库性能。

主要功能：
1. LRU（最近最少使用）页替换策略
2. 页的缓存和逐出
3. 缓存命中统计
4. 页的刷新和同步

缓存策略：
- 使用OrderedDict实现LRU算法
- 当缓存满时，逐出最久未使用的页
- 统计缓存命中、缺失和逐出次数
"""

from collections import OrderedDict
from typing import Dict, Tuple

from storage.disk_manager import DiskManager
from storage.page import Page, PAGE_SIZE


class BufferManager:
    """
    缓冲管理器类
    
    实现页级缓存管理，使用LRU替换策略。
    在内存中维护一个页缓存，减少磁盘I/O操作。
    """
    
    def __init__(self, disk: DiskManager, capacity: int = 64) -> None:
        """
        初始化缓冲管理器
        
        参数:
            disk (DiskManager): 磁盘管理器
            capacity (int): 缓存容量（页数），默认为64页
        """
        self.disk = disk                    # 磁盘管理器
        self.capacity = capacity            # 缓存容量
        self.cache: "OrderedDict[int, Page]" = OrderedDict()  # 页缓存
        self.hits = 0                       # 缓存命中次数
        self.misses = 0                     # 缓存缺失次数
        self.evictions = 0                  # 页逐出次数

    def _evict_if_needed(self) -> None:
        """
        在需要时逐出页
        
        当缓存超过容量限制时，逐出最久未使用的页（LRU策略）。
        逐出时会打印日志信息。
        """
        while len(self.cache) > self.capacity:
            # 逐出最久未使用的页（OrderedDict的第一个元素）
            pid, _ = self.cache.popitem(last=False)
            self.evictions += 1
            print(f"[Buffer] Evict page {pid}")

    def get_page(self, page_id: int) -> Page:
        """
        获取指定页
        
        如果页在缓存中，直接返回（缓存命中）。
        如果页不在缓存中，从磁盘读取并加入缓存（缓存缺失）。
        
        参数:
            page_id (int): 要获取的页ID
            
        返回:
            Page: 页对象
        """
        # 检查页是否在缓存中
        if page_id in self.cache:
            # 缓存命中：移除并重新添加到末尾（更新访问时间）
            page = self.cache.pop(page_id)
            self.cache[page_id] = page
            self.hits += 1
            return page
        
        # 缓存缺失：从磁盘读取
        self.misses += 1
        raw = self.disk.read_page(page_id)
        page = Page.from_bytes(raw)
        page.page_id = page_id
        
        # 加入缓存
        self.cache[page_id] = page
        self._evict_if_needed()
        
        return page

    def new_page(self) -> Page:
        """
        创建新页
        
        分配一个新的页ID，创建页对象并加入缓存。
        
        返回:
            Page: 新创建的页对象
        """
        # 从磁盘管理器分配新页
        page_id = self.disk.allocate_page()
        page = Page(page_id, "")
        
        # 加入缓存
        self.cache[page_id] = page
        self._evict_if_needed()
        
        return page

    def flush_page(self, page_id: int) -> None:
        """
        刷新指定页到磁盘
        
        将缓存中的页数据写入磁盘，确保数据持久化。
        
        参数:
            page_id (int): 要刷新的页ID
        """
        if page_id in self.cache:
            page = self.cache[page_id]
            self.disk.write_page(page_id, page.to_bytes())

    def flush_all(self) -> None:
        """
        刷新所有页到磁盘
        
        将缓存中的所有页数据写入磁盘，确保数据持久化。
        用于系统关闭前的数据保存。
        """
        for page_id, page in list(self.cache.items()):
            self.disk.write_page(page_id, page.to_bytes())

    def stats(self) -> Tuple[int, int, int]:
        """
        获取缓存统计信息
        
        返回:
            Tuple[int, int, int]: (命中次数, 缺失次数, 逐出次数)
        """
        return self.hits, self.misses, self.evictions
