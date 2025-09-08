from collections import OrderedDict
from typing import Dict, Tuple

from .disk_manager import DiskManager
from .page import Page, PAGE_SIZE

class BufferManager:
    """
    极简缓冲管理：维护一个 LRU 的页缓存，按需从磁盘装载和回写。
    统计命中与缺失；逐出时打印日志。
    """

    def __init__(self, disk: DiskManager, capacity: int = 64) -> None:
        self.disk = disk
        self.capacity = capacity
        self.cache: "OrderedDict[int, Page]" = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def _evict_if_needed(self) -> None:
        while len(self.cache) > self.capacity:
            pid, _ = self.cache.popitem(last=False)
            self.evictions += 1
            print(f"[Buffer] Evict page {pid}")

    def get_page(self, page_id: int) -> Page:
        if page_id in self.cache:
            page = self.cache.pop(page_id)
            self.cache[page_id] = page
            self.hits += 1
            return page
        self.misses += 1
        raw = self.disk.read_page(page_id)
        page = Page.from_bytes(raw)
        page.page_id = page_id
        self.cache[page_id] = page
        self._evict_if_needed()
        return page

    def new_page(self) -> Page:
        page_id = self.disk.allocate_page()
        page = Page(page_id)
        self.cache[page_id] = page
        self._evict_if_needed()
        return page

    def flush_page(self, page_id: int) -> None:
        if page_id in self.cache:
            page = self.cache[page_id]
            self.disk.write_page(page_id, page.to_bytes())

    def flush_all(self) -> None:
        for page_id, page in list(self.cache.items()):
            self.disk.write_page(page_id, page.to_bytes())

    def stats(self) -> Tuple[int, int, int]:
        return self.hits, self.misses, self.evictions
