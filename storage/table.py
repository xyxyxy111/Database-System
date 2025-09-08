from typing import Any, Dict, Iterable, List, Optional, Callable

from .buffer_manager import BufferManager
from .page import Page

class Table:
    """
    极简堆表：表由一组页构成，页顺序追加。支持 insert、scan、delete。
    不做 schema 校验，行使用 dict 表示。
    """

    def __init__(self, buffer: BufferManager, name: str) -> None:
        self.buffer = buffer
        self.name = name
        # 通过一个目录页 0 存储下一页 id（简化：固定第 0 页作为超级页）
        if self.buffer.disk.num_pages() == 0:
            self.buffer.new_page()  # 分配 page 0 作为超级页
            self.buffer.flush_page(0)

    def _iter_data_pages(self) -> Iterable[int]:
        # 页 1..N 为数据页
        total = self.buffer.disk.num_pages()
        for pid in range(1, total):
            yield pid

    def insert(self, row: Dict[str, Any]) -> None:
        # 尝试向最后一页插入，否则分配新页
        total = self.buffer.disk.num_pages()
        if total <= 1:
            page = self.buffer.new_page()
        else:
            page = self.buffer.get_page(total - 1)
            if not page.insert_row(row):
                page = self.buffer.new_page()
        if not page.insert_row(row):
            # 罕见情况：一行超过页大小
            raise ValueError("row too large for a single page")
        self.buffer.flush_page(page.page_id)

    def scan(self) -> Iterable[Dict[str, Any]]:
        for pid in self._iter_data_pages():
            page = self.buffer.get_page(pid)
            for r in page.get_rows():
                yield r

    def delete(self, predicate: Optional[Callable[[Dict[str, Any]], bool]]) -> int:
        deleted = 0
        for pid in self._iter_data_pages():
            page = self.buffer.get_page(pid)
            rows = page.get_rows()
            if predicate is None:
                kept: List[Dict[str, Any]] = []
            else:
                kept = []
                for r in rows:
                    if predicate(r):
                        deleted += 1
                    else:
                        kept.append(r)
            if len(kept) != len(rows):
                # 重新写该页
                new_page = Page(pid, kept)
                self.buffer.cache[pid] = new_page
                self.buffer.flush_page(pid)
        return deleted
