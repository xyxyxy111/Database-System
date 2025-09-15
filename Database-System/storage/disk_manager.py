"""磁盘管理器 - 负责页面级别的磁盘读写与页面分配

提供固定大小页面(Page)的顺序分配、读取与写入能力。
该简化实现不维护空闲列表，`deallocate_page` 为空操作。
"""

import os
from pathlib import Path
from typing import Optional


class DiskManager:
    """磁盘管理器

    - 页大小固定为 4096 字节
    - 通过偏移 (page_id * PAGE_SIZE) 定位页
    - 分配页面时在文件末尾追加一个空白页
    """

    PAGE_SIZE = 4096

    def __init__(self, db_path: str):
        """初始化磁盘管理器

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = Path(db_path)
        # 确保父目录存在
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 以二进制读写模式打开文件，不存在则创建
        self._file = open(self.db_path, "a+b")
        self._file.flush()

        # 如果文件大小不是页大小的倍数，向上补齐
        self._pad_to_page_boundary()

    def _pad_to_page_boundary(self):
        """将文件大小补齐到页大小的整数倍"""
        self._file.seek(0, os.SEEK_END)
        size = self._file.tell()
        remainder = size % self.PAGE_SIZE
        if remainder != 0:
            padding = self.PAGE_SIZE - remainder
            self._file.write(b"\x00" * padding)
            self._file.flush()

    def _page_offset(self, page_id: int) -> int:
        return page_id * self.PAGE_SIZE

    def allocate_page(self) -> int:
        """分配一个新页面，返回其 page_id"""
        self._file.seek(0, os.SEEK_END)
        size = self._file.tell()
        page_id = size // self.PAGE_SIZE
        # 在文件末尾追加一个空白页
        self._file.write(b"\x00" * self.PAGE_SIZE)
        self._file.flush()
        return page_id

    def deallocate_page(self, page_id: int) -> bool:
        """释放页面（简化实现：不回收空间，直接返回True）"""
        return True

    def read_page(self, page_id: int) -> Optional[bytes]:
        """读取指定页面的数据

        Returns:
            bytes: 固定大小页面数据；若超出文件范围则返回 None
        """
        offset = self._page_offset(page_id)
        self._file.seek(0, os.SEEK_END)
        size = self._file.tell()
        if offset + self.PAGE_SIZE > size:
            return None
        self._file.seek(offset, os.SEEK_SET)
        data = self._file.read(self.PAGE_SIZE)
        if len(data) != self.PAGE_SIZE:
            return None
        return data

    def write_page(self, page_id: int, data: bytes) -> bool:
        """将数据写入指定页面

        要求 data 长度不超过 PAGE_SIZE。若不足，将自动在页面内补齐。
        """
        if not data or len(data) > self.PAGE_SIZE:
            return False
        offset = self._page_offset(page_id)
        self._file.seek(offset, os.SEEK_SET)
        # 写入 data，并对剩余空间以 0 填充，确保整页写满
        if len(data) < self.PAGE_SIZE:
            data = data + b"\x00" * (self.PAGE_SIZE - len(data))
        written = self._file.write(data)
        self._file.flush()
        return written == self.PAGE_SIZE

    def get_db_size(self) -> int:
        """返回数据库文件大小（字节数）"""
        self._file.seek(0, os.SEEK_END)
        return self._file.tell()

    def flush(self):
        """刷新文件缓冲区到磁盘"""
        self._file.flush()

    def close(self):
        """关闭文件句柄"""
        try:
            self._file.flush()
        finally:
            self._file.close()


