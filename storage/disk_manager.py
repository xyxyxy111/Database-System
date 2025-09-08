import os
from typing import Optional

from .page import PAGE_SIZE

class DiskManager:
    """
    磁盘管理：固定大小页的读写与分配。
    文件布局：连续的 PAGE_SIZE 大小块。
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        os.makedirs(os.path.dirname(file_path), exist_ok=True) if os.path.dirname(file_path) else None
        # 若不存在则创建空文件
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'wb') as f:
                f.truncate(0)

    def _file_size(self) -> int:
        return os.path.getsize(self.file_path)

    def num_pages(self) -> int:
        size = self._file_size()
        return size // PAGE_SIZE

    def read_page(self, page_id: int) -> bytes:
        with open(self.file_path, 'rb') as f:
            f.seek(page_id * PAGE_SIZE)
            data = f.read(PAGE_SIZE)
            if len(data) < PAGE_SIZE:
                # 文件末尾的短读，补齐
                data = data.ljust(PAGE_SIZE, b"\x00")
            return data

    def write_page(self, page_id: int, data: bytes) -> None:
        if len(data) != PAGE_SIZE:
            raise ValueError("write_page requires data of PAGE_SIZE")
        with open(self.file_path, 'r+b') as f:
            f.seek(page_id * PAGE_SIZE)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

    def allocate_page(self) -> int:
        page_id = self.num_pages()
        # 追加一页 0 填充
        with open(self.file_path, 'ab') as f:
            f.write(b"\x00" * PAGE_SIZE)
            f.flush()
            os.fsync(f.fileno())
        return page_id

    def free_page(self, page_id: int) -> None:
        """
        占位：简化实现不做物理回收，仅将目标页清 0。
        真实系统会维护空闲列表并可能在文件尾裁剪。
        """
        with open(self.file_path, 'r+b') as f:
            f.seek(page_id * PAGE_SIZE)
            f.write(b"\x00" * PAGE_SIZE)
            f.flush()
            os.fsync(f.fileno())
