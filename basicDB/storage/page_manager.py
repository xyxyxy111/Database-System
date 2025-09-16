"""页面管理器 - 管理内存中的数据页，提供页面的基本操作接口"""

import struct
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class PageHeader:
    """页面头部信息"""

    page_id: int
    page_type: str  # 'data', 'catalog', 'index'
    record_count: int
    free_space: int
    next_page: int = -1
    prev_page: int = -1


class Page:
    """数据页类"""

    HEADER_SIZE = 32  # 页面头部大小：24字节结构体 + 8字节页面类型

    def __init__(self, page_id: int, page_type: str = "data", page_size: int = 4096):
        self.page_id = page_id
        self.page_size = page_size
        self.header = PageHeader(
            page_id=page_id,
            page_type=page_type,
            record_count=0,
            free_space=page_size - self.HEADER_SIZE,
        )
        self.data = bytearray(page_size)
        self.dirty = False
        self.pin_count = 0
        self.last_access = datetime.now()

        # 初始化页面头部
        self._write_header()

    def _write_header(self):
        """将页面头部写入数据数组"""
        header_data = struct.pack(
            "IIIIii",
            self.header.page_id,
            len(self.header.page_type.encode()),
            self.header.record_count,
            self.header.free_space,
            self.header.next_page,
            self.header.prev_page,
        )

        # 页面类型字符串
        page_type_data = self.header.page_type.encode().ljust(8, b"\x00")

        # 写入头部：前24字节是结构体，接下来8字节是页面类型
        self.data[:24] = header_data
        self.data[24:32] = page_type_data

    def _read_header(self):
        """从数据数组读取页面头部"""
        header_data = self.data[:24]  # 读取完整的24字节头部
        page_id, page_type_len, record_count, free_space, next_page, prev_page = (
            struct.unpack("IIIIii", header_data)
        )

        # 读取页面类型
        page_type_data = self.data[24:32]  # 页面类型从24字节后开始
        page_type = page_type_data.rstrip(b"\x00").decode()

        self.header = PageHeader(
            page_id=page_id,
            page_type=page_type,
            record_count=record_count,
            free_space=free_space,
            next_page=next_page,
            prev_page=prev_page,
        )

    def get_data_start(self) -> int:
        """获取数据区域的起始位置"""
        return self.HEADER_SIZE

    def get_free_space(self) -> int:
        """获取可用空间大小"""
        return self.header.free_space

    def get_data_region(self) -> bytearray:
        """获取数据区域"""
        return self.data[self.get_data_start() :]

    def write_data(self, offset: int, data: bytes) -> bool:
        """在指定偏移量写入数据"""
        data_start = self.get_data_start()
        write_pos = data_start + offset

        if write_pos + len(data) > self.page_size:
            return False

        self.data[write_pos : write_pos + len(data)] = data
        self.dirty = True
        self.last_access = datetime.now()

        # 更新空闲空间
        used_space = offset + len(data)
        available_space = self.page_size - data_start
        self.header.free_space = max(0, available_space - used_space)
        self._write_header()

        return True

    def read_data(self, offset: int, length: int) -> Optional[bytes]:
        """从指定偏移量读取数据"""
        data_start = self.get_data_start()
        read_pos = data_start + offset

        if read_pos + length > self.page_size:
            return None

        self.last_access = datetime.now()
        return bytes(self.data[read_pos : read_pos + length])

    def pin(self):
        """固定页面（增加引用计数）"""
        self.pin_count += 1

    def unpin(self):
        """取消固定页面（减少引用计数）"""
        if self.pin_count > 0:
            self.pin_count -= 1

    def is_pinned(self) -> bool:
        """检查页面是否被固定"""
        return self.pin_count > 0

    def mark_dirty(self):
        """标记页面为脏页"""
        self.dirty = True

    def is_dirty(self) -> bool:
        """检查页面是否为脏页"""
        return self.dirty

    def clear_dirty(self):
        """清除脏页标记"""
        self.dirty = False

    def clear_data(self):
        """清除页面数据区域（保留头部）"""
        # 清除数据区域（从头部结束位置到页面末尾）
        data_start = self.get_data_start()
        self.data[data_start:] = bytearray(self.page_size - data_start)

        # 重置头部信息
        self.header.record_count = 0
        self.header.free_space = self.page_size - self.HEADER_SIZE

        # 更新头部并标记为脏页
        self._write_header()
        self.mark_dirty()
        self.last_access = datetime.now()

    def to_bytes(self) -> bytes:
        """转换为字节数据"""
        self._write_header()  # 确保头部数据是最新的
        return bytes(self.data)

    def from_bytes(self, data: bytes):
        """从字节数据恢复页面"""
        if len(data) != self.page_size:
            raise ValueError(f"Invalid page data size: {len(data)}")

        self.data = bytearray(data)
        self._read_header()
        self.last_access = datetime.now()

    def __repr__(self):
        return f"Page(id={self.page_id}, type={self.header.page_type}, records={self.header.record_count})"


class PageManager:
    """页面管理器"""

    def __init__(self, page_size: int = 4096):
        self.page_size = page_size
        self.pages: Dict[int, Page] = {}
        self.access_order: List[int] = []  # 用于LRU

    def create_page(self, page_id: int, page_type: str = "data") -> Page:
        """创建新页面"""
        if page_id in self.pages:
            raise ValueError(f"Page {page_id} already exists")

        page = Page(page_id, page_type, self.page_size)
        self.pages[page_id] = page
        self._update_access(page_id)

        return page

    def get_page(self, page_id: int) -> Optional[Page]:
        """获取页面"""
        page = self.pages.get(page_id)
        if page:
            self._update_access(page_id)
        return page

    def remove_page(self, page_id: int) -> bool:
        """移除页面"""
        if page_id not in self.pages:
            return False

        page = self.pages[page_id]
        if page.is_pinned():
            return False  # 不能移除被固定的页面

        del self.pages[page_id]
        if page_id in self.access_order:
            self.access_order.remove(page_id)

        return True

    def get_lru_page_id(self) -> Optional[int]:
        """获取最近最少使用的页面ID"""
        for page_id in self.access_order:
            page = self.pages.get(page_id)
            if page and not page.is_pinned():
                return page_id
        return None

    def _update_access(self, page_id: int):
        """更新页面访问顺序"""
        if page_id in self.access_order:
            self.access_order.remove(page_id)
        self.access_order.append(page_id)

    def get_dirty_pages(self) -> List[int]:
        """获取所有脏页ID"""
        return [page_id for page_id, page in self.pages.items() if page.is_dirty()]

    def get_pinned_pages(self) -> List[int]:
        """获取所有被固定的页面ID"""
        return [page_id for page_id, page in self.pages.items() if page.is_pinned()]

    def clear_all(self):
        """清除所有页面"""
        self.pages.clear()
        self.access_order.clear()

    def get_page_count(self) -> int:
        """获取页面数量"""
        return len(self.pages)

    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        total_pages = len(self.pages)
        dirty_pages = len(self.get_dirty_pages())
        pinned_pages = len(self.get_pinned_pages())

        page_types = {}
        for page in self.pages.values():
            page_type = page.header.page_type
            page_types[page_type] = page_types.get(page_type, 0) + 1

        return {
            "total_pages": total_pages,
            "dirty_pages": dirty_pages,
            "pinned_pages": pinned_pages,
            "page_types": page_types,
            "page_size": self.page_size,
        }


def main():
    """测试用例"""
    print("测试页面管理器...")
    print("=" * 50)

    pm = PageManager()

    # 创建几个页面
    print("创建页面...")
    page1 = pm.create_page(1, "data")
    page2 = pm.create_page(2, "catalog")
    page3 = pm.create_page(3, "data")

    print(f"创建了 {pm.get_page_count()} 个页面")
    print()

    # 写入数据
    print("写入数据...")
    test_data = b"Hello, MiniDB! This is test data for page 1."
    success = page1.write_data(0, test_data)
    print(f"写入页面1: {'成功' if success else '失败'}")

    # 读取数据
    print("读取数据...")
    read_data = page1.read_data(0, len(test_data))
    if read_data:
        print(f"从页面1读取: {read_data.decode()}")

    # 固定页面
    print("\n测试页面固定...")
    page1.pin()
    print(f"页面1是否被固定: {page1.is_pinned()}")
    print(f"页面1引用计数: {page1.pin_count}")

    # 获取统计信息
    print("\n统计信息:")
    stats = pm.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")

    # 测试LRU
    print("\n测试LRU...")
    print("访问顺序:", pm.access_order)
    lru_page = pm.get_lru_page_id()
    print(f"LRU页面: {lru_page}")

    # 清理
    page1.unpin()
    print(f"取消固定后页面1是否被固定: {page1.is_pinned()}")


if __name__ == "__main__":
    main()
