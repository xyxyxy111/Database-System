"""磁盘管理器 - 负责处理磁盘I/O操作，管理数据页的持久化存储"""

import os
import struct
from typing import Optional, Set
from pathlib import Path


class DiskManager:
    """磁盘管理器"""
    
    PAGE_SIZE = 4096  # 页面大小：4KB
    HEADER_SIZE = 8   # 页面头部大小
    
    def __init__(self, db_file_path: str):
        self.db_file_path = Path(db_file_path)
        self.file_handle: Optional[object] = None
        self.next_page_id = 0
        self.free_pages: Set[int] = set()
        
        # 确保数据库文件目录存在
        self.db_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 初始化或打开数据库文件
        self._initialize_db_file()
    
    def _initialize_db_file(self):
        """初始化或打开数据库文件"""
        if not self.db_file_path.exists():
            # 创建新的数据库文件
            with open(self.db_file_path, 'wb') as f:
                # 写入文件头（包含下一个页面ID和空闲页面数量）
                header = struct.pack('II', 0, 0)  # next_page_id, free_page_count
                f.write(header)
            self.next_page_id = 0
        else:
            # 读取已存在文件的元数据
            with open(self.db_file_path, 'rb') as f:
                header = f.read(8)
                if len(header) == 8:
                    self.next_page_id, free_page_count = struct.unpack('II', header)
                    # 读取空闲页面列表
                    for _ in range(free_page_count):
                        page_id_data = f.read(4)
                        if len(page_id_data) == 4:
                            page_id = struct.unpack('I', page_id_data)[0]
                            self.free_pages.add(page_id)
    
    def allocate_page(self) -> int:
        """分配一个新页面，返回页面ID"""
        if self.free_pages:
            # 重用空闲页面
            page_id = self.free_pages.pop()
        else:
            # 分配新页面
            page_id = self.next_page_id
            self.next_page_id += 1
        
        # 更新文件头
        self._update_file_header()
        
        # 初始化页面（写入空页面）
        empty_page = b'\x00' * self.PAGE_SIZE
        self._write_page_to_disk(page_id, empty_page)
        
        return page_id
    
    def deallocate_page(self, page_id: int):
        """释放页面"""
        if page_id >= 0:
            self.free_pages.add(page_id)
            self._update_file_header()
    
    def read_page(self, page_id: int) -> Optional[bytes]:
        """从磁盘读取页面"""
        try:
            with open(self.db_file_path, 'rb') as f:
                # 计算页面在文件中的偏移量
                offset = self._get_page_offset(page_id)
                f.seek(offset)
                
                page_data = f.read(self.PAGE_SIZE)
                if len(page_data) == self.PAGE_SIZE:
                    return page_data
                else:
                    return None
        except (OSError, IOError):
            return None
    
    def write_page(self, page_id: int, page_data: bytes) -> bool:
        """将页面写入磁盘"""
        if len(page_data) != self.PAGE_SIZE:
            return False
        
        return self._write_page_to_disk(page_id, page_data)
    
    def _write_page_to_disk(self, page_id: int, page_data: bytes) -> bool:
        """内部方法：将页面数据写入磁盘"""
        try:
            with open(self.db_file_path, 'r+b') as f:
                offset = self._get_page_offset(page_id)
                f.seek(offset)
                f.write(page_data)
                f.flush()
                return True
        except (OSError, IOError):
            return False
    
    def _get_page_offset(self, page_id: int) -> int:
        """计算页面在文件中的偏移量"""
        # 文件头部 + 空闲页面列表 + 页面数据
        header_size = 8 + len(self.free_pages) * 4
        return header_size + page_id * self.PAGE_SIZE
    
    def _update_file_header(self):
        """更新文件头部信息"""
        try:
            with open(self.db_file_path, 'r+b') as f:
                f.seek(0)
                header = struct.pack('II', self.next_page_id, len(self.free_pages))
                f.write(header)
                
                # 写入空闲页面列表
                for page_id in self.free_pages:
                    f.write(struct.pack('I', page_id))
                
                f.flush()
        except (OSError, IOError):
            pass
    
    def get_db_size(self) -> int:
        """获取数据库文件大小"""
        try:
            return os.path.getsize(self.db_file_path)
        except OSError:
            return 0
    
    def get_page_count(self) -> int:
        """获取总页面数"""
        return self.next_page_id
    
    def get_free_page_count(self) -> int:
        """获取空闲页面数"""
        return len(self.free_pages)
    
    def flush(self):
        """刷新所有缓冲的数据到磁盘"""
        self._update_file_header()
    
    def close(self):
        """关闭磁盘管理器"""
        self.flush()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """测试用例"""
    db_path = "test_db.db"
    
    print("测试磁盘管理器...")
    print("=" * 50)
    
    # 清理测试文件
    if os.path.exists(db_path):
        os.remove(db_path)
    
    with DiskManager(db_path) as dm:
        print(f"初始状态:")
        print(f"  页面总数: {dm.get_page_count()}")
        print(f"  空闲页面数: {dm.get_free_page_count()}")
        print(f"  数据库文件大小: {dm.get_db_size()} bytes")
        print()
        
        # 分配几个页面
        print("分配页面...")
        pages = []
        for i in range(3):
            page_id = dm.allocate_page()
            pages.append(page_id)
            print(f"  分配页面 {page_id}")
        
        print(f"分配后页面总数: {dm.get_page_count()}")
        print()
        
        # 写入数据
        print("写入数据...")
        for i, page_id in enumerate(pages):
            test_data = f"Page {page_id} test data".encode().ljust(dm.PAGE_SIZE, b'\x00')
            success = dm.write_page(page_id, test_data)
            print(f"  写入页面 {page_id}: {'成功' if success else '失败'}")
        print()
        
        # 读取数据
        print("读取数据...")
        for page_id in pages:
            page_data = dm.read_page(page_id)
            if page_data:
                content = page_data.decode().rstrip('\x00')
                print(f"  读取页面 {page_id}: {content}")
            else:
                print(f"  读取页面 {page_id}: 失败")
        print()
        
        # 释放一个页面
        print("释放页面...")
        released_page = pages[1]
        dm.deallocate_page(released_page)
        print(f"  释放页面 {released_page}")
        print(f"  空闲页面数: {dm.get_free_page_count()}")
        print()
        
        # 再次分配页面（应该重用释放的页面）
        print("再次分配页面...")
        new_page_id = dm.allocate_page()
        print(f"  分配页面 {new_page_id} (应该重用释放的页面 {released_page})")
        print(f"  页面总数: {dm.get_page_count()}")
        print(f"  空闲页面数: {dm.get_free_page_count()}")


if __name__ == "__main__":
    main()
