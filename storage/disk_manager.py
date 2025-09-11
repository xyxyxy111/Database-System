"""
磁盘管理器模块 (Disk Manager)
===========================

本模块负责管理数据库文件的磁盘存储。
它提供了页级别的磁盘I/O操作，包括页的读取、写入、分配和释放。

主要功能：
1. 页的磁盘读写操作
2. 页的分配和释放
3. 文件大小管理
4. 数据持久化

文件布局：
- 文件由连续的PAGE_SIZE大小的页组成
- 每个页在文件中有固定的偏移位置
- 页ID对应页在文件中的位置
"""

import os
from typing import Optional

from storage.page import PAGE_SIZE


class DiskManager:
    """
    磁盘管理器类
    
    负责管理数据库文件的磁盘存储操作。
    提供页级别的磁盘I/O接口，确保数据的持久化存储。
    """
    
    def __init__(self, file_path: str) -> None:
        """
        初始化磁盘管理器
        
        参数:
            file_path (str): 数据库文件路径
            
        功能:
            - 创建文件目录（如果不存在）
            - 创建空文件（如果不存在）
        """
        self.file_path = file_path
        
        # 创建文件目录（如果不存在）
        if os.path.dirname(file_path):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # 创建空文件（如果不存在）
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'wb') as f:
                f.truncate(0)

    def _file_size(self) -> int:
        """
        获取文件大小
        
        返回:
            int: 文件大小（字节）
        """
        return os.path.getsize(self.file_path)

    def num_pages(self) -> int:
        """
        计算文件中的页数
        
        返回:
            int: 文件中的页数
        """
        size = self._file_size()
        return size // PAGE_SIZE

    def read_page(self, page_id: int) -> bytes:
        """
        从磁盘读取指定页的数据
        
        参数:
            page_id (int): 要读取的页ID
            
        返回:
            bytes: 页的数据（PAGE_SIZE字节）
            
        功能:
            - 根据页ID计算文件偏移位置
            - 读取PAGE_SIZE字节的数据
            - 如果读取的数据不足PAGE_SIZE，用零字节填充
        """
        with open(self.file_path, 'rb') as f:
            # 计算页在文件中的偏移位置
            f.seek(page_id * PAGE_SIZE)
            # 读取页数据
            data = f.read(PAGE_SIZE)
            
            # 如果读取的数据不足PAGE_SIZE，用零字节填充
            if len(data) < PAGE_SIZE:
                data = data.ljust(PAGE_SIZE, b"\x00")
            
            return data

    def write_page(self, page_id: int, data: bytes) -> None:
        """
        将页数据写入磁盘
        
        参数:
            page_id (int): 要写入的页ID
            data (bytes): 要写入的数据，必须为PAGE_SIZE字节
            
        异常:
            ValueError: 当数据大小不等于PAGE_SIZE时抛出
            
        功能:
            - 验证数据大小
            - 根据页ID计算文件偏移位置
            - 写入数据并强制刷新到磁盘
        """
        # 验证数据大小
        if len(data) != PAGE_SIZE:
            raise ValueError("write_page requires data of PAGE_SIZE")
        
        with open(self.file_path, 'r+b') as f:
            # 计算页在文件中的偏移位置
            f.seek(page_id * PAGE_SIZE)
            # 写入数据
            f.write(data)
            # 强制刷新到磁盘
            f.flush()
            os.fsync(f.fileno())

    def allocate_page(self) -> int:
        """
        分配新页
        
        在文件末尾追加一个新页，并返回页ID。
        
        返回:
            int: 新分配的页ID
            
        功能:
            - 在文件末尾追加PAGE_SIZE字节的零填充
            - 返回新页的ID
            - 强制刷新到磁盘
        """
        # 计算新页的ID（当前页数）
        page_id = self.num_pages()
        
        # 在文件末尾追加新页
        with open(self.file_path, 'ab') as f:
            f.write(b"\x00" * PAGE_SIZE)
            f.flush()
            os.fsync(f.fileno())
        
        return page_id

    def free_page(self, page_id: int) -> None:
        """
        释放页（简化实现）
        
        参数:
            page_id (int): 要释放的页ID
            
        注意:
            这是一个简化实现，仅将页清零，不进行物理回收。
            真实系统会维护空闲列表，并可能在文件末尾进行裁剪。
            
        功能:
            - 将指定页的数据清零
            - 强制刷新到磁盘
        """
        with open(self.file_path, 'r+b') as f:
            # 计算页在文件中的偏移位置
            f.seek(page_id * PAGE_SIZE)
            # 写入零字节
            f.write(b"\x00" * PAGE_SIZE)
            f.flush()
            os.fsync(f.fileno())
