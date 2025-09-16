"""存储子系统模块

提供完整的存储解决方案，包括：
- DiskManager: 磁盘I/O和页面分配管理
- PageManager: 页面级别的数据管理
- BufferManager: 缓存和内存管理
- StorageEngine: 统一的存储引擎接口
"""

from .disk_manager import DiskManager
from .page_manager import Page, PageManager
from .buffer_manager import BufferManager, ReplacementPolicy
from .storage_engine import StorageEngine, Record, TableMetadata

__all__ = [
    'DiskManager',
    'Page',
    'PageManager',
    'BufferManager',
    'ReplacementPolicy',
    'StorageEngine',
    'Record',
    'TableMetadata'
]
