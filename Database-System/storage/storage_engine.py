"""存储引擎 - 整合页面管理、缓存管理和磁盘管理，提供统一的存储接口"""

import json
import struct
from pathlib import Path
from typing import Any, Dict, List, Optional

from .buffer_manager import BufferManager, ReplacementPolicy
from .disk_manager import DiskManager
from .page_manager import Page


class Record:
    """数据记录类"""

    def __init__(self, values: List[Any]):
        self.values = values

    def to_bytes(self) -> bytes:
        """序列化为字节数据"""
        data = json.dumps(self.values, ensure_ascii=False)
        return data.encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Record":
        """从字节数据反序列化"""
        json_str = data.decode("utf-8")
        values = json.loads(json_str)
        return cls(values)

    def __repr__(self):
        return f"Record({self.values})"


class TableMetadata:
    """表元数据"""

    def __init__(
        self, table_name: str, column_names: List[str], column_types: List[str]
    ):
        self.table_name = table_name
        self.column_names = column_names
        self.column_types = column_types
        self.page_ids: List[int] = []
        self.record_count = 0

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "table_name": self.table_name,
            "column_names": self.column_names,
            "column_types": self.column_types,
            "page_ids": self.page_ids,
            "record_count": self.record_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableMetadata":
        """从字典创建"""
        metadata = cls(data["table_name"], data["column_names"], data["column_types"])
        metadata.page_ids = data.get("page_ids", [])
        metadata.record_count = data.get("record_count", 0)
        return metadata

    def __repr__(self):
        col_count = len(self.column_names)
        return f"TableMetadata({self.table_name}, {col_count} columns)"


class StorageEngine:
    """存储引擎"""

    def __init__(
        self,
        db_path: str,
        buffer_size: int = 64,
        replacement_policy: ReplacementPolicy = (ReplacementPolicy.LRU),
    ):
        self.db_path = Path(db_path)
        self.meta_path = self.db_path.with_suffix(self.db_path.suffix + ".meta.json")
        self.disk_manager = DiskManager(str(self.db_path))
        self.buffer_manager = BufferManager(
            self.disk_manager, buffer_size, replacement_policy
        )

        # 表元数据存储
        self.table_metadata: Dict[str, TableMetadata] = {}
        self.metadata_page_id = -1  # 元数据页面ID，-1表示未初始化

        # 初始化或加载元数据
        self._initialize_metadata()

    def _initialize_metadata(self):
        """初始化或加载元数据"""
        try:
            # 优先从侧边元数据文件加载（更稳健的持久化）
            if self.meta_path.exists():
                try:
                    with open(self.meta_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.from_dict(data)
                except Exception:
                    # 侧边文件损坏则回退到页内元数据
                    pass
            # 如果元数据页面ID未设置，先尝试获取第一个分配的页面
            if self.metadata_page_id == -1:
                # 尝试读取第一个页面作为元数据页面
                first_page_data = self.disk_manager.read_page(0)
                if first_page_data:
                    self.metadata_page_id = 0
                else:
                    # 第一个页面不存在，创建新的元数据页面
                    self._create_metadata_page()
                    return

            # 尝试从元数据页加载
            metadata_page = self.buffer_manager.get_page(self.metadata_page_id)
            if metadata_page:
                self._load_metadata(metadata_page)
            else:
                # 创建新的元数据页
                self._create_metadata_page()
        except Exception as e:
            print(f"Warning: Failed to initialize metadata: {e}")
            # 如果加载失败，创建新的元数据页
            self._create_metadata_page()

    def _create_metadata_page(self):
        """创建元数据页面"""
        page_id = self.disk_manager.allocate_page()
        self.metadata_page_id = page_id  # 使用实际分配的页面ID

        # 创建空的元数据
        empty_metadata = {}
        data = json.dumps(empty_metadata, ensure_ascii=False).encode("utf-8")

        # 创建一个新的页面对象来写入初始数据
        page = Page(self.metadata_page_id)
        page.page_type = "metadata"

        # 写入元数据到页面
        if len(data) > 0:
            page.write_data(0, data)

        # 将页面数据写入磁盘
        page_bytes = page.to_bytes()
        self.disk_manager.write_page(self.metadata_page_id, page_bytes)

    def _save_metadata(self):
        """保存元数据到磁盘"""
        metadata_dict = {}
        for table_name, metadata in self.table_metadata.items():
            metadata_dict[table_name] = metadata.to_dict()

        data = json.dumps(metadata_dict, ensure_ascii=False).encode("utf-8")

        # 获取或创建元数据页面
        page = self.buffer_manager.get_page(self.metadata_page_id)
        if not page:
            # 创建新页面
            page = Page(self.metadata_page_id)
            page.page_type = "metadata"

        # 清空页面数据区域并写入新的元数据
        # 使用 Page 提供的接口而不是访问内部字段
        page.clear_data()
        data_region = page.get_data_region()
        if len(data) > 0 and len(data) < len(data_region):
            page.write_data(0, data)

        # 写入磁盘
        page_bytes = page.to_bytes()
        self.disk_manager.write_page(self.metadata_page_id, page_bytes)

        # 同步写入侧边元数据文件，确保持久化可靠
        try:
            with open(self.meta_path, 'w', encoding='utf-8') as f:
                json.dump({name: md.to_dict() for name, md in self.table_metadata.items()}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Warning: Failed to write meta file: {e}")

    def from_dict(self, data: Dict[str, Any]):
        """从字典恢复表元数据（用于侧边元数据文件加载）"""
        self.table_metadata.clear()
        tables = data or {}
        for name, md in tables.items():
            try:
                self.table_metadata[name.lower()] = TableMetadata.from_dict(md)
            except Exception:
                continue

    def _load_metadata(self, page: Page):
        """从页面加载元数据"""
        try:
            data_region = page.get_data_region()
            json_data = data_region.rstrip(b"\x00").decode("utf-8")

            if json_data:
                metadata_dict = json.loads(json_data)

                self.table_metadata.clear()
                for table_name, table_data in metadata_dict.items():
                    metadata = TableMetadata.from_dict(table_data)
                    self.table_metadata[table_name] = metadata
        except Exception as e:
            print(f"Warning: Failed to load metadata: {e}")
            self.table_metadata.clear()

    def create_table(
        self, table_name: str, column_names: List[str], column_types: List[str]
    ) -> bool:
        """创建表"""
        name_key = table_name.lower()
        if name_key in self.table_metadata:
            return False  # 表已存在

        # 创建表元数据
        metadata = TableMetadata(name_key, column_names, column_types)
        self.table_metadata[name_key] = metadata

        # 保存元数据
        self._save_metadata()

        return True

    def drop_table(self, table_name: str) -> bool:
        """删除表"""
        name_key = table_name.lower()
        if name_key not in self.table_metadata:
            return False  # 表不存在

        metadata = self.table_metadata[name_key]

        # 释放表的所有页面
        for page_id in metadata.page_ids:
            self.disk_manager.deallocate_page(page_id)

        # 删除元数据
        del self.table_metadata[name_key]

        # 保存元数据
        self._save_metadata()

        return True

    def insert_record(self, table_name: str, values: List[Any]) -> bool:
        """插入记录"""
        name_key = table_name.lower()
        if name_key not in self.table_metadata:
            return False  # 表不存在

        metadata = self.table_metadata[name_key]
        record = Record(values)
        record_data = record.to_bytes()

        # 如果记录太大，无法存储
        if len(record_data) > self.disk_manager.PAGE_SIZE - 100:
            return False

        # 尝试在现有页面中插入
        for page_id in metadata.page_ids:
            page = self.buffer_manager.pin_page(page_id)
            if page and self._try_insert_to_page(page, record_data):
                self.buffer_manager.unpin_page(page_id, True)  # 标记为脏页
                metadata.record_count += 1
                self._save_metadata()
                return True
            if page:
                self.buffer_manager.unpin_page(page_id, False)

        # 需要分配新页面
        new_page_id = self.disk_manager.allocate_page()
        # 调试：记录新页ID
        if os.environ.get('MINIDB_DEBUG') == '1':
            print(f"[DEBUG] allocate_page -> {new_page_id} for table {name_key}")
        if new_page_id < 0:
            return False  # 分配失败

        metadata.page_ids.append(new_page_id)

        # 创建新页面并初始化
        new_page = Page(new_page_id)
        new_page.page_type = "data"

        # 将初始化的页面写入磁盘
        page_bytes = new_page.to_bytes()
        self.disk_manager.write_page(new_page_id, page_bytes)

        # 直接在新页面中插入记录，不依赖缓存管理器
        if self._try_insert_to_page(new_page, record_data):
            # 将修改后的页面写回磁盘
            updated_page_bytes = new_page.to_bytes()
            if self.disk_manager.write_page(new_page_id, updated_page_bytes):
                metadata.record_count += 1
                self._save_metadata()
                if os.environ.get('MINIDB_DEBUG') == '1':
                    print(f"[DEBUG] insert into new page {new_page_id}, total pages: {len(metadata.page_ids)}, record_count: {metadata.record_count}")
                return True

        return False

    def _try_insert_to_page(self, page: Page, record_data: bytes) -> bool:
        """尝试在页面中插入记录"""
        record_size = len(record_data)
        size_header = 4  # 记录大小头部

        if page.get_free_space() < record_size + size_header:
            return False  # 空间不足

        # 计算插入位置
        data_region = page.get_data_region()

        # 找到第一个空位置（简化版本，在末尾添加）
        used_space = self._calculate_used_space(data_region)
        insert_offset = used_space

        # 写入记录大小
        size_bytes = struct.pack("I", record_size)
        if not page.write_data(insert_offset, size_bytes):
            return False

        # 写入记录数据
        if not page.write_data(insert_offset + 4, record_data):
            return False

        return True

    def _calculate_used_space(self, data_region: bytearray) -> int:
        """计算页面中已使用的空间"""
        offset = 0

        while offset < len(data_region) - 4:
            try:
                record_size_bytes = data_region[offset : offset + 4]
                if record_size_bytes == b"\x00\x00\x00\x00":
                    break  # 遇到空记录

                record_size = struct.unpack("I", record_size_bytes)[0]
                if record_size == 0 or record_size > len(data_region):
                    break  # 无效记录大小

                offset += 4 + record_size
            except Exception:
                break

        return offset

    def scan_table(self, table_name: str) -> List[Record]:
        """扫描表中的所有记录"""
        name_key = table_name.lower()
        if name_key not in self.table_metadata:
            return []  # 表不存在

        metadata = self.table_metadata[name_key]
        if os.environ.get('MINIDB_DEBUG') == '1':
            print(f"[DEBUG] scan table {name_key}, pages: {metadata.page_ids}, record_count: {metadata.record_count}")
        records = []

        for page_id in metadata.page_ids:
            page = self.buffer_manager.pin_page(page_id)
            if page:
                page_records = self._scan_page(page)
                records.extend(page_records)
                self.buffer_manager.unpin_page(page_id, False)

        return records

    def _scan_page(self, page: Page) -> List[Record]:
        """扫描页面中的所有记录"""
        records = []
        data_region = page.get_data_region()
        offset = 0

        while offset < len(data_region) - 4:
            try:
                record_size_bytes = data_region[offset : offset + 4]
                if record_size_bytes == b"\x00\x00\x00\x00":
                    break  # 遇到空记录

                record_size = struct.unpack("I", record_size_bytes)[0]
                if record_size == 0 or record_size > len(data_region) - offset - 4:
                    break  # 无效记录大小

                record_data = data_region[offset + 4 : offset + 4 + record_size]
                record = Record.from_bytes(record_data)
                records.append(record)

                offset += 4 + record_size
            except Exception:
                break

        return records

    def get_table_info(self, table_name: str) -> Optional[TableMetadata]:
        """获取表信息"""
        return self.table_metadata.get(table_name.lower())

    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        return list(self.table_metadata.keys())

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name.lower() in self.table_metadata

    def get_statistics(self) -> Dict[str, Any]:
        """获取存储引擎统计信息"""
        total_records = sum(meta.record_count for meta in self.table_metadata.values())
        total_pages = sum(len(meta.page_ids) for meta in self.table_metadata.values())

        buffer_stats = self.buffer_manager.get_buffer_stats()

        return {
            "tables_count": len(self.table_metadata),
            "total_records": total_records,
            "total_pages": total_pages,
            "database_size": self.disk_manager.get_db_size(),
            "buffer_stats": buffer_stats,
        }

    def clear_table(self, table_name: str) -> bool:
        """清空表中的所有数据"""
        try:
            if table_name not in self.table_metadata:
                raise ValueError(f"表 '{table_name}' 不存在")

            # 获取表元数据
            metadata = self.table_metadata[table_name]

            # 释放所有数据页面
            for page_id in metadata.page_ids:
                # 将页面内容清空
                page = self.buffer_manager.get_page(page_id)
                page.clear_data()
                # page.mark_dirty() 已经在 clear_data() 方法中调用了

            # 重置表元数据
            metadata.page_ids.clear()
            metadata.record_count = 0

            # 保存元数据
            self._save_metadata()

            return True

        except Exception as e:
            print(f"清空表失败: {e}")
            return False

    def flush(self):
        """刷新所有数据到磁盘"""
        self._save_metadata()
        self.buffer_manager.flush_all_pages()
        self.disk_manager.flush()

    def close(self):
        """关闭存储引擎"""
        self.flush()
        self.buffer_manager.shutdown()
        self.disk_manager.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """测试用例"""
    import os

    db_path = "test_storage.db"

    # 清理测试文件
    if os.path.exists(db_path):
        os.remove(db_path)

    print("测试存储引擎...")
    print("=" * 50)

    with StorageEngine(db_path, buffer_size=8) as storage:
        # 创建表
        print("1. 创建表...")
        success = storage.create_table(
            "student", ["id", "name", "age"], ["INT", "VARCHAR", "INT"]
        )
        print(f"   创建表 student: {'成功' if success else '失败'}")

        # 插入数据
        print("\n2. 插入数据...")
        test_records = [
            [1, "Alice", 20],
            [2, "Bob", 22],
            [3, "Charlie", 19],
            [4, "Diana", 21],
        ]

        for i, record in enumerate(test_records):
            success = storage.insert_record("student", record)
            print(f"   插入记录 {i+1}: {'成功' if success else '失败'}")

        # 查询数据
        print("\n3. 查询数据...")
        records = storage.scan_table("student")
        print(f"   查询到 {len(records)} 条记录:")
        for record in records:
            print(f"     {record.values}")

        # 获取统计信息
        print("\n4. 统计信息:")
        stats = storage.get_statistics()
        print(f"   表数量: {stats['tables_count']}")
        print(f"   记录总数: {stats['total_records']}")
        print(f"   页面总数: {stats['total_pages']}")
        print(f"   数据库大小: {stats['database_size']} bytes")
        print(f"   缓存命中率: {stats['buffer_stats']['hit_rate']:.2%}")

    print("\n测试完成!")


if __name__ == "__main__":
    main()
