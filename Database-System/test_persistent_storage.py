#!/usr/bin/env python3
"""
测试页式存储的持久化功能
"""

import os
import sys

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.storage_engine import StorageEngine, Record
from storage.disk_manager import DiskManager
from storage.page_manager import Page, PageManager
from storage.buffer_manager import BufferManager, ReplacementPolicy


def test_disk_manager():
    """测试磁盘管理器的基本功能"""
    print("=== 测试磁盘管理器 ===")

    # 清理测试文件
    test_file = "test_disk.db"
    if os.path.exists(test_file):
        os.remove(test_file)

    try:
        # 创建磁盘管理器
        disk_manager = DiskManager(test_file)
        print(f"✅ 创建磁盘管理器成功")

        # 分配页面
        page_id1 = disk_manager.allocate_page()
        page_id2 = disk_manager.allocate_page()
        print(f"✅ 分配页面: {page_id1}, {page_id2}")

        # 创建测试数据
        test_data = b"Hello, MiniDB! This is a test page data." + b"\x00" * (4096 - 41)

        # 写入页面
        success1 = disk_manager.write_page(page_id1, test_data)
        print(f"✅ 写入页面1: {'成功' if success1 else '失败'}")

        # 读取页面
        read_data = disk_manager.read_page(page_id1)
        print(f"✅ 读取页面1: {'成功' if read_data == test_data else '失败'}")

        # 验证文件大小
        file_size = os.path.getsize(test_file)
        print(f"✅ 数据库文件大小: {file_size} 字节")

        # 清理
        disk_manager.close()

    except Exception as e:
        print(f"❌ 磁盘管理器测试失败: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理测试文件
        if os.path.exists(test_file):
            os.remove(test_file)


def test_page_manager():
    """测试页面管理器功能"""
    print("\n=== 测试页面管理器 ===")

    try:
        # 创建页面管理器
        pm = PageManager()
        print("✅ 创建页面管理器成功")

        # 创建页面
        page1 = pm.create_page(1, "data")
        page2 = pm.create_page(2, "index")
        print(f"✅ 创建页面: {page1.page_id}, {page2.page_id}")

        # 写入数据
        test_data = b"Test data for page 1"
        page1.write_data(0, test_data)
        page1.mark_dirty()

        # 读取数据
        read_data = page1.read_data(0, len(test_data))
        print(f"✅ 数据读写: {'成功' if read_data == test_data else '失败'}")

        # 测试页面固定
        page1.pin()
        print(f"✅ 页面固定: {page1.is_pinned()}")

        page1.unpin()
        print(f"✅ 取消固定: {page1.is_pinned()}")

        # 获取统计信息
        stats = pm.get_statistics()
        print(f"✅ 统计信息: {stats}")

    except Exception as e:
        print(f"❌ 页面管理器测试失败: {e}")
        import traceback

        traceback.print_exc()


def test_buffer_manager():
    """测试缓存管理器功能"""
    print("\n=== 测试缓存管理器 ===")

    test_file = "test_buffer.db"
    if os.path.exists(test_file):
        os.remove(test_file)

    try:
        # 创建缓存管理器
        disk_manager = DiskManager(test_file)
        buffer_manager = BufferManager(disk_manager, buffer_size=3)
        print("✅ 创建缓存管理器成功")

        # 分配和获取页面
        page_id1 = disk_manager.allocate_page()
        page_id2 = disk_manager.allocate_page()
        page_id3 = disk_manager.allocate_page()

        page1 = buffer_manager.get_page(page_id1)
        page2 = buffer_manager.get_page(page_id2)
        page3 = buffer_manager.get_page(page_id3)

        print(f"✅ 获取页面: {page1.page_id}, {page2.page_id}, {page3.page_id}")

        # 写入数据
        page1.write_data(0, b"Data for page 1")
        page2.write_data(0, b"Data for page 2")
        page3.write_data(0, b"Data for page 3")

        # 标记为脏页
        page1.mark_dirty()
        page2.mark_dirty()
        page3.mark_dirty()

        # 固定页面测试
        buffer_manager.pin_page(page_id1)
        print(f"✅ 固定页面1: {page1.is_pinned()}")

        # 刷新所有页面
        buffer_manager.flush_all_pages()
        print("✅ 刷新所有页面完成")

        # 获取缓存统计
        stats = buffer_manager.get_buffer_stats()
        print(f"✅ 缓存统计: {stats}")

        # 清理
        buffer_manager.shutdown()
        disk_manager.close()

    except Exception as e:
        print(f"❌ 缓存管理器测试失败: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if os.path.exists(test_file):
            os.remove(test_file)


def test_storage_engine():
    """测试存储引擎的综合功能"""
    print("\n=== 测试存储引擎 ===")

    test_file = "test_storage_engine.db"
    if os.path.exists(test_file):
        os.remove(test_file)

    try:
        # 创建存储引擎
        storage = StorageEngine(test_file)
        print("✅ 创建存储引擎成功")

        # 创建表
        success = storage.create_table(
            "users", ["id", "name", "age"], ["INT", "VARCHAR", "INT"]
        )
        print(f"✅ 创建表: {'成功' if success else '失败'}")

        if success:
            # 插入记录 - 直接传入值列表，不是Record对象
            values1 = [1, "Alice", 25]
            values2 = [2, "Bob", 30]
            values3 = [3, "Charlie", 35]

            result1 = storage.insert_record("users", values1)
            result2 = storage.insert_record("users", values2)
            result3 = storage.insert_record("users", values3)

            print(f"✅ 插入记录: {result1}, {result2}, {result3}")

            # 扫描表
            records = storage.scan_table("users")
            print(f"✅ 扫描表获得 {len(records)} 条记录:")
            for record in records:
                print(f"    {record.values}")

            # 获取表信息
            table_info = storage.get_table_info("users")
            print(
                f"✅ 表信息: {table_info.table_name}, {len(table_info.column_names)} 列, {table_info.record_count} 记录"
            )

            # 获取统计信息
            stats = storage.get_statistics()
            print(f"✅ 存储统计: {stats}")

        # 刷新和关闭
        storage.flush()
        storage.close()
        print("✅ 存储引擎关闭成功")

        # 验证持久化 - 重新打开数据库
        print("\n--- 测试持久化 ---")
        storage2 = StorageEngine(test_file)

        # 检查表是否存在
        if storage2.table_exists("users"):
            print("✅ 表持久化成功")

            # 扫描数据
            records = storage2.scan_table("users")
            print(f"✅ 重新加载后获得 {len(records)} 条记录:")
            for record in records:
                print(f"    {record.values}")
        else:
            print("❌ 表持久化失败")

        storage2.close()

        # 检查文件大小
        file_size = os.path.getsize(test_file)
        print(f"✅ 数据库文件大小: {file_size} 字节")

    except Exception as e:
        print(f"❌ 存储引擎测试失败: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if os.path.exists(test_file):
            os.remove(test_file)


def test_page_persistence():
    """测试页面持久化的具体细节"""
    print("\n=== 测试页面持久化细节 ===")

    test_file = "test_page_persistence.db"
    if os.path.exists(test_file):
        os.remove(test_file)

    try:
        # 第一阶段：写入数据
        print("第一阶段：写入数据")
        disk_manager = DiskManager(test_file)

        # 创建页面并写入数据
        page_id = disk_manager.allocate_page()
        page = Page(page_id, "data")

        test_data1 = b"First record data"
        test_data2 = b"Second record data"

        page.write_data(0, test_data1)
        page.write_data(len(test_data1) + 4, test_data2)  # 4字节用于长度信息

        # 序列化页面并写入磁盘
        page_bytes = page.to_bytes()
        success = disk_manager.write_page(page_id, page_bytes)
        print(f"✅ 写入页面到磁盘: {'成功' if success else '失败'}")

        disk_manager.close()

        # 第二阶段：读取数据
        print("第二阶段：读取数据")
        disk_manager2 = DiskManager(test_file)

        # 从磁盘读取页面
        page_data = disk_manager2.read_page(page_id)
        if page_data:
            print("✅ 从磁盘读取页面成功")

            # 反序列化页面
            recovered_page = Page(page_id)
            recovered_page.from_bytes(page_data)

            # 读取数据
            read_data1 = recovered_page.read_data(0, len(test_data1))
            read_data2 = recovered_page.read_data(len(test_data1) + 4, len(test_data2))

            print(
                f"✅ 数据恢复: 数据1={'正确' if read_data1 == test_data1 else '错误'}"
            )
            print(
                f"✅ 数据恢复: 数据2={'正确' if read_data2 == test_data2 else '错误'}"
            )

            print(f"原始数据1: {test_data1}")
            print(f"恢复数据1: {read_data1}")
            print(f"原始数据2: {test_data2}")
            print(f"恢复数据2: {read_data2}")
        else:
            print("❌ 从磁盘读取页面失败")

        disk_manager2.close()

        # 显示文件内容（十六进制）
        with open(test_file, "rb") as f:
            content = f.read()
            print(f"✅ 文件大小: {len(content)} 字节")
            print(f"文件头部 (前64字节): {content[:64].hex()}")

    except Exception as e:
        print(f"❌ 页面持久化测试失败: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if os.path.exists(test_file):
            os.remove(test_file)


def main():
    """主测试函数"""
    print("MiniDB 页式存储持久化功能测试")
    print("=" * 50)

    # 运行各个组件的测试
    test_disk_manager()
    test_page_manager()
    test_buffer_manager()
    test_storage_engine()
    test_page_persistence()

    print("\n" + "=" * 50)
    print("页式存储测试完成!")


if __name__ == "__main__":
    main()
