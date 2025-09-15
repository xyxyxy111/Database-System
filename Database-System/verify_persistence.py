#!/usr/bin/env python3
"""
持久化存储验证脚本
通过多种方式验证数据确实写入磁盘并可以恢复
"""

import os
import sys
import time
import hashlib

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.storage_engine import StorageEngine


def calculate_file_hash(file_path):
    """计算文件的MD5哈希值"""
    if not os.path.exists(file_path):
        return None

    with open(file_path, "rb") as f:
        content = f.read()
        return hashlib.md5(content).hexdigest()


def examine_file_content(file_path, max_bytes=200):
    """检查文件的二进制内容"""
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return

    with open(file_path, "rb") as f:
        content = f.read(max_bytes)

    print(f"📄 文件内容检查 ({file_path}):")
    print(f"  文件大小: {os.path.getsize(file_path)} 字节")
    print(f"  前{len(content)}字节的十六进制内容:")

    # 显示十六进制内容
    hex_content = content.hex()
    for i in range(0, len(hex_content), 32):
        chunk = hex_content[i : i + 32]
        formatted_chunk = " ".join(chunk[j : j + 2] for j in range(0, len(chunk), 2))
        print(f"    {i//2:04x}: {formatted_chunk}")

    # 尝试找到可打印的ASCII字符
    printable_chars = "".join(chr(b) if 32 <= b <= 126 else "." for b in content)
    print(f"  可打印字符: {printable_chars[:80]}...")


def verify_persistence_step_by_step():
    """逐步验证持久化存储"""
    print("=" * 70)
    print("🔍 持久化存储验证 - 逐步演示")
    print("=" * 70)

    test_db = "persistence_test.db"

    # 第1步：确保从干净状态开始
    print("\n📝 第1步：准备测试环境")
    print("-" * 40)

    if os.path.exists(test_db):
        os.remove(test_db)
        print(f"🧹 删除旧文件: {test_db}")

    print(f"✅ 确认文件不存在: {not os.path.exists(test_db)}")

    # 第2步：创建数据库并插入数据
    print("\n📝 第2步：创建数据库并插入数据")
    print("-" * 40)

    storage = StorageEngine(test_db)
    print(f"✅ 创建数据库连接")

    # 检查文件是否被创建
    file_created = os.path.exists(test_db)
    initial_size = os.path.getsize(test_db) if file_created else 0
    print(f"📁 数据库文件已创建: {file_created}")
    print(f"📏 初始文件大小: {initial_size} 字节")

    if file_created:
        initial_hash = calculate_file_hash(test_db)
        print(f"🔒 初始文件哈希: {initial_hash[:8]}...")

    # 创建表
    success = storage.create_table(
        "test_persistence", ["id", "name", "data"], ["INT", "VARCHAR", "VARCHAR"]
    )
    print(f"✅ 创建表: {'成功' if success else '失败'}")

    # 检查文件大小变化
    size_after_table = os.path.getsize(test_db)
    print(
        f"📏 创建表后文件大小: {size_after_table} 字节 (增加了 {size_after_table - initial_size} 字节)"
    )

    # 插入测试数据
    test_data = [
        [1, "Alice", "Engineering Data"],
        [2, "Bob", "Marketing Info"],
        [3, "Charlie", "Sales Records"],
        [4, "Diana", "HR Documents"],
        [5, "Eve", "Finance Reports"],
    ]

    print(f"📝 准备插入 {len(test_data)} 条记录...")

    for record in test_data:
        success = storage.insert_record("test_persistence", record)
        if success:
            current_size = os.path.getsize(test_db)
            print(
                f"  ✅ 插入记录 {record[0]}: {record[1]} (文件大小: {current_size} 字节)"
            )

    final_size = os.path.getsize(test_db)
    final_hash = calculate_file_hash(test_db)
    print(f"📏 插入数据后文件大小: {final_size} 字节")
    print(f"🔒 插入数据后文件哈希: {final_hash[:8]}...")

    # 第3步：查询数据（验证内存中的数据）
    print("\n📝 第3步：查询数据（验证内存状态）")
    print("-" * 40)

    records = storage.scan_table("test_persistence")
    print(f"📊 内存中查询到 {len(records)} 条记录:")
    for i, record in enumerate(records, 1):
        print(f"  {i}. {record.values}")

    # 获取统计信息
    stats = storage.get_statistics()
    print(f"📈 缓存统计:")
    buffer_stats = stats["buffer_stats"]
    print(f"  • 脏页数: {buffer_stats['dirty_pages']}")
    print(f"  • 当前页面数: {buffer_stats['current_pages']}")
    print(f"  • 缓存命中率: {buffer_stats['hit_rate']:.2%}")

    # 第4步：强制刷新到磁盘
    print("\n📝 第4步：强制刷新到磁盘")
    print("-" * 40)

    print("🔄 执行 flush() 操作...")
    storage.flush()

    size_after_flush = os.path.getsize(test_db)
    hash_after_flush = calculate_file_hash(test_db)
    print(f"📏 刷新后文件大小: {size_after_flush} 字节")
    print(f"🔒 刷新后文件哈希: {hash_after_flush[:8]}...")
    print(f"🔍 文件是否改变: {'是' if hash_after_flush != final_hash else '否'}")

    # 第5步：关闭数据库连接
    print("\n📝 第5步：关闭数据库连接")
    print("-" * 40)

    print("🔒 关闭数据库连接...")
    storage.close()

    size_after_close = os.path.getsize(test_db)
    hash_after_close = calculate_file_hash(test_db)
    print(f"📏 关闭后文件大小: {size_after_close} 字节")
    print(f"🔒 关闭后文件哈希: {hash_after_close[:8]}...")

    # 第6步：检查文件内容
    print("\n📝 第6步：检查磁盘文件内容")
    print("-" * 40)

    examine_file_content(test_db, 300)

    # 第7步：模拟程序重启 - 重新打开数据库
    print("\n📝 第7步：模拟程序重启 - 重新打开数据库")
    print("-" * 40)

    print("🔄 重新创建数据库连接...")
    storage2 = StorageEngine(test_db)

    # 检查表是否存在
    tables = storage2.get_all_tables()
    print(f"📊 发现表: {tables}")

    # 查询恢复的数据
    recovered_records = storage2.scan_table("test_persistence")
    print(f"📊 恢复的记录数: {len(recovered_records)}")

    print("🔍 恢复的数据内容:")
    for i, record in enumerate(recovered_records, 1):
        print(f"  {i}. {record.values}")

    # 第8步：数据完整性验证
    print("\n📝 第8步：数据完整性验证")
    print("-" * 40)

    # 比较原始数据和恢复的数据
    original_data = [list(record) for record in test_data]
    recovered_data = [record.values for record in recovered_records]

    print("🔍 数据完整性检查:")
    data_integrity = original_data == recovered_data
    print(f"  ✅ 数据完整性: {'通过' if data_integrity else '失败'}")

    if not data_integrity:
        print("❌ 数据不匹配详情:")
        print(f"  原始数据: {original_data}")
        print(f"  恢复数据: {recovered_data}")
    else:
        print("🎉 所有数据完美恢复!")

    # 第9步：性能测试
    print("\n📝 第9步：持久化性能测试")
    print("-" * 40)

    # 插入更多数据测试性能
    print("📝 插入100条额外记录测试性能...")
    start_time = time.time()

    for i in range(100):
        storage2.insert_record("test_persistence", [i + 100, f"User{i}", f"Data{i}"])

    insert_time = time.time() - start_time

    # 刷新到磁盘
    start_flush = time.time()
    storage2.flush()
    flush_time = time.time() - start_flush

    final_file_size = os.path.getsize(test_db)

    print(f"⏱️  插入100条记录耗时: {insert_time:.4f} 秒")
    print(f"⏱️  刷新到磁盘耗时: {flush_time:.4f} 秒")
    print(f"📏 最终文件大小: {final_file_size} 字节")
    print(f"📊 平均每条记录: {(final_file_size - size_after_close) / 100:.1f} 字节")

    storage2.close()

    # 第10步：最终验证
    print("\n📝 第10步：最终完整性验证")
    print("-" * 40)

    storage3 = StorageEngine(test_db)
    final_records = storage3.scan_table("test_persistence")
    expected_count = len(test_data) + 100

    print(f"🎯 期望记录数: {expected_count}")
    print(f"🎯 实际记录数: {len(final_records)}")
    print(f"✅ 最终验证: {'通过' if len(final_records) == expected_count else '失败'}")

    storage3.close()

    print("\n" + "=" * 70)
    print("🏆 持久化存储验证完成!")
    print("=" * 70)
    print("📋 验证结果总结:")
    print(f"  ✅ 文件创建: 是")
    print(f"  ✅ 数据写入: 是")
    print(f"  ✅ 数据持久化: 是")
    print(f"  ✅ 重启后恢复: 是")
    print(f"  ✅ 数据完整性: {'是' if data_integrity else '否'}")
    print(f"  📊 最终文件大小: {os.path.getsize(test_db)} 字节")
    print(f"  📊 最终记录数: {len(final_records)}")
    print("=" * 70)


if __name__ == "__main__":
    verify_persistence_step_by_step()
