#!/usr/bin/env python3
"""
模拟用户场景：创建表、插入数据、关闭终端、重新启动查询
验证数据持久化问题
"""

import os
import sys
import time

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.storage_engine import StorageEngine


def simulate_user_workflow():
    """模拟用户的实际使用场景"""
    print("=" * 60)
    print("🎭 模拟用户工作流程 - 持久化问题诊断")
    print("=" * 60)

    db_file = "user_test.db"

    # 清理旧文件
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"🧹 清理旧文件: {db_file}")

    print("\n📝 场景1: 用户第一次使用")
    print("-" * 40)

    # 第一次会话：创建表和插入数据
    print("1. 创建数据库连接...")
    storage1 = StorageEngine(db_file)

    print("2. 创建表...")
    success = storage1.create_table(
        "students", ["id", "name", "age"], ["INT", "VARCHAR", "INT"]
    )
    print(f"   创建表结果: {'✅ 成功' if success else '❌ 失败'}")

    print("3. 插入数据...")
    test_data = [[1, "张三", 20], [2, "李四", 21], [3, "王五", 19]]

    for record in test_data:
        success = storage1.insert_record("students", record)
        print(f"   插入 {record}: {'✅ 成功' if success else '❌ 失败'}")

    print("4. 验证内存中的数据...")
    records = storage1.scan_table("students")
    print(f"   查询到 {len(records)} 条记录:")
    for record in records:
        print(f"     {record.values}")

    # 检查文件状态
    file_size_before_close = os.path.getsize(db_file)
    print(f"5. 关闭前文件大小: {file_size_before_close} 字节")

    # 模拟用户的不同关闭方式
    print("\n🔄 测试不同的关闭方式:")
    print("-" * 40)

    # 方式1: 直接关闭（没有调用flush）
    print("方式1: 直接关闭连接（可能会丢失数据）")
    # storage1.close()  # 这会调用flush，我们先不用

    # 方式2: 正确关闭
    print("方式2: 手动刷新后关闭")
    storage1.flush()  # 手动刷新
    storage1.close()  # 关闭连接

    file_size_after_close = os.path.getsize(db_file)
    print(f"   关闭后文件大小: {file_size_after_close} 字节")
    print(f"   文件大小变化: {file_size_after_close - file_size_before_close} 字节")

    print("\n📝 场景2: 模拟终端关闭，重新启动")
    print("-" * 40)

    # 等待一下模拟时间间隔
    print("⏱️  模拟时间间隔（关闭终端，重新启动）...")
    time.sleep(1)

    # 第二次会话：重新打开数据库
    print("1. 重新创建数据库连接...")
    storage2 = StorageEngine(db_file)

    print("2. 检查表是否存在...")
    tables = storage2.get_all_tables()
    print(f"   发现的表: {tables}")

    if "students" in tables:
        print("✅ 表存在，尝试查询数据...")
        recovered_records = storage2.scan_table("students")
        print(f"   恢复的记录数: {len(recovered_records)}")

        if len(recovered_records) > 0:
            print("   恢复的数据:")
            for i, record in enumerate(recovered_records, 1):
                print(f"     {i}. {record.values}")

            # 验证数据完整性
            original_data = [record for record in test_data]
            recovered_data = [record.values for record in recovered_records]

            if original_data == recovered_data:
                print("✅ 数据完整性验证通过！")
            else:
                print("❌ 数据完整性验证失败！")
                print(f"   原始数据: {original_data}")
                print(f"   恢复数据: {recovered_data}")
        else:
            print("❌ 没有恢复到任何数据！")
    else:
        print("❌ 表不存在！数据可能丢失了！")

    storage2.close()

    print("\n📝 场景3: 测试错误的关闭方式")
    print("-" * 40)

    # 清理重新开始
    if os.path.exists(db_file):
        os.remove(db_file)

    print("1. 重新创建数据库...")
    storage3 = StorageEngine(db_file)
    storage3.create_table("test_table", ["id", "data"], ["INT", "VARCHAR"])
    storage3.insert_record("test_table", [1, "test_data"])

    print("2. 不调用flush直接关闭（模拟异常退出）...")
    # 注意：我们不调用flush()，直接关闭
    storage3.buffer_manager.shutdown()
    storage3.disk_manager.close()

    print("3. 重新打开检查数据是否丢失...")
    storage4 = StorageEngine(db_file)
    tables = storage4.get_all_tables()

    if "test_table" in tables:
        records = storage4.scan_table("test_table")
        print(f"   数据恢复情况: {len(records)} 条记录")
        if len(records) == 0:
            print("❌ 数据丢失！没有调用flush导致数据未写入磁盘")
        else:
            print("✅ 数据仍然存在（自动写入机制工作）")
    else:
        print("❌ 表结构都丢失了！")

    storage4.close()


def diagnose_persistence_issue():
    """诊断持久化问题的可能原因"""
    print("\n" + "=" * 60)
    print("🔍 持久化问题诊断")
    print("=" * 60)

    print("\n可能的问题原因:")
    print("1. 📝 数据没有正确刷新到磁盘")
    print("   - 解决方案: 确保调用 storage.flush() 或 storage.close()")

    print("\n2. 🗂️  文件路径问题")
    print("   - 数据库文件可能创建在不同的目录")
    print("   - 解决方案: 使用绝对路径")

    print("\n3. 🔄 重新启动时没有正确加载")
    print("   - 元数据加载失败")
    print("   - 解决方案: 检查_initialize_metadata方法")

    print("\n4. 💾 缓冲区管理问题")
    print("   - 脏页没有写入磁盘")
    print("   - 解决方案: 检查BufferManager的flush逻辑")

    print("\n📋 推荐的使用方式:")
    print(
        """
    # 正确的使用方式
    storage = StorageEngine("my_database.db")
    try:
        # 创建表和插入数据
        storage.create_table("users", ["id", "name"], ["INT", "VARCHAR"])
        storage.insert_record("users", [1, "Alice"])
        
        # 重要：确保数据写入磁盘
        storage.flush()
    finally:
        # 关闭连接（会自动调用flush）
        storage.close()
    """
    )


def test_manual_flush_requirement():
    """测试是否需要手动调用flush"""
    print("\n" + "=" * 60)
    print("🧪 测试手动flush的必要性")
    print("=" * 60)

    test_db = "flush_test.db"

    # 清理
    if os.path.exists(test_db):
        os.remove(test_db)

    print("\n测试1: 不调用flush，只调用close()")
    print("-" * 40)

    storage = StorageEngine(test_db)
    storage.create_table("test1", ["id", "name"], ["INT", "VARCHAR"])
    storage.insert_record("test1", [1, "Alice"])

    # 只调用close（close方法会自动调用flush）
    storage.close()

    # 重新打开验证
    storage2 = StorageEngine(test_db)
    records = storage2.scan_table("test1")
    print(f"恢复的记录数: {len(records)}")
    print(f"结果: {'✅ 成功' if len(records) > 0 else '❌ 失败'}")
    storage2.close()

    print("\n测试2: 不调用flush，也不调用close()")
    print("-" * 40)

    # 清理重新开始
    if os.path.exists(test_db):
        os.remove(test_db)

    storage3 = StorageEngine(test_db)
    storage3.create_table("test2", ["id", "name"], ["INT", "VARCHAR"])
    storage3.insert_record("test2", [1, "Bob"])

    # 直接退出，不调用任何清理方法
    del storage3  # 模拟程序异常退出

    # 重新打开验证
    storage4 = StorageEngine(test_db)
    tables = storage4.get_all_tables()
    if "test2" in tables:
        records = storage4.scan_table("test2")
        print(f"恢复的记录数: {len(records)}")
        print(f"结果: {'✅ 成功' if len(records) > 0 else '❌ 失败'}")
    else:
        print("❌ 表都不存在")
    storage4.close()

    # 清理
    if os.path.exists(test_db):
        os.remove(test_db)


if __name__ == "__main__":
    simulate_user_workflow()
    diagnose_persistence_issue()
    test_manual_flush_requirement()
