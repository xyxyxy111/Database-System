#!/usr/bin/env python3
"""
重现用户问题：直接退出Python不调用close()导致数据丢失
"""

import os
import sys

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.storage_engine import StorageEngine


def test_direct_exit():
    """测试直接退出Python的情况"""
    print("=" * 50)
    print("🚨 重现问题：直接退出Python不调用close()")
    print("=" * 50)

    db_file = "direct_exit_test.db"

    # 清理旧文件
    if os.path.exists(db_file):
        os.remove(db_file)

    print("1. 创建数据库和插入数据...")
    storage = StorageEngine(db_file)
    storage.create_table("users", ["id", "name"], ["INT", "VARCHAR"])
    storage.insert_record("users", [1, "Test User"])

    print("2. 查看当前数据...")
    records = storage.scan_table("users")
    print(f"   内存中的记录: {len(records)}")

    # 检查缓存状态
    stats = storage.get_statistics()
    buffer_stats = stats["buffer_stats"]
    print(f"   脏页数: {buffer_stats['dirty_pages']}")
    print(f"   当前页面数: {buffer_stats['current_pages']}")

    print("3. 模拟直接退出Python（不调用close()）...")
    # 直接删除storage对象，模拟程序直接退出
    del storage

    print("4. 重新打开数据库检查数据...")
    storage2 = StorageEngine(db_file)

    tables = storage2.get_all_tables()
    print(f"   发现的表: {tables}")

    if "users" in tables:
        recovered_records = storage2.scan_table("users")
        print(f"   恢复的记录数: {len(recovered_records)}")

        if len(recovered_records) == 0:
            print("❌ 问题确认：数据丢失了！")
            print("   原因：脏页没有写入磁盘")
        else:
            print("✅ 数据恢复成功")
    else:
        print("❌ 严重问题：连表结构都丢失了！")

    storage2.close()


def show_solution():
    """展示解决方案"""
    print("\n" + "=" * 50)
    print("💡 解决方案")
    print("=" * 50)

    print(
        """
🔧 解决方法1: 总是显式调用close()
```python
storage = StorageEngine("my_db.db")
try:
    # 你的数据库操作
    storage.create_table("users", ["id", "name"], ["INT", "VARCHAR"])
    storage.insert_record("users", [1, "Alice"])
finally:
    storage.close()  # 确保调用close()
```

🔧 解决方法2: 使用with语句（推荐）
```python
with StorageEngine("my_db.db") as storage:
    # 你的数据库操作
    storage.create_table("users", ["id", "name"], ["INT", "VARCHAR"])
    storage.insert_record("users", [1, "Alice"])
    # 自动调用close()
```

🔧 解决方法3: 手动调用flush()
```python
storage = StorageEngine("my_db.db")
storage.create_table("users", ["id", "name"], ["INT", "VARCHAR"])
storage.insert_record("users", [1, "Alice"])
storage.flush()  # 强制写入磁盘
# 即使不调用close()，数据也已经保存
```
"""
    )


def demonstrate_solution():
    """演示正确的使用方法"""
    print("\n" + "=" * 50)
    print("✅ 演示正确的使用方法")
    print("=" * 50)

    db_file = "solution_demo.db"

    # 清理旧文件
    if os.path.exists(db_file):
        os.remove(db_file)

    print("方法1: 使用with语句（自动管理资源）")
    print("-" * 30)

    with StorageEngine(db_file) as storage:
        storage.create_table(
            "products", ["id", "name", "price"], ["INT", "VARCHAR", "FLOAT"]
        )
        storage.insert_record("products", [1, "Laptop", 999.99])
        storage.insert_record("products", [2, "Mouse", 29.99])
        print("✅ 数据已插入")
    # with语句结束时自动调用close()

    print("验证数据是否持久化...")
    with StorageEngine(db_file) as storage:
        records = storage.scan_table("products")
        print(f"✅ 恢复了 {len(records)} 条记录")
        for record in records:
            print(f"   {record.values}")

    print("\n方法2: 手动flush()保证数据安全")
    print("-" * 30)

    # 清理重新开始
    if os.path.exists(db_file):
        os.remove(db_file)

    storage = StorageEngine(db_file)
    storage.create_table("orders", ["id", "customer"], ["INT", "VARCHAR"])
    storage.insert_record("orders", [1, "张三"])

    # 重要：手动flush确保数据写入磁盘
    storage.flush()
    print("✅ 调用flush()后，即使不调用close()数据也是安全的")

    # 模拟程序意外退出（不调用close）
    del storage

    # 验证数据
    storage2 = StorageEngine(db_file)
    records = storage2.scan_table("orders")
    print(f"✅ 即使没有调用close()，也恢复了 {len(records)} 条记录")
    storage2.close()

    # 清理
    if os.path.exists(db_file):
        os.remove(db_file)


if __name__ == "__main__":
    test_direct_exit()
    show_solution()
    demonstrate_solution()
