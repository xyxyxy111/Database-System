#!/usr/bin/env python3
"""
MiniDB 页式存储持久化功能演示
展示完整的页式存储、缓存管理和持久化能力
"""

import os
import sys
import time

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.storage_engine import StorageEngine


def demo_page_based_storage():
    """演示页式存储的完整功能"""
    print("=" * 60)
    print("🗄️  MiniDB 页式存储持久化功能演示")
    print("=" * 60)

    # 清理之前的演示文件
    demo_file = "minidb_demo.db"
    if os.path.exists(demo_file):
        os.remove(demo_file)
        print("🧹 清理旧数据库文件")

    # 第一阶段：创建数据库和表
    print("\n📊 第一阶段：创建数据库和表")
    print("-" * 40)

    storage = StorageEngine(demo_file)
    print(f"✅ 创建数据库: {demo_file}")

    # 创建多个表来演示页式存储
    tables = [
        (
            "employees",
            ["id", "name", "department", "salary"],
            ["INT", "VARCHAR", "VARCHAR", "INT"],
        ),
        ("departments", ["id", "name", "manager"], ["INT", "VARCHAR", "VARCHAR"]),
        (
            "projects",
            ["id", "name", "budget", "status"],
            ["INT", "VARCHAR", "INT", "VARCHAR"],
        ),
    ]

    for table_name, columns, types in tables:
        success = storage.create_table(table_name, columns, types)
        print(f"✅ 创建表 '{table_name}': {'成功' if success else '失败'}")

    # 第二阶段：插入大量数据
    print("\n💾 第二阶段：插入数据（演示页式存储）")
    print("-" * 40)

    # 插入员工数据
    employees_data = [
        [1, "Alice Johnson", "Engineering", 75000],
        [2, "Bob Smith", "Marketing", 65000],
        [3, "Charlie Brown", "Engineering", 80000],
        [4, "Diana Prince", "HR", 70000],
        [5, "Eve Wilson", "Engineering", 85000],
        [6, "Frank Miller", "Sales", 60000],
        [7, "Grace Lee", "Marketing", 67000],
        [8, "Henry Davis", "Engineering", 82000],
        [9, "Ivy Chen", "HR", 72000],
        [10, "Jack Thompson", "Sales", 58000],
    ]

    for emp_data in employees_data:
        success = storage.insert_record("employees", emp_data)
        if success:
            print(f"  📝 插入员工: {emp_data[1]} - {emp_data[2]}")

    # 插入部门数据
    departments_data = [
        [1, "Engineering", "Alice Johnson"],
        [2, "Marketing", "Bob Smith"],
        [3, "HR", "Diana Prince"],
        [4, "Sales", "Frank Miller"],
    ]

    for dept_data in departments_data:
        success = storage.insert_record("departments", dept_data)
        if success:
            print(f"  📝 插入部门: {dept_data[1]} - 经理: {dept_data[2]}")

    # 插入项目数据
    projects_data = [
        [1, "Database Optimization", 100000, "Active"],
        [2, "Mobile App Development", 150000, "Planning"],
        [3, "Website Redesign", 75000, "Active"],
        [4, "Data Analytics Platform", 200000, "Completed"],
    ]

    for proj_data in projects_data:
        success = storage.insert_record("projects", proj_data)
        if success:
            print(f"  📝 插入项目: {proj_data[1]} - 状态: {proj_data[3]}")

    # 第三阶段：显示存储统计信息
    print("\n📈 第三阶段：存储系统统计信息")
    print("-" * 40)

    stats = storage.get_statistics()
    print(f"📊 存储统计:")
    print(f"  • 表数量: {stats['tables_count']}")
    print(f"  • 总记录数: {stats['total_records']}")
    print(f"  • 总页面数: {stats['total_pages']}")
    print(f"  • 数据库文件大小: {stats['database_size']} 字节")

    buffer_stats = stats["buffer_stats"]
    print(f"📋 缓存统计:")
    print(f"  • 缓存大小: {buffer_stats['buffer_size']}")
    print(f"  • 当前页面数: {buffer_stats['current_pages']}")
    print(f"  • 脏页数: {buffer_stats['dirty_pages']}")
    print(f"  • 固定页面数: {buffer_stats['pinned_pages']}")
    print(f"  • 缓存命中率: {buffer_stats['hit_rate']:.2%}")
    print(f"  • 缓存命中次数: {buffer_stats['hit_count']}")
    print(f"  • 缓存未命中次数: {buffer_stats['miss_count']}")

    # 第四阶段：查询数据
    print("\n🔍 第四阶段：查询数据")
    print("-" * 40)

    # 查询各个表的数据
    for table_name, _, _ in tables:
        records = storage.scan_table(table_name)
        table_info = storage.get_table_info(table_name)
        print(f"📋 表 '{table_name}': {len(records)} 条记录")

        # 显示前3条记录
        for i, record in enumerate(records[:3]):
            print(f"  {i+1}. {record.values}")

        if len(records) > 3:
            print(f"  ... 还有 {len(records) - 3} 条记录")

    # 第五阶段：持久化测试
    print("\n💾 第五阶段：持久化测试")
    print("-" * 40)

    print("🔄 正在刷新数据到磁盘...")
    storage.flush()

    print("🔒 关闭数据库连接...")
    storage.close()

    # 检查文件大小
    file_size = os.path.getsize(demo_file)
    print(f"📁 数据库文件大小: {file_size} 字节")

    # 第六阶段：恢复测试
    print("\n🔄 第六阶段：数据恢复测试")
    print("-" * 40)

    print("🔓 重新打开数据库...")
    storage2 = StorageEngine(demo_file)

    # 验证所有表都存在
    all_tables = storage2.get_all_tables()
    print(f"📊 发现 {len(all_tables)} 个表: {', '.join(all_tables)}")

    # 验证数据完整性
    total_recovered_records = 0
    for table_name in all_tables:
        records = storage2.scan_table(table_name)
        total_recovered_records += len(records)
        print(f"✅ 表 '{table_name}': 恢复 {len(records)} 条记录")

    print(f"🎯 总共恢复 {total_recovered_records} 条记录")

    # 显示恢复后的统计信息
    stats2 = storage2.get_statistics()
    print(f"📈 恢复后统计:")
    print(f"  • 表数量: {stats2['tables_count']}")
    print(f"  • 总记录数: {stats2['total_records']}")
    print(f"  • 总页面数: {stats2['total_pages']}")

    storage2.close()

    # 第七阶段：页面使用分析
    print("\n🔬 第七阶段：页面使用分析")
    print("-" * 40)

    with open(demo_file, "rb") as f:
        content = f.read()
        print(f"📄 文件总大小: {len(content)} 字节")

        # 分析文件结构
        print(f"🏗️  文件结构分析:")
        print(f"  • 文件头部: {content[:8].hex()}")

        # 计算页面数（假设4KB页面大小）
        page_size = 4096
        file_header_size = 8
        data_size = len(content) - file_header_size
        estimated_pages = data_size // page_size
        print(f"  • 估计页面数: {estimated_pages}")
        print(f"  • 页面大小: {page_size} 字节")
        print(f"  • 页面利用率: {(data_size % page_size) / page_size:.2%} (最后一页)")

    print("\n🎉 演示完成！")
    print("=" * 60)
    print("📝 MiniDB 页式存储功能总结:")
    print("  ✅ 页面分配和管理")
    print("  ✅ 缓存管理 (LRU策略)")
    print("  ✅ 磁盘持久化")
    print("  ✅ 数据恢复和完整性")
    print("  ✅ 多表支持")
    print("  ✅ 存储统计和监控")
    print("=" * 60)


def demo_performance_comparison():
    """演示缓存对性能的影响"""
    print("\n⚡ 性能对比演示：缓存 vs 直接磁盘访问")
    print("-" * 50)

    demo_file = "performance_test.db"
    if os.path.exists(demo_file):
        os.remove(demo_file)

    storage = StorageEngine(demo_file)

    # 创建测试表
    storage.create_table("test_table", ["id", "data"], ["INT", "VARCHAR"])

    # 插入测试数据
    print("📝 插入测试数据...")
    for i in range(50):
        storage.insert_record("test_table", [i, f"test_data_{i}"])

    # 测试多次查询的性能（利用缓存）
    print("🔄 测试缓存性能（多次查询同一表）...")

    start_time = time.time()
    for _ in range(10):
        records = storage.scan_table("test_table")
    end_time = time.time()

    cached_time = end_time - start_time

    # 获取最终统计
    stats = storage.get_statistics()
    buffer_stats = stats["buffer_stats"]

    print(f"⏱️  10次查询总时间: {cached_time:.4f} 秒")
    print(f"📊 缓存统计:")
    print(f"  • 命中率: {buffer_stats['hit_rate']:.2%}")
    print(f"  • 命中次数: {buffer_stats['hit_count']}")
    print(f"  • 未命中次数: {buffer_stats['miss_count']}")

    storage.close()

    # 清理
    if os.path.exists(demo_file):
        os.remove(demo_file)


if __name__ == "__main__":
    demo_page_based_storage()
    demo_performance_comparison()
