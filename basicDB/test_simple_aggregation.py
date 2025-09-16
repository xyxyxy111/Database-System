"""
简化的聚合函数测试
使用支持的数据类型进行测试
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import dbEngine


def test_basic_aggregation():
    """测试基本聚合函数"""
    print("=== 基本聚合函数测试 ===")

    # 创建数据库引擎
    db_path = "test_simple_aggregation.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    engine = dbEngine(db_path)

    try:
        # 1. 创建简单测试表
        print("\n1. 创建表...")
        result = engine.execute_sql(
            """
            CREATE TABLE numbers (
                id INTEGER,
                value INTEGER
            )
        """
        )
        print(f"创建表结果: {result.message}")

        if not result.success:
            print(f"创建表失败，退出测试")
            return

        # 2. 插入测试数据
        print("\n2. 插入测试数据...")
        test_data = [
            (1, 10),
            (2, 20),
            (3, 30),
            (4, 40),
            (5, 50),
        ]

        for data in test_data:
            result = engine.execute_sql(
                f"""
                INSERT INTO numbers VALUES ({data[0]}, {data[1]})
            """
            )
            if not result.success:
                print(f"插入失败: {result.message}")

        print("测试数据插入完成")

        # 3. 测试COUNT(*)
        print("\n3. 测试COUNT(*)...")
        result = engine.execute_sql("SELECT COUNT(*) FROM numbers")
        print(f"COUNT(*): {result.data if result.success else result.message}")

        # 4. 测试COUNT(列名)
        print("\n4. 测试COUNT(value)...")
        result = engine.execute_sql("SELECT COUNT(value) FROM numbers")
        print(f"COUNT(value): {result.data if result.success else result.message}")

        # 5. 测试SUM
        print("\n5. 测试SUM(value)...")
        result = engine.execute_sql("SELECT SUM(value) FROM numbers")
        print(f"SUM(value): {result.data if result.success else result.message}")

        # 6. 测试MAX
        print("\n6. 测试MAX(value)...")
        result = engine.execute_sql("SELECT MAX(value) FROM numbers")
        print(f"MAX(value): {result.data if result.success else result.message}")

        # 7. 测试MIN
        print("\n7. 测试MIN(value)...")
        result = engine.execute_sql("SELECT MIN(value) FROM numbers")
        print(f"MIN(value): {result.data if result.success else result.message}")

        print("\n=== 基本聚合函数测试完成 ===")

    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理测试文件
        if os.path.exists(db_path):
            os.remove(db_path)


if __name__ == "__main__":
    test_basic_aggregation()
