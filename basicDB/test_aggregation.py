"""
测试聚合函数功能
测试COUNT、SUM、AVG、MAX、MIN等聚合函数
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import dbEngine


def test_aggregation_functions():
    """测试聚合函数"""
    print("=== 聚合函数测试 ===")

    # 创建数据库引擎
    db_path = "test_aggregation.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    engine = dbEngine(db_path)

    try:
        # 1. 创建测试表
        print("\n1. 创建表...")
        result = engine.execute_sql(
            """
            CREATE TABLE products (
                id INTEGER,
                name TEXT,
                price DECIMAL,
                category TEXT,
                stock INTEGER
            )
        """
        )
        print(f"创建表结果: {result.message}")

        # 2. 插入测试数据
        print("\n2. 插入测试数据...")
        test_data = [
            (1, "Laptop", 999.99, "Electronics", 10),
            (2, "Mouse", 29.99, "Electronics", 50),
            (3, "Keyboard", 79.99, "Electronics", 30),
            (4, "Chair", 199.99, "Furniture", 15),
            (5, "Desk", 299.99, "Furniture", 8),
            (6, "Book", 19.99, "Books", 100),
            (7, "Pen", 2.99, "Stationery", 200),
        ]

        for data in test_data:
            result = engine.execute_sql(
                f"""
                INSERT INTO products VALUES 
                ({data[0]}, '{data[1]}', {data[2]}, '{data[3]}', {data[4]})
            """
            )
            if not result.success:
                print(f"插入失败: {result.message}")

        print("测试数据插入完成")

        # 3. 测试COUNT函数
        print("\n3. 测试COUNT函数...")

        # COUNT(*)
        result = engine.execute_sql("SELECT COUNT(*) FROM products")
        print(f"COUNT(*): {result.data if result.success else result.message}")

        # COUNT(列名)
        result = engine.execute_sql("SELECT COUNT(name) FROM products")
        print(f"COUNT(name): {result.data if result.success else result.message}")

        # 4. 测试SUM函数
        print("\n4. 测试SUM函数...")
        result = engine.execute_sql("SELECT SUM(price) FROM products")
        print(f"SUM(price): {result.data if result.success else result.message}")

        result = engine.execute_sql("SELECT SUM(stock) FROM products")
        print(f"SUM(stock): {result.data if result.success else result.message}")

        # 5. 测试AVG函数
        print("\n5. 测试AVG函数...")
        result = engine.execute_sql("SELECT AVG(price) FROM products")
        print(f"AVG(price): {result.data if result.success else result.message}")

        # 6. 测试MAX函数
        print("\n6. 测试MAX函数...")
        result = engine.execute_sql("SELECT MAX(price) FROM products")
        print(f"MAX(price): {result.data if result.success else result.message}")

        result = engine.execute_sql("SELECT MAX(stock) FROM products")
        print(f"MAX(stock): {result.data if result.success else result.message}")

        # 7. 测试MIN函数
        print("\n7. 测试MIN函数...")
        result = engine.execute_sql("SELECT MIN(price) FROM products")
        print(f"MIN(price): {result.data if result.success else result.message}")

        result = engine.execute_sql("SELECT MIN(stock) FROM products")
        print(f"MIN(stock): {result.data if result.success else result.message}")

        # 8. 测试DISTINCT聚合
        print("\n8. 测试DISTINCT聚合...")
        result = engine.execute_sql("SELECT COUNT(DISTINCT category) FROM products")
        print(
            f"COUNT(DISTINCT category): "
            f"{result.data if result.success else result.message}"
        )

        print("\n=== 聚合函数测试完成 ===")

    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理测试文件
        if os.path.exists(db_path):
            os.remove(db_path)


if __name__ == "__main__":
    test_aggregation_functions()
