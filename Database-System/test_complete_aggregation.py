"""
完整的聚合函数测试
包含DISTINCT、AVG等高级功能
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import dbEngine


def test_complete_aggregation():
    """测试完整聚合函数功能"""
    print("=== 完整聚合函数测试 ===")

    # 创建数据库引擎
    db_path = "test_complete_aggregation.db"
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
                name VARCHAR(50),
                price INTEGER,
                category VARCHAR(20)
            )
        """
        )
        print(f"创建表结果: {result.message}")

        if not result.success:
            print("创建表失败，退出测试")
            return

        # 2. 插入测试数据
        print("\n2. 插入测试数据...")
        test_data = [
            (1, "Laptop", 1000, "Electronics"),
            (2, "Mouse", 30, "Electronics"),
            (3, "Keyboard", 80, "Electronics"),
            (4, "Chair", 200, "Furniture"),
            (5, "Desk", 300, "Furniture"),
            (6, "Book", 20, "Books"),
            (7, "Pen", 3, "Stationery"),
            (8, "Notebook", 25, "Electronics"),  # 相同价格和类别
        ]

        for data in test_data:
            result = engine.execute_sql(
                f"""
                INSERT INTO products VALUES 
                ({data[0]}, '{data[1]}', {data[2]}, '{data[3]}')
            """
            )
            if not result.success:
                print(f"插入失败: {result.message}")

        print("测试数据插入完成")

        # 3. 测试AVG函数
        print("\n3. 测试AVG函数...")
        result = engine.execute_sql("SELECT AVG(price) FROM products")
        expected_avg = sum([1000, 30, 80, 200, 300, 20, 3, 25]) / 8
        print(f"AVG(price): {result.data if result.success else result.message}")
        print(f"期望值: {expected_avg}")

        # 4. 测试COUNT DISTINCT
        print("\n4. 测试COUNT(DISTINCT category)...")
        result = engine.execute_sql("SELECT COUNT(DISTINCT category) FROM products")
        # 应该有4个不同的类别: Electronics, Furniture, Books, Stationery
        print(
            f"COUNT(DISTINCT category): {result.data if result.success else result.message}"
        )
        print("期望值: 4 (Electronics, Furniture, Books, Stationery)")

        # 5. 测试混合数据类型的聚合
        print("\n5. 测试字符串MIN/MAX...")
        result = engine.execute_sql("SELECT MIN(name) FROM products")
        print(f"MIN(name): {result.data if result.success else result.message}")

        result = engine.execute_sql("SELECT MAX(name) FROM products")
        print(f"MAX(name): {result.data if result.success else result.message}")

        # 6. 验证聚合函数结果的正确性
        print("\n6. 验证统计...")

        # 查看所有数据
        result = engine.execute_sql("SELECT id, price FROM products")
        if result.success:
            print("所有价格数据:")
            for record in result.data:
                print(
                    f"  ID {record.get('ID', 'N/A')}: "
                    f"价格 {record.get('PRICE', 'N/A')}"
                )

            # 手动计算验证
            prices = []
            for record in result.data:
                price = record.get("PRICE")
                if price is not None:
                    prices.append(int(price))

            if prices:
                print(f"\n手动计算验证:")
                print(f"  总数: {len(prices)}")
                print(f"  总和: {sum(prices)}")
                print(f"  平均值: {sum(prices) / len(prices)}")
                print(f"  最大值: {max(prices)}")
                print(f"  最小值: {min(prices)}")

        print("\n=== 完整聚合函数测试完成 ===")

    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理测试文件
        if os.path.exists(db_path):
            os.remove(db_path)


if __name__ == "__main__":
    test_complete_aggregation()
