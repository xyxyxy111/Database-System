"""
简化的事务测试 - 避免算术表达式
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import dbEngine


def test_simple_transactions():
    """测试简化的事务功能"""
    print("=== 简化事务功能测试 ===")

    # 创建数据库引擎
    db_path = "test_simple_transactions.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    engine = dbEngine(db_path)

    try:
        # 1. 创建测试表
        print("\n1. 创建表...")
        result = engine.execute_sql(
            """
            CREATE TABLE accounts (
                id INTEGER,
                name VARCHAR(20),
                balance INTEGER
            )
        """
        )
        print(f"创建表结果: {result.message}")

        # 2. 插入初始数据
        print("\n2. 插入初始数据...")
        result = engine.execute_sql("INSERT INTO accounts VALUES (1, 'Alice', 1000)")
        print(f"插入Alice: {result.message}")

        result = engine.execute_sql("INSERT INTO accounts VALUES (2, 'Bob', 500)")
        print(f"插入Bob: {result.message}")

        # 3. 查看初始数据
        print("\n3. 查看初始数据...")
        result = engine.execute_sql("SELECT * FROM accounts")
        if result.success:
            print("初始账户数据:")
            for record in result.data:
                print(f"  {record}")

        # 4. 测试事务语句
        print("\n4. 测试事务控制...")

        # 测试BEGIN
        result = engine.execute_sql("BEGIN")
        print(f"BEGIN: {result.message}")

        # 插入一些数据
        result = engine.execute_sql("INSERT INTO accounts VALUES (3, 'Charlie', 300)")
        print(f"事务中插入Charlie: {result.message}")

        # 查看事务中的数据
        result = engine.execute_sql("SELECT * FROM accounts")
        if result.success:
            print("事务中的数据:")
            for record in result.data:
                print(f"  {record}")

        # 测试ROLLBACK
        print("\n5. 测试回滚...")
        result = engine.execute_sql("ROLLBACK")
        print(f"ROLLBACK: {result.message}")

        # 查看回滚后的数据
        result = engine.execute_sql("SELECT * FROM accounts")
        if result.success:
            print("回滚后的数据:")
            for record in result.data:
                print(f"  {record}")

        # 6. 测试提交事务
        print("\n6. 测试提交事务...")

        result = engine.execute_sql("BEGIN")
        print(f"BEGIN: {result.message}")

        result = engine.execute_sql("INSERT INTO accounts VALUES (4, 'David', 200)")
        print(f"事务中插入David: {result.message}")

        result = engine.execute_sql("COMMIT")
        print(f"COMMIT: {result.message}")

        # 查看最终数据
        result = engine.execute_sql("SELECT * FROM accounts")
        if result.success:
            print("最终数据:")
            for record in result.data:
                print(f"  {record}")

        print("\n=== 简化事务功能测试完成 ===")

    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理测试文件
        if os.path.exists(db_path):
            os.remove(db_path)


if __name__ == "__main__":
    test_simple_transactions()
