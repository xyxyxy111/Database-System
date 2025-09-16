"""
基础事务支持测试
测试BEGIN、COMMIT、ROLLBACK功能
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import dbEngine


def test_basic_transactions():
    """测试基础事务功能"""
    print("=== 基础事务功能测试 ===")

    # 创建数据库引擎
    db_path = "test_transactions.db"
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

        if not result.success:
            print("创建表失败，退出测试")
            return

        # 2. 插入初始数据
        print("\n2. 插入初始数据...")
        initial_data = [
            (1, "Alice", 1000),
            (2, "Bob", 500),
        ]

        for data in initial_data:
            result = engine.execute_sql(
                f"""
                INSERT INTO accounts VALUES ({data[0]}, '{data[1]}', {data[2]})
            """
            )
            if not result.success:
                print(f"插入失败: {result.message}")

        print("初始数据插入完成")

        # 3. 查看初始数据
        print("\n3. 查看初始数据...")
        result = engine.execute_sql("SELECT id, name, balance FROM accounts")
        if result.success:
            print("初始账户数据:")
            for record in result.data:
                print(
                    f"  ID {record.get('ID', 'N/A')}: "
                    f"{record.get('NAME', 'N/A')} - "
                    f"余额 {record.get('BALANCE', 'N/A')}"
                )

        # 4. 测试事务语句解析
        print("\n4. 测试事务语句...")

        # 开始事务
        print("- 开始事务")
        result = engine.execute_sql("BEGIN")
        print(f"  BEGIN: {result.message if result.success else result.message}")

        # 在事务中进行操作（转账）
        print("- 事务中操作：Alice向Bob转账100")
        result = engine.execute_sql(
            """
            UPDATE accounts SET balance = balance - 100 WHERE id = 1
        """
        )
        print(
            f"  减少Alice余额: {result.message if result.success else result.message}"
        )

        result = engine.execute_sql(
            """
            UPDATE accounts SET balance = balance + 100 WHERE id = 2
        """
        )
        print(f"  增加Bob余额: {result.message if result.success else result.message}")

        # 查看事务中的数据
        print("\n- 事务中查看数据:")
        result = engine.execute_sql("SELECT id, name, balance FROM accounts")
        if result.success:
            for record in result.data:
                print(
                    f"  ID {record.get('ID', 'N/A')}: "
                    f"{record.get('NAME', 'N/A')} - "
                    f"余额 {record.get('BALANCE', 'N/A')}"
                )

        # 5. 测试回滚
        print("\n5. 测试回滚...")
        result = engine.execute_sql("ROLLBACK")
        print(f"ROLLBACK: {result.message if result.success else result.message}")

        # 查看回滚后的数据
        print("\n- 回滚后查看数据:")
        result = engine.execute_sql("SELECT id, name, balance FROM accounts")
        if result.success:
            for record in result.data:
                print(
                    f"  ID {record.get('ID', 'N/A')}: "
                    f"{record.get('NAME', 'N/A')} - "
                    f"余额 {record.get('BALANCE', 'N/A')}"
                )

        # 6. 测试提交事务
        print("\n6. 测试提交事务...")

        # 再次开始事务
        result = engine.execute_sql("BEGIN")
        print(f"BEGIN: {result.message if result.success else result.message}")

        # 进行另一个操作
        result = engine.execute_sql(
            """
            UPDATE accounts SET balance = balance + 50 WHERE id = 1
        """
        )
        print(
            f"给Alice增加50余额: {result.message if result.success else result.message}"
        )

        # 提交事务
        result = engine.execute_sql("COMMIT")
        print(f"COMMIT: {result.message if result.success else result.message}")

        # 查看最终数据
        print("\n- 最终数据:")
        result = engine.execute_sql("SELECT id, name, balance FROM accounts")
        if result.success:
            for record in result.data:
                print(
                    f"  ID {record.get('ID', 'N/A')}: "
                    f"{record.get('NAME', 'N/A')} - "
                    f"余额 {record.get('BALANCE', 'N/A')}"
                )

        print("\n=== 基础事务功能测试完成 ===")

    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理测试文件
        if os.path.exists(db_path):
            os.remove(db_path)


if __name__ == "__main__":
    test_basic_transactions()
