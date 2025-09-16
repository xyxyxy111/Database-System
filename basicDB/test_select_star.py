"""测试 SELECT * 功能"""

import os

from database import dbEngine


def test_select_star():
    db_path = "test_star.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    try:
        with dbEngine(db_path) as db:
            # 创建表
            db.execute_sql("CREATE TABLE test (id INT, name VARCHAR(50))")

            # 插入数据
            db.execute_sql("INSERT INTO test VALUES (1, 'Alice')")
            db.execute_sql("INSERT INTO test VALUES (2, 'Bob')")

            print("=== 测试普通查询 ===")
            result = db.execute_sql("SELECT id, name FROM test")
            print(f"成功: {result.success}")
            if result.success:
                print(f"数据: {result.data}")
            else:
                print(f"错误: {result.message}")

            print("\n=== 测试 SELECT * 查询 ===")
            result = db.execute_sql("SELECT * FROM test")
            print(f"成功: {result.success}")
            if result.success:
                print(f"数据: {result.data}")
            else:
                print(f"错误: {result.message}")

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()

    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    test_select_star()
