"""简化的 WHERE 条件测试"""

import os

from database import dbEngine


def test_where_simple():
    db_path = "test_where.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    try:
        with dbEngine(db_path) as db:
            # 创建表
            result = db.execute_sql("CREATE TABLE test (id INT, age INT)")
            print(f"创建表: {result.success}")

            # 插入数据
            db.execute_sql("INSERT INTO test VALUES (1, 20)")
            db.execute_sql("INSERT INTO test VALUES (2, 25)")
            db.execute_sql("INSERT INTO test VALUES (3, 18)")

            # 查询所有数据
            result = db.execute_sql("SELECT id, age FROM test")
            print(f"查询所有: {result.success}, 数据: {result.data}")

            # WHERE 条件查询
            result = db.execute_sql("SELECT id, age FROM test WHERE age > 20")
            print(f"WHERE 查询: {result.success}")
            if not result.success:
                print(f"错误: {result.message}")
            else:
                print(f"结果: {result.data}")

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()

    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    test_where_simple()
