#!/usr/bin/env python3
"""调试 UPDATE 结果验证问题"""

from database.db_engine import dbEngine


def test_update_verification():
    """测试 UPDATE 结果验证"""

    try:
        # 初始化数据库
        db = dbEngine("debug_update_verification")

        # 创建表
        print("1. 创建表...")
        result = db.execute_sql(
            """
            CREATE TABLE students (
                id INT,
                name VARCHAR(50),
                age INT,
                score INT
            )
        """
        )
        print(f"   {result.message}")

        # 插入数据
        print("\n2. 插入数据...")
        result = db.execute_sql("INSERT INTO students VALUES (1, 'Alice', 20, 85)")
        print(f"   {result.message}")

        # 查询原始数据
        print("\n3. 查询原始数据...")
        result = db.execute_sql("SELECT name, score FROM students WHERE name = 'Alice'")
        print(f"   成功: {result.success}")
        print(f"   数据: {result.data}")
        if result.data:
            print(f"   第一条记录: {result.data[0]}")
            print(f"   所有键: {list(result.data[0].keys())}")

        # 执行 UPDATE
        print("\n4. 执行 UPDATE...")
        result = db.execute_sql("UPDATE students SET score = 87 WHERE name = 'Alice'")
        print(f"   成功: {result.success}")
        print(f"   消息: {result.message}")
        print(f"   影响行数: {result.affected_rows}")

        # 验证更新结果
        print("\n5. 验证更新结果...")
        result = db.execute_sql("SELECT name, score FROM students WHERE name = 'Alice'")
        print(f"   成功: {result.success}")
        print(f"   数据: {result.data}")
        if result.data:
            print(f"   第一条记录: {result.data[0]}")
            print(f"   所有键: {list(result.data[0].keys())}")
            print(f"   score 值: {result.data[0].get('score')}")
            print(f"   SCORE 值: {result.data[0].get('SCORE')}")

        # 关闭数据库
        db.close()

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_update_verification()
