#!/usr/bin/env python3
"""快速验证扩展功能"""

from database.db_engine import dbEngine


def quick_test():
    """快速测试所有扩展功能"""
    print("🚀 MiniDB 扩展功能快速验证")
    print("=" * 50)

    # 创建数据库实例
    db = dbEngine("quick_test")

    try:
        # 1. 创建表
        print("\n1. 创建表...")
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

        # 2. 插入数据
        print("\n2. 插入测试数据...")
        test_data = [
            "INSERT INTO students VALUES (1, 'Alice', 20, 85)",
            "INSERT INTO students VALUES (2, 'Bob', 22, 90)",
            "INSERT INTO students VALUES (3, 'Charlie', 19, 78)",
        ]

        for sql in test_data:
            result = db.execute_sql(sql)
            print(f"   {result.message}")

        # 3. SELECT * 查询
        print("\n3. SELECT * 查询...")
        result = db.execute_sql("SELECT * FROM students")
        print(f"   返回 {len(result.data)} 条记录:")
        for i, row in enumerate(result.data, 1):
            print(f"     {i}. {row}")

        # 4. WHERE 条件查询
        print("\n4. WHERE 条件查询...")
        result = db.execute_sql("SELECT * FROM students WHERE age > 20")
        print(f"   年龄>20的学生: {len(result.data)} 条")
        for row in result.data:
            print(f"     {row}")

        # 5. UPDATE 操作
        print("\n5. UPDATE 操作...")
        result = db.execute_sql("UPDATE students SET score = 88 WHERE name = 'Alice'")
        print(f"   {result.message}")

        # 验证 UPDATE 结果
        result = db.execute_sql("SELECT name, score FROM students WHERE name = 'Alice'")
        if result.data:
            print(f"   Alice 的新分数: {result.data[0]['SCORE']}")

        # 6. DELETE 操作
        print("\n6. DELETE 操作...")
        result = db.execute_sql("DELETE FROM students WHERE score < 80")
        print(f"   {result.message}")

        # 验证 DELETE 结果
        result = db.execute_sql("SELECT * FROM students")
        print(f"   剩余学生: {len(result.data)} 人")
        for row in result.data:
            print(f"     {row}")

        print("\n🎉 所有扩展功能验证完成！")

    except Exception as e:
        print(f"❌ 测试出现错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    quick_test()
