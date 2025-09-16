#!/usr/bin/env python3
"""
ORDER BY功能测试
"""

import os
import sys

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.db_engine import dbEngine


def test_order_by():
    """测试ORDER BY功能"""
    # 创建数据库引擎
    db = dbEngine("test_order_by.db")

    try:
        print("=== ORDER BY 功能测试 ===")

        # 清理测试数据
        db.execute_sql("DROP TABLE IF EXISTS students")

        # 创建测试表
        print("\n1. 创建测试表...")
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
        print(f"创建表结果: {result}")

        # 插入测试数据
        print("\n2. 插入测试数据...")
        test_data = [
            (1, "Alice", 20, 85),
            (2, "Bob", 22, 92),
            (3, "Charlie", 19, 78),
            (4, "Diana", 21, 96),
            (5, "Eve", 20, 88),
        ]

        for student_id, name, age, score in test_data:
            result = db.execute_sql(
                f"""
                INSERT INTO students VALUES ({student_id}, '{name}', {age}, {score})
            """
            )
            print(f"插入学生 {name}: {result}")

        # 测试基本ORDER BY ASC
        print("\n3. 测试ORDER BY score ASC:")
        result = db.execute_sql("SELECT * FROM students ORDER BY score ASC")
        print(f"结果: {result}")
        if result.success:
            for row in result.data:
                print(f"  {row}")

        # 测试ORDER BY DESC
        print("\n4. 测试ORDER BY score DESC:")
        result = db.execute_sql("SELECT * FROM students ORDER BY score DESC")
        print(f"结果: {result}")
        if result.success:
            for row in result.data:
                print(f"  {row}")

        # 测试ORDER BY与WHERE结合
        print("\n5. 测试WHERE + ORDER BY:")
        result = db.execute_sql(
            "SELECT * FROM students WHERE age >= 20 ORDER BY score DESC"
        )
        print(f"结果: {result}")
        if result.success:
            for row in result.data:
                print(f"  {row}")

        # 测试多列排序（如果支持）
        print("\n6. 测试多列排序:")
        result = db.execute_sql("SELECT * FROM students ORDER BY age ASC")
        print(f"结果: {result}")
        if result.success:
            for row in result.data:
                print(f"  {row}")

        print("\n=== 所有测试完成 ===")

    except Exception as e:
        print(f"测试出错: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理
        try:
            db.close()
        except:
            pass


if __name__ == "__main__":
    test_order_by()
