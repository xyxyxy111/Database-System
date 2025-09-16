#!/usr/bin/env python3
"""
JOIN功能测试
"""

import os
import sys

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.db_engine import dbEngine


def test_join():
    """测试JOIN功能"""
    # 创建数据库引擎
    db = dbEngine("test_join.db")

    try:
        print("=== JOIN 功能测试 ===")

        # 清理测试数据
        db.execute_sql("DROP TABLE IF EXISTS students")
        db.execute_sql("DROP TABLE IF EXISTS courses")
        db.execute_sql("DROP TABLE IF EXISTS enrollments")

        # 创建测试表
        print("\n1. 创建测试表...")

        # 学生表
        result = db.execute_sql(
            """
            CREATE TABLE students (
                id INT,
                name VARCHAR(50),
                age INT
            )
        """
        )
        print(f"创建学生表: {result}")

        # 课程表
        result = db.execute_sql(
            """
            CREATE TABLE courses (
                id INT,
                title VARCHAR(50),
                credits INT
            )
        """
        )
        print(f"创建课程表: {result}")

        # 选课表
        result = db.execute_sql(
            """
            CREATE TABLE enrollments (
                student_id INT,
                course_id INT,
                grade INT
            )
        """
        )
        print(f"创建选课表: {result}")

        # 插入测试数据
        print("\n2. 插入测试数据...")

        # 学生数据
        students_data = [
            (1, "Alice", 20),
            (2, "Bob", 22),
            (3, "Charlie", 19),
        ]

        for student_id, name, age in students_data:
            result = db.execute_sql(
                f"""
                INSERT INTO students VALUES ({student_id}, '{name}', {age})
            """
            )
            print(f"插入学生 {name}: {result}")

        # 课程数据
        courses_data = [
            (101, "Math", 3),
            (102, "Physics", 4),
            (103, "Chemistry", 3),
        ]

        for course_id, title, credits in courses_data:
            result = db.execute_sql(
                f"""
                INSERT INTO courses VALUES ({course_id}, '{title}', {credits})
            """
            )
            print(f"插入课程 {title}: {result}")

        # 选课数据
        enrollments_data = [
            (1, 101, 85),  # Alice选Math
            (1, 102, 92),  # Alice选Physics
            (2, 101, 78),  # Bob选Math
            (3, 103, 88),  # Charlie选Chemistry
        ]

        for student_id, course_id, grade in enrollments_data:
            result = db.execute_sql(
                f"""
                INSERT INTO enrollments VALUES ({student_id}, {course_id}, {grade})
            """
            )
            print(f"插入选课记录 学生{student_id}-课程{course_id}: {result}")

        # 测试基本INNER JOIN
        print("\n3. 测试INNER JOIN (学生和选课):")
        result = db.execute_sql(
            """
            SELECT * FROM students 
            INNER JOIN enrollments ON id = student_id
        """
        )
        print(f"结果: {result}")
        if result.success:
            for row in result.data:
                print(f"  {row}")

        print("\n=== 所有JOIN测试完成 ===")

    except Exception as e:
        print(f"测试出错: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理
        try:
            db.close()
        except Exception:
            pass


if __name__ == "__main__":
    test_join()
