"""
MiniDB 扩展功能测试
测试 UPDATE, DELETE 以及增强的 WHERE 条件功能
"""

import os

from database import dbEngine


def test_extended_features():
    """测试数据库扩展功能"""

    print("MiniDB 扩展功能测试")
    print("=" * 40)

    # 测试数据库文件
    db_path = "test_extensions.db"

    # 清理现有文件
    if os.path.exists(db_path):
        os.remove(db_path)

    tests_passed = 0
    tests_total = 0

    try:
        with dbEngine(db_path, buffer_size=16) as db:

            # 测试1: 创建测试表
            print("\n1. 创建测试表...")
            tests_total += 1

            sql = """
            CREATE TABLE students (
                id INT,
                name VARCHAR(50),
                age INT,
                score INT
            )
            """
            result = db.execute_sql(sql)
            if result.success:
                print("   ✓ 创建表成功")
                tests_passed += 1
            else:
                print(f"   ✗ 创建表失败: {result.message}")

            # 测试2: 插入测试数据
            print("\n2. 插入测试数据...")
            tests_total += 1

            insert_sqls = [
                "INSERT INTO students VALUES (1, 'Alice', 20, 85)",
                "INSERT INTO students VALUES (2, 'Bob', 22, 90)",
                "INSERT INTO students VALUES (3, 'Charlie', 19, 78)",
                "INSERT INTO students VALUES (4, 'Diana', 21, 92)",
                "INSERT INTO students VALUES (5, 'Eve', 20, 88)",
            ]

            insert_success = 0
            for sql in insert_sqls:
                result = db.execute_sql(sql)
                if result.success:
                    insert_success += 1

            if insert_success == len(insert_sqls):
                print(f"   ✓ 插入成功: {insert_success}/{len(insert_sqls)} 条记录")
                tests_passed += 1
            else:
                print(f"   ✗ 插入失败: {insert_success}/{len(insert_sqls)} 条记录")

            # 测试3: 基础查询
            print("\n3. 基础查询...")
            tests_total += 1

            result = db.execute_sql("SELECT id, name, age, score FROM students")
            if result.success and len(result.data) == 5:
                print(f"   ✓ 查询成功: 返回 {len(result.data)} 条记录")
                for i, record in enumerate(result.data[:3], 1):
                    print(f"      记录{i}: {record}")
                if len(result.data) > 3:
                    print(f"      ... 还有 {len(result.data) - 3} 条记录")
                tests_passed += 1
            else:
                print(
                    f"   ✗ 查询失败: {result.message if not result.success else '记录数不正确'}"
                )

            # 测试4: WHERE 条件查询
            print("\n4. WHERE 条件查询...")
            tests_total += 1

            test_queries = [
                ("SELECT * FROM students WHERE age > 20", "年龄大于20"),
                ("SELECT * FROM students WHERE score >= 88", "分数大于等于88"),
                ("SELECT * FROM students WHERE name = 'Alice'", "姓名等于Alice"),
            ]

            where_success = 0
            for sql, desc in test_queries:
                result = db.execute_sql(sql)
                if result.success:
                    where_success += 1
                    print(f"   ✓ {desc}: {len(result.data)} 条记录")
                else:
                    print(f"   ✗ {desc}: 查询失败")

            if where_success == len(test_queries):
                tests_passed += 1
                print("   ✓ WHERE 条件查询测试通过")
            else:
                print("   ✗ WHERE 条件查询测试失败")

            # 测试5: UPDATE 语句
            print("\n5. UPDATE 语句测试...")
            tests_total += 1

            # 单条更新
            result1 = db.execute_sql(
                "UPDATE students SET score = 87 WHERE name = 'Alice'"
            )

            # 批量更新
            result2 = db.execute_sql("UPDATE students SET age = 23 WHERE age > 21")

            # 验证更新结果
            verify_result = db.execute_sql(
                "SELECT name, score FROM students WHERE name = 'Alice'"
            )

            if (
                result1.success
                and result2.success
                and verify_result.success
                and len(verify_result.data) > 0
                and verify_result.data[0].get("SCORE") == 87
            ):
                print("   ✓ UPDATE 语句执行成功")
                print(f"      单条更新: 影响 {result1.affected_rows} 行")
                print(f"      批量更新: 影响 {result2.affected_rows} 行")
                tests_passed += 1
            else:
                print("   ✗ UPDATE 语句执行失败")
                print(f"      结果1: {result1.message}")
                print(f"      结果2: {result2.message}")

            # 测试6: DELETE 语句
            print("\n6. DELETE 语句测试...")
            tests_total += 1

            # 删除特定记录
            result1 = db.execute_sql("DELETE FROM students WHERE score < 80")

            # 验证删除结果
            verify_result = db.execute_sql("SELECT COUNT(*) as count FROM students")
            remaining_count = len(db.execute_sql("SELECT * FROM students").data)

            if result1.success and remaining_count < 5:
                print("   ✓ DELETE 语句执行成功")
                print(f"      删除记录: {result1.affected_rows} 行")
                print(f"      剩余记录: {remaining_count} 行")
                tests_passed += 1
            else:
                print("   ✗ DELETE 语句执行失败")
                print(f"      结果: {result1.message}")

            # 测试7: 复合操作测试
            print("\n7. 复合操作测试...")
            tests_total += 1

            # 先查询
            before_result = db.execute_sql("SELECT * FROM students")
            before_count = len(before_result.data) if before_result.success else 0

            # 插入新记录
            insert_result = db.execute_sql(
                "INSERT INTO students VALUES (6, 'Frank', 24, 95)"
            )

            # 更新新记录
            update_result = db.execute_sql(
                "UPDATE students SET score = 96 WHERE name = 'Frank'"
            )

            # 查询验证
            after_result = db.execute_sql("SELECT * FROM students WHERE name = 'Frank'")

            if (
                insert_result.success
                and update_result.success
                and after_result.success
                and len(after_result.data) > 0
                and after_result.data[0].get("SCORE") == 96
            ):
                print("   ✓ 复合操作测试通过")
                print(f"      操作前记录数: {before_count}")
                print(f"      插入后Frank的分数: {after_result.data[0].get('SCORE')}")
                tests_passed += 1
            else:
                print("   ✗ 复合操作测试失败")

            # 测试总结
            print(f"\n测试总结:")
            print(f"   通过: {tests_passed}/{tests_total}")
            print(f"   成功率: {tests_passed/tests_total*100:.1f}%")

            if tests_passed == tests_total:
                print("   🎉 所有扩展功能测试通过！")
            else:
                print("   ⚠️  部分测试失败，需要检查实现")

    except Exception as e:
        print(f"\n✗ 测试过程中出现错误: {str(e)}")
        import traceback

        traceback.print_exc()

    # 清理文件
    if os.path.exists(db_path):
        os.remove(db_path)


def test_syntax_features():
    """测试语法功能"""
    print("\n" + "=" * 40)
    print("语法功能测试")
    print("=" * 40)

    from compiler import PlanGenerator, SemanticAnalyzer, SQLLexer, SQLParser
    from compiler.catalog import Catalog

    # 测试 UPDATE 语句的编译
    update_sql = "UPDATE students SET age = 25, score = 90 WHERE name = 'Alice'"

    try:
        # 词法分析
        lexer = SQLLexer(update_sql)
        tokens = lexer.tokenize()
        print(f"✓ 词法分析成功: {len(tokens)} 个 tokens")

        # 语法分析
        parser = SQLParser(tokens)
        ast = parser.parse()
        print("✓ 语法分析成功: AST 构建完成")

        # 语义分析
        catalog = Catalog()
        # 添加测试表到目录
        from compiler.catalog import ColumnInfo

        columns = [
            ColumnInfo(name="ID", data_type="INT", is_nullable=False),
            ColumnInfo(name="NAME", data_type="VARCHAR", is_nullable=True),
            ColumnInfo(name="AGE", data_type="INT", is_nullable=True),
            ColumnInfo(name="SCORE", data_type="INT", is_nullable=True),
        ]
        catalog.create_table("STUDENTS", columns)

        analyzer = SemanticAnalyzer(catalog)
        success, errors = analyzer.analyze(ast)

        if success:
            print("✓ 语义分析成功")
        else:
            print(f"✗ 语义分析失败: {errors}")

        # 执行计划生成
        plan_gen = PlanGenerator(catalog)
        plans = plan_gen.generate(ast)
        print(f"✓ 执行计划生成成功: {len(plans)} 个计划")

        for i, plan in enumerate(plans, 1):
            print(f"   计划 {i}: {plan.operator_type}")
            if hasattr(plan, "properties"):
                for key, value in plan.properties.items():
                    print(f"      {key}: {value}")

    except Exception as e:
        print(f"✗ 语法测试失败: {str(e)}")


if __name__ == "__main__":
    test_extended_features()
    test_syntax_features()
