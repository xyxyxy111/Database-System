#!/usr/bin/env python3
"""
测试查询优化和错误诊断功能
"""

from database.db_engine import dbEngine
import os
import tempfile


def test_query_optimization():
    """测试查询优化功能"""
    print("=== 测试查询优化功能 ===")

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        engine = dbEngine(db_path)

        # 创建测试表
        create_sql = """
        CREATE TABLE users (
            id INT,
            name VARCHAR(50),
            age INT
        )
        """
        result = engine.execute_sql(create_sql)
        print(f"创建表: {result.success}")

        # 插入测试数据
        insert_sqls = [
            "INSERT INTO users VALUES (1, 'Alice', 25)",
            "INSERT INTO users VALUES (2, 'Bob', 30)",
            "INSERT INTO users VALUES (3, 'Charlie', 35)",
        ]

        for sql in insert_sqls:
            result = engine.execute_sql(sql)
            print(f"插入数据: {result.success}")

        # 测试可优化的查询
        print("\n--- 测试查询优化 ---")

        # 带常量表达式的查询
        test_sql = "SELECT * FROM users WHERE age > 20 + 5"
        print(f"\n执行SQL: {test_sql}")
        result = engine.execute_sql(test_sql)
        print(f"结果: {result.success}")
        if result.data:
            print(f"返回 {len(result.data)} 行数据")

        # 复杂条件查询
        test_sql2 = (
            "SELECT * FROM users WHERE (age > 25 AND age < 40) OR name = 'Alice'"
        )
        print(f"\n执行SQL: {test_sql2}")
        result = engine.execute_sql(test_sql2)
        print(f"结果: {result.success}")
        if result.data:
            print(f"返回 {len(result.data)} 行数据")

        # 获取优化报告
        opt_report = engine.get_optimization_report()
        if opt_report:
            print(f"\n📊 优化统计:")
            print(f"  - 总查询数: {opt_report.get('total_queries', 0)}")
            print(f"  - 优化查询数: {opt_report.get('optimized_queries', 0)}")
            print(f"  - 优化率: {opt_report.get('optimization_rate', 0):.2%}")

        # 获取性能报告
        perf_report = engine.get_performance_report()
        if perf_report:
            print(f"\n⏱️  性能统计:")
            print(f"  - 总查询数: {perf_report.get('total_queries', 0)}")
            print(
                f"  - 平均执行时间: {perf_report.get('average_execution_time', 0):.3f}秒"
            )
            if perf_report.get("slow_queries"):
                print(f"  - 慢查询数: {len(perf_report['slow_queries'])}")

        engine.close()

    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_error_diagnostics():
    """测试错误诊断功能"""
    print("\n=== 测试错误诊断功能 ===")

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        engine = dbEngine(db_path)

        # 测试各种错误情况
        error_test_cases = [
            # 语法错误
            {
                "sql": "SELET * FROM users",
                "description": "拼写错误 (SELET instead of SELECT)",
            },
            {
                "sql": "SELECT * FORM users",
                "description": "关键字拼写错误 (FORM instead of FROM)",
            },
            {"sql": "SELECT * FROM users WHERE", "description": "不完整的WHERE子句"},
            {"sql": "SELECT * FROM (users", "description": "括号不匹配"},
            {
                "sql": "SELECT * FROM users WHERE name = 'Alice",
                "description": "引号不匹配",
            },
            # 语义错误
            {"sql": "SELECT * FROM nonexistent_table", "description": "表不存在"},
            {"sql": "SELECT nonexistent_column FROM users", "description": "列不存在"},
        ]

        # 先创建一个测试表
        create_sql = "CREATE TABLE users (id INT, name VARCHAR(50), age INT)"
        engine.execute_sql(create_sql)

        print("\n--- 测试错误诊断 ---")

        for i, test_case in enumerate(error_test_cases, 1):
            print(f"\n{i}. {test_case['description']}")
            print(f"SQL: {test_case['sql']}")
            print("执行结果:")

            result = engine.execute_sql(test_case["sql"])
            if not result.success:
                # 错误消息会通过增强的错误诊断显示
                print(f"❌ {result.message}")
            else:
                print("✅ 意外成功")

        engine.close()

    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_performance_analysis():
    """测试性能分析功能"""
    print("\n=== 测试性能分析功能 ===")

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        engine = dbEngine(db_path)

        # 创建测试表
        create_sql = "CREATE TABLE large_table (id INT, data VARCHAR(100))"
        engine.execute_sql(create_sql)

        # 插入大量数据来测试性能
        print("插入测试数据...")
        for i in range(100):
            sql = f"INSERT INTO large_table VALUES ({i}, 'data_{i}')"
            engine.execute_sql(sql)

        # 执行一些查询来测试性能分析
        print("\n--- 性能分析测试 ---")

        queries = [
            "SELECT * FROM large_table",
            "SELECT * FROM large_table WHERE id > 50",
            "SELECT COUNT(*) FROM large_table",
            "SELECT * FROM large_table WHERE data LIKE '%data_1%'",
        ]

        for query in queries:
            print(f"\n执行: {query}")
            result = engine.execute_sql(query)
            print(f"结果: {result.success}")
            if result.data:
                print(f"返回 {len(result.data)} 行")

        # 显示最终性能报告
        perf_report = engine.get_performance_report()
        if perf_report:
            print(f"\n📈 最终性能报告:")
            print(f"  - 总查询数: {perf_report.get('total_queries', 0)}")
            print(
                f"  - 平均执行时间: {perf_report.get('average_execution_time', 0):.3f}秒"
            )
            print(f"  - 最快查询: {perf_report.get('fastest_query_time', 0):.3f}秒")
            print(f"  - 最慢查询: {perf_report.get('slowest_query_time', 0):.3f}秒")

            slow_queries = perf_report.get("slow_queries", [])
            if slow_queries:
                print(f"  - 慢查询数: {len(slow_queries)}")
                for sq in slow_queries[:3]:  # 显示前3个慢查询
                    print(
                        f"    * {sq.get('sql', '')[:50]}... ({sq.get('execution_time', 0):.3f}秒)"
                    )

        engine.close()

    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)


def main():
    """主测试函数"""
    print("MiniDB 查询优化和错误诊断功能测试")
    print("=" * 50)

    try:
        # 测试查询优化
        test_query_optimization()

        # 测试错误诊断
        test_error_diagnostics()

        # 测试性能分析
        test_performance_analysis()

        print("\n" + "=" * 50)
        print("✅ 所有测试完成!")

    except Exception as e:
        print(f"\n❌ 测试过程中出现错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
