#!/usr/bin/env python3
"""
测试JOIN语法和谓词下推功能
"""

from database.db_engine import dbEngine
import os
import tempfile


def test_join_syntax():
    """测试JOIN语法解析"""
    print("=== JOIN语法测试 ===")

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        engine = dbEngine(db_path)

        # 创建测试表
        print("1. 创建测试表:")

        create_users_sql = """
        CREATE TABLE users (
            id INT,
            name VARCHAR(50),
            age INT
        )
        """
        result = engine.execute_sql(create_users_sql)
        print(f"   users表: {'✅ 成功' if result.success else '❌ 失败'}")

        create_orders_sql = """
        CREATE TABLE orders (
            id INT,
            user_id INT,
            amount INT
        )
        """
        result = engine.execute_sql(create_orders_sql)
        print(f"   orders表: {'✅ 成功' if result.success else '❌ 失败'}")

        # 插入测试数据
        print("\n2. 插入测试数据:")

        user_data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]

        for user_id, name, age in user_data:
            sql = f"INSERT INTO users VALUES ({user_id}, '{name}', {age})"
            result = engine.execute_sql(sql)
            print(f"   用户{name}: {'✅' if result.success else '❌'}")

        order_data = [(1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 3, 300)]

        for order_id, user_id, amount in order_data:
            sql = f"INSERT INTO orders VALUES ({order_id}, {user_id}, {amount})"
            result = engine.execute_sql(sql)
            print(f"   订单{order_id}: {'✅' if result.success else '❌'}")

        # 测试JOIN查询语法
        print("\n3. 测试JOIN查询语法:")

        join_test_cases = [
            {
                "sql": "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
                "description": "基本INNER JOIN",
            },
            {
                "sql": "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
                "description": "LEFT JOIN",
            },
            {
                "sql": "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 25",
                "description": "JOIN with WHERE条件（可能触发谓词下推）",
            },
            {
                "sql": "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 25 AND orders.amount > 150",
                "description": "复杂JOIN with多表WHERE条件",
            },
        ]

        for i, test_case in enumerate(join_test_cases, 1):
            print(f"\n   测试 {i}: {test_case['description']}")
            print(f"   SQL: {test_case['sql']}")

            # 执行前检查优化统计
            initial_stats = engine.get_optimization_report()
            initial_pushdown = (
                initial_stats["statistics"]["predicate_pushdown"]
                if initial_stats
                else 0
            )

            result = engine.execute_sql(test_case["sql"])

            if result.success:
                print(
                    f"   结果: ✅ 成功，返回 {len(result.data) if result.data else 0} 行"
                )
                if result.data and len(result.data) <= 3:
                    for row in result.data:
                        print(f"     {row}")
            else:
                print(f"   结果: ❌ 失败 - {result.message}")

            # 检查优化统计
            final_stats = engine.get_optimization_report()
            final_pushdown = (
                final_stats["statistics"]["predicate_pushdown"] if final_stats else 0
            )

            if final_pushdown > initial_pushdown:
                print(
                    f"   🔧 检测到谓词下推优化! ({final_pushdown - initial_pushdown} 次)"
                )

        # 显示最终优化报告
        print("\n4. 最终优化统计:")
        opt_report = engine.get_optimization_report()
        if opt_report:
            print(f"   📊 优化统计:")
            stats = opt_report["statistics"]
            print(f"     - 谓词下推: {stats['predicate_pushdown']} 次")
            print(f"     - 常量折叠: {stats['constant_folding']} 次")
            print(f"     - 表达式简化: {stats['expression_simplification']} 次")
            print(f"     - 冗余消除: {stats['redundant_elimination']} 次")

            if opt_report["applied_optimizations"]:
                print(f"   🔧 应用的优化:")
                for opt in opt_report["applied_optimizations"]:
                    print(f"     - {opt}")

        engine.close()
        print("\n✅ JOIN语法测试完成")

    except Exception as e:
        print(f"\n❌ 测试过程中出现异常: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_predicate_pushdown_manually():
    """手动测试谓词下推逻辑"""
    print("\n=== 手动测试谓词下推逻辑 ===")

    try:
        from compiler.query_optimizer import QueryOptimizer
        from compiler.ast_nodes import (
            BinaryExpression,
            Identifier,
            Literal,
            SelectStatement,
            JoinClause,
        )

        optimizer = QueryOptimizer()

        # 创建模拟的JOIN查询：
        # SELECT * FROM users JOIN orders ON users.id = orders.user_id
        # WHERE users.age > 25 AND orders.amount > 150

        # 创建WHERE条件
        age_condition = BinaryExpression(
            Identifier("users.age"), ">", Literal(25, "INT"), line=1, column=1
        )

        amount_condition = BinaryExpression(
            Identifier("orders.amount"), ">", Literal(150, "INT"), line=1, column=30
        )

        where_clause = BinaryExpression(
            age_condition, "AND", amount_condition, line=1, column=15
        )

        # 创建JOIN条件
        join_condition = BinaryExpression(
            Identifier("users.id"), "=", Identifier("orders.user_id"), line=1, column=1
        )

        # 创建JOIN子句
        join_clause = JoinClause(
            join_type="INNER",
            table_name="orders",
            on_condition=join_condition,
            line=1,
            column=1,
        )

        print("1. 原始查询结构:")
        print(f"   WHERE条件: {where_clause}")
        print(f"   JOIN条件: {join_clause.on_condition}")

        # 测试谓词下推
        print("\n2. 测试谓词下推:")

        # 提取谓词
        predicates = optimizer._extract_predicates(where_clause)
        print(f"   提取到 {len(predicates)} 个谓词:")
        for i, pred in enumerate(predicates, 1):
            print(f"     {i}. {pred}")

        # 分析每个谓词涉及的表
        print("\n3. 谓词表分析:")
        for i, pred in enumerate(predicates, 1):
            tables = optimizer._get_predicate_tables(pred)
            print(f"   谓词{i} 涉及表: {tables}")

        # 分析JOIN涉及的表
        join_tables = optimizer._get_join_tables(join_clause)
        print(f"\n4. JOIN涉及的表: {join_tables}")

        # 测试谓词下推决策
        print("\n5. 谓词下推决策:")
        for i, pred in enumerate(predicates, 1):
            can_push = optimizer._can_push_predicate(pred, join_clause)
            print(f"   谓词{i} 可以下推到JOIN: {'✅ 是' if can_push else '❌ 否'}")

        # 实际执行谓词下推
        print("\n6. 执行谓词下推:")
        optimized_joins, pushed_predicates = optimizer._apply_predicate_pushdown(
            [join_clause], where_clause
        )

        print(f"   下推的谓词数量: {len(pushed_predicates)}")
        for i, pred in enumerate(pushed_predicates, 1):
            print(f"     {i}. {pred}")

        if optimized_joins:
            print(f"   优化后的JOIN条件: {optimized_joins[0].on_condition}")

        # 检查优化统计
        print(f"\n7. 优化统计:")
        print(f"   谓词下推次数: {optimizer.optimization_stats['predicate_pushdown']}")
        print(f"   应用的优化: {optimizer.optimizations_applied}")

        print("\n✅ 手动谓词下推测试完成")

    except Exception as e:
        print(f"\n❌ 手动测试失败: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    print("MiniDB JOIN语法和谓词下推功能测试")
    print("=" * 50)

    test_predicate_pushdown_manually()
    test_join_syntax()

    print("\n" + "=" * 50)
    print("测试完成!")
