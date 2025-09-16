#!/usr/bin/env python3
"""
测试谓词下推功能
"""

from database.db_engine import dbEngine
import os
import tempfile


def test_predicate_pushdown():
    """测试谓词下推功能"""
    print("=== 谓词下推功能测试 ===")

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

        # 测试JOIN查询（模拟谓词下推场景）
        print("\n3. 测试JOIN查询:")

        # 注意：由于当前的JOIN语法可能还不完全支持，我们先测试简单查询
        # 来验证优化器的基本工作

        test_queries = [
            {
                "sql": "SELECT * FROM users WHERE age > 25",
                "description": "简单WHERE查询",
            },
            {
                "sql": "SELECT * FROM users WHERE age > 20 AND name = 'Alice'",
                "description": "复合条件查询（可以优化为谓词分解）",
            },
        ]

        for i, test_case in enumerate(test_queries, 1):
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
                if result.data:
                    for row in result.data[:2]:  # 显示前2行
                        print(f"     {row}")
            else:
                print(f"   结果: ❌ 失败 - {result.message}")

            # 检查优化统计
            final_stats = engine.get_optimization_report()
            final_pushdown = (
                final_stats["statistics"]["predicate_pushdown"] if final_stats else 0
            )

            if final_pushdown > initial_pushdown:
                print(f"   🔧 检测到谓词下推优化!")

        # 显示最终优化报告
        print("\n4. 优化统计报告:")
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
        else:
            print("   ❌ 无法获取优化报告")

        engine.close()
        print("\n✅ 谓词下推测试完成")

    except Exception as e:
        print(f"\n❌ 测试过程中出现异常: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_predicate_analysis():
    """测试谓词分析功能"""
    print("\n=== 谓词分析功能测试 ===")

    try:
        # 测试优化器的谓词分析能力
        from compiler.query_optimizer import QueryOptimizer
        from compiler.ast_nodes import BinaryExpression, Identifier, Literal

        optimizer = QueryOptimizer()

        # 创建测试谓词: users.age > 25 AND users.name = 'Alice'
        age_predicate = BinaryExpression(
            Identifier("users.age"), ">", Literal(25, "INT"), line=1, column=1
        )

        name_predicate = BinaryExpression(
            Identifier("users.name"),
            "=",
            Literal("Alice", "VARCHAR"),
            line=1,
            column=20,
        )

        compound_predicate = BinaryExpression(
            age_predicate, "AND", name_predicate, line=1, column=15
        )

        # 测试谓词提取
        print("1. 测试谓词提取:")
        predicates = optimizer._extract_predicates(compound_predicate)
        print(f"   提取到 {len(predicates)} 个谓词:")
        for i, pred in enumerate(predicates, 1):
            print(f"     {i}. {pred}")

        # 测试表名提取
        print("\n2. 测试表名提取:")
        for i, pred in enumerate(predicates, 1):
            tables = optimizer._get_predicate_tables(pred)
            print(f"   谓词 {i} 涉及的表: {tables}")

        print("\n✅ 谓词分析测试完成")

    except Exception as e:
        print(f"\n❌ 谓词分析测试失败: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    print("MiniDB 谓词下推功能测试")
    print("=" * 50)

    test_predicate_analysis()
    test_predicate_pushdown()

    print("\n" + "=" * 50)
    print("测试完成!")
