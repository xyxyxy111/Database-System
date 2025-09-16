#!/usr/bin/env python3
"""
简化的优化和诊断测试
"""

from database.db_engine import dbEngine
import os
import tempfile


def test_basic_functionality():
    """测试基本功能"""
    print("=== 基本功能测试 ===")

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        engine = dbEngine(db_path)

        # 创建测试表
        print("1. 创建表测试:")
        create_sql = "CREATE TABLE test_table (id INT, name VARCHAR(50))"
        result = engine.execute_sql(create_sql)
        print(f"   结果: {'✅ 成功' if result.success else '❌ 失败'}")
        if not result.success:
            print(f"   错误: {result.message}")

        # 插入数据测试
        print("\n2. 插入数据测试:")
        insert_sql = "INSERT INTO test_table VALUES (1, 'Alice')"
        result = engine.execute_sql(insert_sql)
        print(f"   结果: {'✅ 成功' if result.success else '❌ 失败'}")
        if not result.success:
            print(f"   错误: {result.message}")

        # 简单查询测试
        print("\n3. 简单查询测试:")
        select_sql = "SELECT * FROM test_table"
        result = engine.execute_sql(select_sql)
        print(f"   结果: {'✅ 成功' if result.success else '❌ 失败'}")
        if result.success and result.data:
            print(f"   返回 {len(result.data)} 行数据")
            print(f"   数据: {result.data[0] if result.data else 'None'}")
        elif not result.success:
            print(f"   错误: {result.message}")

        # 测试优化功能是否可用
        print("\n4. 优化功能测试:")
        opt_report = engine.get_optimization_report()
        if opt_report is not None:
            print("   ✅ 查询优化功能可用")
            print(f"   优化统计: {opt_report}")
        else:
            print("   ❌ 查询优化功能不可用")

        # 测试性能分析功能是否可用
        print("\n5. 性能分析测试:")
        perf_report = engine.get_performance_report()
        if perf_report is not None:
            print("   ✅ 性能分析功能可用")
            print(f"   性能统计: {perf_report}")
        else:
            print("   ❌ 性能分析功能不可用")

        # 测试错误诊断
        print("\n6. 错误诊断测试:")
        error_sql = "SELET * FROM test_table"  # 故意的拼写错误
        result = engine.execute_sql(error_sql)
        print(f"   结果: {'✅ 捕获错误' if not result.success else '❌ 应该失败'}")
        if not result.success:
            print(f"   错误信息: {result.message[:100]}...")

        engine.close()
        print("\n✅ 基本功能测试完成")

    except Exception as e:
        print(f"\n❌ 测试过程中出现异常: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_advanced_features_status():
    """测试高级功能状态"""
    print("\n=== 高级功能状态检查 ===")

    try:
        # 检查模块是否可以导入
        modules_to_test = [
            ("compiler.query_optimizer", "QueryOptimizer"),
            ("compiler.error_diagnostics", "ErrorDiagnostics"),
            ("compiler.performance_analyzer", "PerformanceAnalyzer"),
        ]

        for module_name, class_name in modules_to_test:
            try:
                module = __import__(module_name, fromlist=[class_name])
                cls = getattr(module, class_name)
                instance = cls()
                print(f"✅ {class_name}: 可用")
            except Exception as e:
                print(f"❌ {class_name}: 不可用 - {e}")

        # 检查DatabaseEngine中的高级功能标志
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            engine = dbEngine(db_path)

            features_status = {
                "query_optimizer": engine.query_optimizer is not None,
                "error_diagnostics": engine.error_diagnostics is not None,
                "performance_analyzer": engine.performance_analyzer is not None,
            }

            print(f"\nDatabaseEngine 功能状态:")
            for feature, status in features_status.items():
                status_str = "✅ 已启用" if status else "❌ 未启用"
                print(f"  {feature}: {status_str}")

            engine.close()

        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    except Exception as e:
        print(f"❌ 功能状态检查失败: {e}")


if __name__ == "__main__":
    print("MiniDB 优化和诊断功能 - 简化测试")
    print("=" * 50)

    test_advanced_features_status()
    test_basic_functionality()

    print("\n" + "=" * 50)
    print("测试完成!")
