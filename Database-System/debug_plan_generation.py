"""
调试执行计划生成
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from compiler import SQLLexer, SQLParser, PlanGenerator, SemanticAnalyzer
from compiler.catalog import Catalog


def test_plan_generation():
    """测试执行计划生成"""
    print("=== 测试执行计划生成 ===")

    sql = "SELECT COUNT(*) FROM numbers"
    print(f"SQL: {sql}")

    # 词法分析
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()

    # 语法分析
    parser = SQLParser(tokens)
    ast = parser.parse()

    # 创建目录
    catalog = Catalog()

    # 语义分析
    semantic_analyzer = SemanticAnalyzer(catalog)
    semantic_analyzer.analyze(ast)

    # 生成执行计划
    plan_generator = PlanGenerator(catalog)
    execution_plan = plan_generator.generate(ast)

    print(f"\n执行计划类型: {type(execution_plan)}")

    if isinstance(execution_plan, list):
        print(f"执行计划数量: {len(execution_plan)}")
        for i, plan in enumerate(execution_plan):
            print(f"\n计划 {i}: {type(plan)}")
            if hasattr(plan, "to_dict"):
                plan_dict = plan.to_dict()
                print(f"  计划字典: {plan_dict}")

                # 检查计划的properties
                properties = plan_dict.get("properties", {})
                print(f"  计划属性:")
                for key, value in properties.items():
                    print(f"    {key}: {value}")
                    if key == "select_list":
                        print(f"      Select list项数: {len(value) if value else 0}")
                        for j, item in enumerate(value or []):
                            print(f"        Item {j}: {type(item)} - {item}")
    else:
        print(f"执行计划字典: {execution_plan.to_dict()}")

        # 检查计划的properties
        plan_dict = execution_plan.to_dict()
        properties = plan_dict.get("properties", {})
        print(f"\n计划属性:")
        for key, value in properties.items():
            print(f"  {key}: {value}")
            if key == "select_list":
                print(f"    Select list项数: {len(value) if value else 0}")
                for i, item in enumerate(value or []):
                    print(f"      Item {i}: {type(item)} - {item}")


if __name__ == "__main__":
    test_plan_generation()
