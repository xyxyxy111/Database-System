"""
调试聚合函数解析
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from compiler import SQLLexer, SQLParser


def test_parse_aggregate():
    """测试聚合函数解析"""
    print("=== 测试聚合函数解析 ===")

    sql = "SELECT COUNT(*) FROM numbers"
    print(f"SQL: {sql}")

    # 词法分析
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()

    print("\nTokens:")
    for token in tokens:
        if token.value == "":
            print(f"{token.type.name}")
        else:
            print(f"{token.type.name}: {token.value}")

    # 语法分析
    try:
        parser = SQLParser(tokens)
        ast = parser.parse()
        print(f"\nAST解析成功: {type(ast)}")

        if hasattr(ast, "statements") and ast.statements:
            stmt = ast.statements[0]
            print(f"Statement类型: {type(stmt)}")

            if hasattr(stmt, "select_list"):
                print(f"Select list长度: {len(stmt.select_list)}")
                for i, item in enumerate(stmt.select_list):
                    print(f"  Item {i}: {type(item)} - {item}")

    except Exception as e:
        print(f"\n解析失败: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_parse_aggregate()
