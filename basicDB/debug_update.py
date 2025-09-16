#!/usr/bin/env python3
"""调试 UPDATE 语句的语义分析问题"""

from compiler.catalog import Catalog, ColumnInfo
from compiler.lexer import SQLLexer
from compiler.parser import SQLParser
from compiler.semantic_analyzer import SemanticAnalyzer


def test_update_semantics():
    """测试 UPDATE 语句的语义分析"""

    # 创建目录
    catalog = Catalog()

    # 添加测试表
    columns = [
        ColumnInfo(name="ID", data_type="INT", is_nullable=False),
        ColumnInfo(name="NAME", data_type="STRING", is_nullable=True),
        ColumnInfo(name="AGE", data_type="INT", is_nullable=True),
        ColumnInfo(name="SCORE", data_type="INT", is_nullable=True),
    ]
    catalog.create_table("STUDENTS", columns)

    # UPDATE 语句
    sql = "UPDATE students SET age = 25, score = 90 WHERE name = 'Alice'"

    print(f"SQL: {sql}")

    # 词法分析
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        print(f"✓ 词法分析成功: {len(tokens)} 个 tokens")
        for token in tokens:
            print(f"  {token}")
    except Exception as e:
        print(f"✗ 词法分析失败: {e}")
        return

    # 语法分析
    try:
        parser = SQLParser(tokens)
        ast = parser.parse_statement()
        print(f"✓ 语法分析成功: {type(ast).__name__}")
        print(f"  表名: {ast.table_name}")
        print(f"  赋值: {ast.assignments}")
        print(f"  条件: {ast.where_clause}")

        # 打印赋值详情
        for i, (col, expr) in enumerate(ast.assignments):
            print(f"  赋值 {i+1}: {col} = {expr} (type: {type(expr).__name__})")
            if hasattr(expr, "value"):
                print(f"    值: {expr.value}")
            if hasattr(expr, "name"):
                print(f"    名称: {expr.name}")

    except Exception as e:
        print(f"✗ 语法分析失败: {e}")
        return

    # 语义分析
    try:
        analyzer = SemanticAnalyzer(catalog)
        analyzer.analyze(ast)
        print("✓ 语义分析成功")
        if analyzer.errors:
            print(f"警告: {len(analyzer.errors)} 个错误")
            for error in analyzer.errors:
                print(f"  {error}")
    except Exception as e:
        print(f"✗ 语义分析失败: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_update_semantics()
