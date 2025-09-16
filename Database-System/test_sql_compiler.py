"""
SQL编译器测试程序
用于测试词法分析、语法分析、语义分析和执行计划生成功能
"""

import sys
import os

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from compiler import SQLLexer, SQLParser, SemanticAnalyzer, PlanGenerator


def test_lexer():
    """测试词法分析器"""
    print("=" * 60)
    print("测试词法分析器")
    print("=" * 60)
    
    test_sql = """
    CREATE TABLE student(
        id INT,
        name VARCHAR(50),
        age INT
    );
    
    INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
    SELECT id, name FROM student WHERE age > 18;
    DELETE FROM student WHERE id = 1;
    """
    
    print("输入SQL:")
    print(test_sql)
    print()
    
    try:
        lexer = SQLLexer(test_sql)
        tokens = lexer.tokenize()
        
        print("词法分析结果:")
        print("-" * 40)
        for token in tokens:
            if token.type.name != 'EOF':
                print(f"{token.type.name:<12} '{token.value}'")
        
        print(f"\n共生成 {len([t for t in tokens if t.type.name != 'EOF'])} 个Token")
        return tokens
        
    except Exception as e:
        print(f"词法分析失败: {e}")
        return None


def test_parser(tokens):
    """测试语法分析器"""
    print("\n" + "=" * 60)
    print("测试语法分析器")
    print("=" * 60)
    
    if not tokens:
        print("没有可用的Token进行语法分析")
        return None
    
    try:
        parser = SQLParser(tokens)
        ast = parser.parse()
        
        print("语法分析成功!")
        print("抽象语法树结构:")
        print("-" * 40)
        
        for i, stmt in enumerate(ast.statements, 1):
            print(f"{i}. {stmt}")
        
        return ast
        
    except Exception as e:
        print(f"语法分析失败: {e}")
        return None


def test_semantic_analyzer(ast):
    """测试语义分析器"""
    print("\n" + "=" * 60)
    print("测试语义分析器")
    print("=" * 60)
    
    if not ast:
        print("没有可用的AST进行语义分析")
        return None, None
    
    try:
        analyzer = SemanticAnalyzer()
        success, errors = analyzer.analyze(ast)
        
        if success:
            print("语义分析通过!")
            print(f"建立了 {len(analyzer.catalog.get_tables())} 个表的目录信息")
            
            # 显示catalog信息
            for table_name in analyzer.catalog.get_tables():
                table_info = analyzer.catalog.get_table_info(table_name)
                print(f"表 '{table_name}': {[col.name + ' ' + col.data_type for col in table_info.columns]}")
        else:
            print("语义分析发现错误:")
            for error in errors:
                print(f"  - {error}")
        
        return analyzer.catalog, success
        
    except Exception as e:
        print(f"语义分析失败: {e}")
        return None, False


def test_plan_generator(ast, catalog):
    """测试执行计划生成器"""
    print("\n" + "=" * 60)
    print("测试执行计划生成器")
    print("=" * 60)
    
    if not ast or not catalog:
        print("没有可用的AST或Catalog进行执行计划生成")
        return None
    
    try:
        generator = PlanGenerator(catalog)
        plans = generator.generate(ast)
        
        print("执行计划生成成功!")
        print("执行计划:")
        print("-" * 40)
        
        for i, plan in enumerate(plans, 1):
            print(f"\nPlan {i}:")
            print(plan.to_tree_string())
        
        return plans
        
    except Exception as e:
        print(f"执行计划生成失败: {e}")
        return None


def test_error_cases():
    """测试错误处理"""
    print("\n" + "=" * 60)
    print("测试错误处理")
    print("=" * 60)
    
    error_sqls = [
        # 语法错误
        ("缺少分号", "CREATE TABLE test(id INT)"),
        # 词法错误  
        ("未闭合字符串", "SELECT * FROM test WHERE name = 'Alice"),
        # 语义错误
        ("表不存在", "SELECT * FROM nonexistent;"),
        ("列不存在", "CREATE TABLE test(id INT); SELECT name FROM test;"),
    ]
    
    for test_name, sql in error_sqls:
        print(f"\n测试 {test_name}:")
        print(f"SQL: {sql}")
        
        try:
            # 词法分析
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            
            # 语法分析
            parser = SQLParser(tokens)
            ast = parser.parse()
            
            # 语义分析
            analyzer = SemanticAnalyzer()
            success, errors = analyzer.analyze(ast)
            
            if not success:
                print("✓ 正确检测到语义错误:")
                for error in errors:
                    print(f"  {error}")
            else:
                print("✗ 未检测到预期错误")
                
        except Exception as e:
            print(f"✓ 正确检测到错误: {e}")


def main():
    """主测试函数"""
    print("SQL编译器综合测试")
    print("作者: MiniDB项目组")
    print("日期: 2025-09-08")
    
    # 测试正常流程
    tokens = test_lexer()
    ast = test_parser(tokens)
    catalog, semantic_success = test_semantic_analyzer(ast)
    if semantic_success:
        plans = test_plan_generator(ast, catalog)
    
    # 测试错误处理
    test_error_cases()
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
