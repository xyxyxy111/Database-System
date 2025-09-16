#!/usr/bin/env python3
"""验证MiniDB SQL编译器功能是否正常工作"""

import sys
import os

# 添加项目路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)


def test_imports():
    """测试模块导入"""
    print("1. 测试模块导入...")
    try:
        from compiler import SQLLexer, SQLParser, SemanticAnalyzer, PlanGenerator
        print("   ✓ 所有模块导入成功")
        return True
    except Exception as e:
        print(f"   ✗ 模块导入失败: {e}")
        return False


def test_lexer():
    """测试词法分析器"""
    print("2. 测试词法分析器...")
    try:
        from compiler import SQLLexer
        
        sql = "CREATE TABLE test(id INT, name VARCHAR(50));"
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        
        # 验证Token数量 (CREATE, TABLE, test, (, id, INT, ,, name, VARCHAR, (, 50, ), ), ;, EOF)
        expected_count = 15  # 包括EOF
        if len(tokens) == expected_count:
            print(f"   ✓ 词法分析成功，生成 {len(tokens)-1} 个Token")
            return True
        else:
            print(f"   ✗ Token数量不符，期望 {expected_count}，实际 {len(tokens)}")
            return False
            
    except Exception as e:
        print(f"   ✗ 词法分析失败: {e}")
        return False


def test_parser():
    """测试语法分析器"""
    print("3. 测试语法分析器...")
    try:
        from compiler import SQLLexer, SQLParser
        
        sql = "CREATE TABLE test(id INT); SELECT id FROM test;"
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        
        parser = SQLParser(tokens)
        ast = parser.parse()
        
        if len(ast.statements) == 2:
            print(f"   ✓ 语法分析成功，解析 {len(ast.statements)} 个语句")
            return True
        else:
            print(f"   ✗ 语句数量不符，期望 2，实际 {len(ast.statements)}")
            return False
            
    except Exception as e:
        print(f"   ✗ 语法分析失败: {e}")
        return False


def test_semantic():
    """测试语义分析器"""
    print("4. 测试语义分析器...")
    try:
        from compiler import SQLLexer, SQLParser, SemanticAnalyzer
        
        sql = "CREATE TABLE test(id INT); SELECT id FROM test;"
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        
        analyzer = SemanticAnalyzer()
        success, errors = analyzer.analyze(ast)
        
        if success and len(errors) == 0:
            print("   ✓ 语义分析成功，无错误")
            return True
        else:
            print(f"   ✗ 语义分析失败，发现 {len(errors)} 个错误")
            return False
            
    except Exception as e:
        print(f"   ✗ 语义分析失败: {e}")
        return False


def test_plan_generator():
    """测试执行计划生成器"""
    print("5. 测试执行计划生成器...")
    try:
        from compiler import SQLLexer, SQLParser, SemanticAnalyzer, PlanGenerator
        
        sql = "CREATE TABLE test(id INT); SELECT id FROM test;"
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        
        analyzer = SemanticAnalyzer()
        success, errors = analyzer.analyze(ast)
        
        if success:
            generator = PlanGenerator(analyzer.catalog)
            plans = generator.generate(ast)
            
            if len(plans) == 2:
                print(f"   ✓ 执行计划生成成功，生成 {len(plans)} 个计划")
                return True
            else:
                print(f"   ✗ 计划数量不符，期望 2，实际 {len(plans)}")
                return False
        else:
            print("   ✗ 语义分析失败，无法生成执行计划")
            return False
            
    except Exception as e:
        print(f"   ✗ 执行计划生成失败: {e}")
        return False


def test_complete_workflow():
    """测试完整工作流程"""
    print("6. 测试完整工作流程...")
    try:
        from compiler import SQLLexer, SQLParser, SemanticAnalyzer, PlanGenerator
        
        sql = """
        CREATE TABLE student(id INT, name VARCHAR(50), age INT);
        INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
        SELECT id, name FROM student WHERE age > 18;
        """
        
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
            print(f"   ✗ 语义分析失败: {errors}")
            return False
        
        # 执行计划生成
        generator = PlanGenerator(analyzer.catalog)
        plans = generator.generate(ast)
        
        print(f"   ✓ 完整流程成功！处理了 {len(ast.statements)} 个语句，生成了 {len(plans)} 个执行计划")
        return True
        
    except Exception as e:
        print(f"   ✗ 完整流程失败: {e}")
        return False


def main():
    """主函数"""
    print("MiniDB SQL编译器功能验证")
    print("=" * 40)
    
    tests = [
        test_imports,
        test_lexer,
        test_parser,
        test_semantic,
        test_plan_generator,
        test_complete_workflow
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 40)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！SQL编译器功能正常！")
        return 0
    else:
        print("❌ 部分测试失败，请检查错误信息")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
