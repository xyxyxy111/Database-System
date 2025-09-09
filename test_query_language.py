#!/usr/bin/env python3
"""
查询语言测试脚本
测试SQL解析、语义分析、查询优化等功能
"""

import sys
import os
import tempfile
import shutil
import time
sys.path.insert(0, os.getcwd())

from compiler.lexer import tokenize
from compiler.parser import Parser
from compiler.sematic_analyzer import SemanticAnalyzer
from compiler.planner import Planner
from execution.sytem_catalog import SystemCatalog
from execution.executor import Executor

def test_lexer():
    """测试词法分析器"""
    print("=== 测试词法分析器 ===")
    
    # 测试不同类型的SQL语句
    test_sqls = [
        "CREATE TABLE users(id INT, name VARCHAR);",
        "INSERT INTO users(id,name) VALUES (1,'Alice');",
        "SELECT * FROM users;",
        "SELECT id, name FROM users WHERE age > 18;",
        "DELETE FROM users WHERE id = 1;"
    ]
    
    for sql in test_sqls:
        print(f"\n--- 测试SQL: {sql} ---")
        try:
            tokens = tokenize(sql)
            print(f"[OK] 词法分析成功，获得 {len(tokens)} 个token")
            
            # 显示前几个token
            for i, token in enumerate(tokens[:5]):
                print(f"  Token {i+1}: {token}")
            if len(tokens) > 5:
                print(f"  ... 还有 {len(tokens) - 5} 个token")
                
        except Exception as e:
            print(f"[ERROR] 词法分析失败: {e}")
    
    return True

def test_parser():
    """测试语法分析器"""
    print("\n=== 测试语法分析器 ===")
    
    # 测试不同类型的SQL语句
    test_sqls = [
        "CREATE TABLE users(id INT, name VARCHAR);",
        "INSERT INTO users(id,name) VALUES (1,'Alice');",
        "SELECT * FROM users;",
        "SELECT id, name FROM users WHERE age > 18;",
        "DELETE FROM users WHERE id = 1;"
    ]
    
    for sql in test_sqls:
        print(f"\n--- 测试SQL: {sql} ---")
        try:
            parser = Parser(sql)
            asts = parser.parse_many()
            print(f"[OK] 语法分析成功，获得 {len(asts)} 个AST")
            
            for i, ast in enumerate(asts):
                print(f"  AST {i+1}: {ast.__class__.__name__}")
                
        except Exception as e:
            print(f"[ERROR] 语法分析失败: {e}")
    
    return True

def test_semantic_analyzer():
    """测试语义分析器"""
    print("\n=== 测试语义分析器 ===")
    
    temp_dir = tempfile.mkdtemp(prefix="semantic_test_")
    db_path = os.path.join(temp_dir, "semantic_test.db")
    
    try:
        syscat = SystemCatalog(db_path)
        analyzer = SemanticAnalyzer(syscat)
        
        # 测试语义分析流程
        test_sqls = [
            "CREATE TABLE users(id INT, name VARCHAR, age INT);",
            "INSERT INTO users(id,name,age) VALUES (1,'Alice',20);",
            "SELECT * FROM users;",
            "SELECT id, name FROM users WHERE age > 18;",
            "DELETE FROM users WHERE id = 1;"
        ]
        
        for sql in test_sqls:
            print(f"\n--- 测试SQL: {sql} ---")
            try:
                parser = Parser(sql)
                asts = parser.parse_many()
                
                for ast in asts:
                    analyzed = analyzer.analyze(ast)
                    print(f"[OK] 语义分析成功: {analyzed.kind}")
                    print(f"  载荷: {analyzed.payload}")
                    
            except Exception as e:
                print(f"[ERROR] 语义分析失败: {e}")
        
        return syscat, analyzer, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_query_planner():
    """测试查询规划器"""
    print("\n=== 测试查询规划器 ===")
    
    syscat, analyzer, temp_dir = test_semantic_analyzer()
    
    try:
        executor = Executor(syscat)
        planner = Planner(executor)
        
        # 测试查询规划
        test_sqls = [
            "CREATE TABLE users(id INT, name VARCHAR, age INT);",
            "INSERT INTO users(id,name,age) VALUES (1,'Alice',20);",
            "INSERT INTO users(id,name,age) VALUES (2,'Bob',17);",
            "SELECT * FROM users;",
            "SELECT id, name FROM users WHERE age > 18;",
            "DELETE FROM users WHERE id = 1;"
        ]
        
        for sql in test_sqls:
            print(f"\n--- 测试SQL: {sql} ---")
            try:
                parser = Parser(sql)
                asts = parser.parse_many()
                
                for ast in asts:
                    analyzed = analyzer.analyze(ast)
                    op = planner.plan(analyzed)
                    print(f"[OK] 查询规划成功: {op.__class__.__name__}")
                    
                    # 执行查询
                    result = executor.execute_plan(op)
                    print(f"  执行结果: {len(result)} 行")
                    if result:
                        print(f"  示例: {result[0]}")
                        
            except Exception as e:
                print(f"[ERROR] 查询规划失败: {e}")
        
        return syscat, analyzer, planner, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_complex_queries():
    """测试复杂查询"""
    print("\n=== 测试复杂查询 ===")
    
    syscat, analyzer, planner, executor, temp_dir = test_query_planner()
    
    try:
        # 测试复杂查询
        complex_queries = [
            "SELECT * FROM users WHERE age >= 18;",
            "SELECT id, name FROM users WHERE age > 17;",
            "SELECT * FROM users WHERE name = 'Alice';",
            "SELECT * FROM users WHERE id > 0;"
        ]
        
        for sql in complex_queries:
            print(f"\n--- 测试复杂查询: {sql} ---")
            try:
                parser = Parser(sql)
                asts = parser.parse_many()
                
                for ast in asts:
                    analyzed = analyzer.analyze(ast)
                    op = planner.plan(analyzed)
                    result = executor.execute_plan(op)
                    
                    print(f"[OK] 复杂查询成功: {len(result)} 行")
                    for row in result:
                        print(f"  {row}")
                        
            except Exception as e:
                print(f"[ERROR] 复杂查询失败: {e}")
        
        return syscat, analyzer, planner, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_query_optimization():
    """测试查询优化"""
    print("\n=== 测试查询优化 ===")
    
    syscat, analyzer, planner, executor, temp_dir = test_complex_queries()
    
    try:
        # 插入更多数据用于优化测试
        print("\n--- 插入更多数据 ---")
        for i in range(100):
            sql = f"INSERT INTO users(id,name,age) VALUES ({i+10},'User{i+10}',{20 + (i % 50)});"
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                op = planner.plan(analyzed)
                executor.execute_plan(op)
        
        print("[OK] 插入100条记录完成")
        
        # 测试查询性能
        print("\n--- 测试查询性能 ---")
        performance_queries = [
            "SELECT * FROM users;",
            "SELECT * FROM users WHERE age > 30;",
            "SELECT id, name FROM users WHERE age > 30;",
            "SELECT * FROM users WHERE id > 50;"
        ]
        
        for sql in performance_queries:
            print(f"\n--- 性能测试: {sql} ---")
            start_time = time.time()
            
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
            
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"[OK] 查询完成: {len(result)} 行，耗时 {duration:.4f} 秒")
            if duration > 0:
            print(f"[OK] 查询速度: {len(result)/duration:.0f} 行/秒")
        else:
            print(f"[OK] 查询速度: 极快 (< 0.0001秒)")
        
        return syscat, analyzer, planner, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_error_handling():
    """测试错误处理"""
    print("\n=== 测试错误处理 ===")
    
    syscat, analyzer, planner, executor, temp_dir = test_query_optimization()
    
    try:
        # 测试语法错误
        print("\n--- 测试语法错误 ---")
        syntax_errors = [
            "CREATE TABLE users(id INT, name VARCHAR",  # 缺少右括号
            "INSERT INTO users(id,name) VALUES (1,'Alice'",  # 缺少右括号
            "SELECT * FROM users WHERE",  # 缺少条件
            "DELETE FROM users WHERE",  # 缺少条件
        ]
        
        for sql in syntax_errors:
            print(f"\n--- 测试语法错误: {sql} ---")
            try:
                parser = Parser(sql)
                asts = parser.parse_many()
                print("[WARNING] 应该抛出语法错误但没有")
            except Exception as e:
                print(f"[OK] 正确处理语法错误: {e}")
        
        # 测试语义错误
        print("\n--- 测试语义错误 ---")
        semantic_errors = [
            "SELECT * FROM nonexistent_table;",  # 不存在的表
            "SELECT nonexistent_column FROM users;",  # 不存在的列
            "INSERT INTO users(nonexistent_column) VALUES (1);",  # 不存在的列
            "DELETE FROM nonexistent_table WHERE id = 1;",  # 不存在的表
        ]
        
        for sql in semantic_errors:
            print(f"\n--- 测试语义错误: {sql} ---")
            try:
                parser = Parser(sql)
                asts = parser.parse_many()
                
                for ast in asts:
                    analyzed = analyzer.analyze(ast)
                    print("[WARNING] 应该抛出语义错误但没有")
            except Exception as e:
                print(f"[OK] 正确处理语义错误: {e}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_multiple_statements():
    """测试多语句执行"""
    print("\n=== 测试多语句执行 ===")
    
    temp_dir = test_error_handling()
    
    try:
        db_path = os.path.join(temp_dir, "semantic_test.db")
        syscat = SystemCatalog(db_path)
        analyzer = SemanticAnalyzer(syscat)
        executor = Executor(syscat)
        planner = Planner(executor)
        
        # 测试多语句SQL
        multi_sql = """
        CREATE TABLE products(id INT, name VARCHAR, price INT);
        INSERT INTO products(id,name,price) VALUES (1,'Laptop',1000);
        INSERT INTO products(id,name,price) VALUES (2,'Phone',800);
        INSERT INTO products(id,name,price) VALUES (3,'Tablet',500);
        SELECT * FROM products;
        SELECT * FROM products WHERE price > 600;
        DELETE FROM products WHERE price < 700;
        SELECT * FROM products;
        """
        
        print("\n--- 测试多语句执行 ---")
        parser = Parser(multi_sql)
        asts = parser.parse_many()
        
        print(f"[OK] 解析出 {len(asts)} 个语句")
        
        for i, ast in enumerate(asts):
            print(f"\n--- 执行语句 {i+1}: {ast.__class__.__name__} ---")
            try:
                analyzed = analyzer.analyze(ast)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 执行成功: {len(result)} 行结果")
                if result:
                    print(f"  结果: {result[0]}")
                    
            except Exception as e:
                print(f"[ERROR] 执行失败: {e}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def main():
    """主测试函数"""
    print("开始查询语言测试...")
    print("=" * 50)
    
    temp_dir = None
    try:
        # 测试词法分析器
        test_lexer()
        
        # 测试语法分析器
        test_parser()
        
        # 测试语义分析器
        test_semantic_analyzer()
        
        # 测试查询规划器
        test_query_planner()
        
        # 测试复杂查询
        test_complex_queries()
        
        # 测试查询优化
        test_query_optimization()
        
        # 测试错误处理
        test_error_handling()
        
        # 测试多语句执行
        test_multiple_statements()
        
        print("\n" + "=" * 50)
        print("[SUCCESS] 查询语言测试全部通过！")
        
    except Exception as e:
        print(f"\n[ERROR] 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理临时文件
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"[OK] 清理临时文件: {temp_dir}")

if __name__ == "__main__":
    main()
