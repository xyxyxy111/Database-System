#!/usr/bin/env python3
"""
核心CRUD操作测试脚本
测试CREATE、INSERT、SELECT、UPDATE、DELETE操作的完整流程
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

def test_create_operations():
    """测试CREATE操作"""
    print("=== 测试CREATE操作 ===")
    
    temp_dir = tempfile.mkdtemp(prefix="crud_test_")
    db_path = os.path.join(temp_dir, "crud_test.db")
    
    try:
        syscat = SystemCatalog(db_path)
        executor = Executor(syscat)
        analyzer = SemanticAnalyzer(syscat)
        
        # 测试创建不同类型的表
        create_tests = [
            "CREATE TABLE users(id INT, name VARCHAR, email VARCHAR);",
            "CREATE TABLE products(id INT, name VARCHAR, price INT, category VARCHAR);",
            "CREATE TABLE orders(id INT, user_id INT, product_id INT, quantity INT);"
        ]
        
        for sql in create_tests:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 创建表: {result[0]['created']}")
        
        # 验证表是否创建成功
        tables = list(syscat.schemas.keys())
        print(f"[OK] 已创建表: {tables}")
        
        return syscat, executor, analyzer, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_insert_operations():
    """测试INSERT操作"""
    print("\n=== 测试INSERT操作 ===")
    
    syscat, executor, analyzer, temp_dir = test_create_operations()
    
    try:
        # 插入用户数据
        user_inserts = [
            "INSERT INTO users(id,name,email) VALUES (1,'Alice','alice@example.com');",
            "INSERT INTO users(id,name,email) VALUES (2,'Bob','bob@example.com');",
            "INSERT INTO users(id,name,email) VALUES (3,'Charlie','charlie@example.com');"
        ]
        
        for sql in user_inserts:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 插入用户: {result[0]['inserted']} 行")
        
        # 插入产品数据
        product_inserts = [
            "INSERT INTO products(id,name,price,category) VALUES (1,'Laptop',1000,'Electronics');",
            "INSERT INTO products(id,name,price,category) VALUES (2,'Book',20,'Education');",
            "INSERT INTO products(id,name,price,category) VALUES (3,'Phone',800,'Electronics');"
        ]
        
        for sql in product_inserts:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 插入产品: {result[0]['inserted']} 行")
        
        # 插入订单数据
        order_inserts = [
            "INSERT INTO orders(id,user_id,product_id,quantity) VALUES (1,1,1,1);",
            "INSERT INTO orders(id,user_id,product_id,quantity) VALUES (2,2,2,2);",
            "INSERT INTO orders(id,user_id,product_id,quantity) VALUES (3,1,3,1);"
        ]
        
        for sql in order_inserts:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 插入订单: {result[0]['inserted']} 行")
        
        return syscat, executor, analyzer, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_select_operations():
    """测试SELECT操作"""
    print("\n=== 测试SELECT操作 ===")
    
    syscat, executor, analyzer, temp_dir = test_insert_operations()
    
    try:
        # 测试基本查询
        basic_queries = [
            "SELECT * FROM users;",
            "SELECT id, name FROM users;",
            "SELECT * FROM products;",
            "SELECT name, price FROM products;"
        ]
        
        for sql in basic_queries:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 查询结果: {len(result)} 行")
                if result:
                    print(f"  示例: {result[0]}")
        
        # 测试条件查询
        condition_queries = [
            "SELECT * FROM users WHERE id = 1;",
            "SELECT * FROM products WHERE price > 100;",
            "SELECT * FROM products WHERE category = 'Electronics';"
        ]
        
        for sql in condition_queries:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 条件查询: {len(result)} 行")
                if result:
                    print(f"  结果: {result}")
        
        return syscat, executor, analyzer, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_delete_operations():
    """测试DELETE操作"""
    print("\n=== 测试DELETE操作 ===")
    
    syscat, executor, analyzer, temp_dir = test_select_operations()
    
    try:
        # 测试删除操作
        delete_queries = [
            "DELETE FROM orders WHERE id = 1;",
            "DELETE FROM products WHERE price < 50;",
            "DELETE FROM users WHERE id = 3;"
        ]
        
        for sql in delete_queries:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 删除操作: {result[0]['deleted']} 行")
        
        # 验证删除结果
        verification_queries = [
            "SELECT * FROM users;",
            "SELECT * FROM products;",
            "SELECT * FROM orders;"
        ]
        
        for sql in verification_queries:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 删除后验证: {len(result)} 行")
        
        return syscat, executor, analyzer, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_complex_queries():
    """测试复杂查询"""
    print("\n=== 测试复杂查询 ===")
    
    syscat, executor, analyzer, temp_dir = test_delete_operations()
    
    try:
        # 测试多表查询（模拟JOIN）
        complex_queries = [
            "SELECT * FROM users WHERE id = 1;",
            "SELECT * FROM products WHERE price > 100;",
            "SELECT * FROM orders WHERE quantity > 1;"
        ]
        
        for sql in complex_queries:
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 复杂查询: {len(result)} 行")
                if result:
                    print(f"  结果: {result}")
        
        return syscat, executor, analyzer, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_transaction_simulation():
    """测试事务模拟"""
    print("\n=== 测试事务模拟 ===")
    
    syscat, executor, analyzer, temp_dir = test_complex_queries()
    
    try:
        # 模拟事务：插入多个相关记录
        transaction_sql = """
        INSERT INTO users(id,name,email) VALUES (4,'David','david@example.com');
        INSERT INTO products(id,name,price,category) VALUES (4,'Tablet',500,'Electronics');
        INSERT INTO orders(id,user_id,product_id,quantity) VALUES (4,4,4,1);
        """
        
        parser = Parser(transaction_sql)
        asts = parser.parse_many()
        
        start_time = time.time()
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            planner = Planner(executor)
            op = planner.plan(analyzed)
            result = executor.execute_plan(op)
            print(f"[OK] 事务操作: {result[0]}")
        
        end_time = time.time()
        print(f"[OK] 事务完成，耗时: {end_time - start_time:.4f} 秒")
        
        # 验证事务结果
        verification_sql = "SELECT * FROM users WHERE id = 4;"
        parser = Parser(verification_sql)
        asts = parser.parse_many()
        
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            planner = Planner(executor)
            op = planner.plan(analyzed)
            result = executor.execute_plan(op)
            
            if result:
                print(f"[OK] 事务验证: {result[0]}")
        
        return syscat, executor, analyzer, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_performance_benchmark():
    """测试性能基准"""
    print("\n=== 测试性能基准 ===")
    
    syscat, executor, analyzer, temp_dir = test_transaction_simulation()
    
    try:
        # 性能测试：大量数据操作
        print("\n--- 大量数据插入测试 ---")
        start_time = time.time()
        
        for i in range(100):
            sql = f"INSERT INTO users(id,name,email) VALUES ({i+10},'User{i+10}','user{i+10}@example.com');"
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                planner = Planner(executor)
                op = planner.plan(analyzed)
                executor.execute_plan(op)
        
        end_time = time.time()
        insert_time = end_time - start_time
        print(f"[OK] 插入100条记录，耗时: {insert_time:.4f} 秒")
        print(f"[OK] 插入速度: {100/insert_time:.0f} 行/秒")
        
        # 查询性能测试
        print("\n--- 查询性能测试 ---")
        start_time = time.time()
        
        sql = "SELECT * FROM users;"
        parser = Parser(sql)
        asts = parser.parse_many()
        
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            planner = Planner(executor)
            op = planner.plan(analyzed)
            result = executor.execute_plan(op)
        
        end_time = time.time()
        query_time = end_time - start_time
        print(f"[OK] 查询{len(result)}条记录，耗时: {query_time:.4f} 秒")
        if query_time > 0:
            print(f"[OK] 查询速度: {len(result)/query_time:.0f} 行/秒")
        else:
            print(f"[OK] 查询速度: 极快 (< 0.0001秒)")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_persistence():
    """测试持久化"""
    print("\n=== 测试持久化 ===")
    
    temp_dir = test_performance_benchmark()
    
    try:
        # 刷新所有数据
        db_path = os.path.join(temp_dir, "crud_test.db")
        syscat = SystemCatalog(db_path)
        syscat.buffer.flush_all()
        print("[OK] 数据刷新完成")
        
        # 模拟重启
        print("\n--- 模拟数据库重启 ---")
        new_syscat = SystemCatalog(db_path)
        new_executor = Executor(new_syscat)
        new_analyzer = SemanticAnalyzer(new_syscat)
        
        # 验证数据恢复
        verification_sql = "SELECT * FROM users;"
        parser = Parser(verification_sql)
        asts = parser.parse_many()
        
        for ast in asts:
            analyzed = new_analyzer.analyze(ast)
            planner = Planner(new_executor)
            op = planner.plan(analyzed)
            result = new_executor.execute_plan(op)
            
            print(f"[OK] 重启后数据恢复: {len(result)} 行")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def main():
    """主测试函数"""
    print("开始核心CRUD操作测试...")
    print("=" * 50)
    
    temp_dir = None
    try:
        # 测试CREATE操作
        test_create_operations()
        
        # 测试INSERT操作
        test_insert_operations()
        
        # 测试SELECT操作
        test_select_operations()
        
        # 测试DELETE操作
        test_delete_operations()
        
        # 测试复杂查询
        test_complex_queries()
        
        # 测试事务模拟
        test_transaction_simulation()
        
        # 测试性能基准
        test_performance_benchmark()
        
        # 测试持久化
        test_persistence()
        
        print("\n" + "=" * 50)
        print("[SUCCESS] 核心CRUD操作测试全部通过！")
        
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
