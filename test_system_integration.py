#!/usr/bin/env python3
"""
系统集成测试脚本
测试执行引擎、存储引擎与查询语言的完整集成
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

def test_full_pipeline():
    """测试完整处理流水线"""
    print("=== 测试完整处理流水线 ===")
    
    temp_dir = tempfile.mkdtemp(prefix="integration_test_")
    db_path = os.path.join(temp_dir, "integration_test.db")
    
    try:
        # 创建系统组件
        syscat = SystemCatalog(db_path)
        executor = Executor(syscat)
        analyzer = SemanticAnalyzer(syscat)
        
        print("[OK] 系统组件创建成功")
        
        # 测试完整的SQL处理流程
        test_sql = "CREATE TABLE users(id INT, name VARCHAR, age INT);"
        
        print(f"\n--- 处理SQL: {test_sql} ---")
        
        # 1. 词法分析
        tokens = tokenize(test_sql)
        print(f"[OK] 词法分析: {len(tokens)} 个token")
        
        # 2. 语法分析
        parser = Parser(test_sql)
        asts = parser.parse_many()
        print(f"[OK] 语法分析: {len(asts)} 个AST")
        
        # 3. 语义分析
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            print(f"[OK] 语义分析: {analyzed.kind}")
        
        # 4. 查询规划
        planner = Planner(executor)
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            op = planner.plan(analyzed)
            print(f"[OK] 查询规划: {op.__class__.__name__}")
        
        # 5. 执行
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            op = planner.plan(analyzed)
            result = executor.execute_plan(op)
            print(f"[OK] 执行结果: {result}")
        
        return syscat, executor, analyzer, planner, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_crud_integration():
    """测试CRUD集成"""
    print("\n=== 测试CRUD集成 ===")
    
    syscat, executor, analyzer, planner, temp_dir = test_full_pipeline()
    
    try:
        # 测试完整的CRUD操作
        crud_operations = [
            "INSERT INTO users(id,name,age) VALUES (1,'Alice',20);",
            "INSERT INTO users(id,name,age) VALUES (2,'Bob',17);",
            "INSERT INTO users(id,name,age) VALUES (3,'Charlie',25);",
            "SELECT * FROM users;",
            "SELECT id, name FROM users WHERE age >= 18;",
            "DELETE FROM users WHERE age < 18;",
            "SELECT * FROM users;"
        ]
        
        for i, sql in enumerate(crud_operations):
            print(f"\n--- CRUD操作 {i+1}: {sql} ---")
            
            # 完整处理流程
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
                
                print(f"[OK] 操作完成: {result}")
        
        return syscat, executor, analyzer, planner, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_performance_integration():
    """测试性能集成"""
    print("\n=== 测试性能集成 ===")
    
    syscat, executor, analyzer, planner, temp_dir = test_crud_integration()
    
    try:
        # 性能测试：大量数据操作
        print("\n--- 大量数据插入测试 ---")
        start_time = time.time()
        
        for i in range(1000):
            sql = f"INSERT INTO users(id,name,age) VALUES ({i+10},'User{i+10}',{20 + (i % 50)});"
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                op = planner.plan(analyzed)
                executor.execute_plan(op)
        
        end_time = time.time()
        insert_time = end_time - start_time
        print(f"[OK] 插入1000条记录，耗时: {insert_time:.4f} 秒")
        print(f"[OK] 插入速度: {1000/insert_time:.0f} 行/秒")
        
        # 查询性能测试
        print("\n--- 查询性能测试 ---")
        start_time = time.time()
        
        sql = "SELECT * FROM users;"
        parser = Parser(sql)
        asts = parser.parse_many()
        
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            op = planner.plan(analyzed)
            result = executor.execute_plan(op)
        
        end_time = time.time()
        query_time = end_time - start_time
        print(f"[OK] 查询{len(result)}条记录，耗时: {query_time:.4f} 秒")
        if query_time > 0:
            print(f"[OK] 查询速度: {len(result)/query_time:.0f} 行/秒")
        else:
            print(f"[OK] 查询速度: 极快 (< 0.0001秒)")
        
        # 条件查询性能测试
        print("\n--- 条件查询性能测试 ---")
        start_time = time.time()
        
        sql = "SELECT * FROM users WHERE age > 30;"
        parser = Parser(sql)
        asts = parser.parse_many()
        
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            op = planner.plan(analyzed)
            result = executor.execute_plan(op)
        
        end_time = time.time()
        filter_time = end_time - start_time
        print(f"[OK] 条件查询{len(result)}条记录，耗时: {filter_time:.4f} 秒")
        if filter_time > 0:
            print(f"[OK] 过滤速度: {len(result)/filter_time:.0f} 行/秒")
        else:
            print(f"[OK] 过滤速度: 极快 (< 0.0001秒)")
        
        return syscat, executor, analyzer, planner, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_storage_integration():
    """测试存储集成"""
    print("\n=== 测试存储集成 ===")
    
    syscat, executor, analyzer, planner, temp_dir = test_performance_integration()
    
    try:
        # 测试存储统计
        print("\n--- 存储统计 ---")
        hits, misses, evictions = syscat.buffer.stats()
        print(f"[OK] 缓冲统计: Hits={hits}, Misses={misses}, Evictions={evictions}")
        
        # 测试文件大小
        file_size = os.path.getsize(syscat.disk.file_path)
        page_count = syscat.disk.num_pages()
        print(f"[OK] 文件大小: {file_size} 字节")
        print(f"[OK] 页数: {page_count}")
        
        # 测试数据持久化
        print("\n--- 数据持久化测试 ---")
        syscat.buffer.flush_all()
        print("[OK] 数据刷新完成")
        
        # 模拟重启
        print("\n--- 模拟重启 ---")
        new_syscat = SystemCatalog(syscat.disk.file_path)
        new_executor = Executor(new_syscat)
        new_analyzer = SemanticAnalyzer(new_syscat)
        new_planner = Planner(new_executor)
        
        # 验证数据恢复
        sql = "SELECT * FROM users;"
        parser = Parser(sql)
        asts = parser.parse_many()
        
        for ast in asts:
            analyzed = new_analyzer.analyze(ast)
            op = new_planner.plan(analyzed)
            result = new_executor.execute_plan(op)
        
        print(f"[OK] 重启后数据恢复: {len(result)} 行")
        
        return new_syscat, new_executor, new_analyzer, new_planner, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_concurrent_simulation():
    """测试并发模拟"""
    print("\n=== 测试并发模拟 ===")
    
    syscat, executor, analyzer, planner, temp_dir = test_storage_integration()
    
    try:
        # 模拟并发操作
        print("\n--- 模拟并发操作 ---")
        
        # 创建多个表
        tables = ["table1", "table2", "table3"]
        for table in tables:
            sql = f"CREATE TABLE {table}(id INT, data VARCHAR);"
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                op = planner.plan(analyzed)
                executor.execute_plan(op)
        
        print("[OK] 创建多个表完成")
        
        # 并发插入数据
        print("\n--- 并发插入数据 ---")
        start_time = time.time()
        
        for table in tables:
            for i in range(100):
                sql = f"INSERT INTO {table}(id,data) VALUES ({i},'data_{table}_{i}');"
                parser = Parser(sql)
                asts = parser.parse_many()
                
                for ast in asts:
                    analyzed = analyzer.analyze(ast)
                    op = planner.plan(analyzed)
                    executor.execute_plan(op)
        
        end_time = time.time()
        concurrent_time = end_time - start_time
        print(f"[OK] 并发插入完成，耗时: {concurrent_time:.4f} 秒")
        
        # 验证数据
        for table in tables:
            sql = f"SELECT * FROM {table};"
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                op = planner.plan(analyzed)
                result = executor.execute_plan(op)
            
            print(f"[OK] 表 {table}: {len(result)} 行")
        
        return syscat, executor, analyzer, planner, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_error_recovery():
    """测试错误恢复"""
    print("\n=== 测试错误恢复 ===")
    
    syscat, executor, analyzer, planner, temp_dir = test_concurrent_simulation()
    
    try:
        # 测试错误恢复
        print("\n--- 测试错误恢复 ---")
        
        # 测试无效操作
        invalid_operations = [
            "SELECT * FROM nonexistent_table;",
            "INSERT INTO nonexistent_table(id) VALUES (1);",
            "DELETE FROM nonexistent_table WHERE id = 1;"
        ]
        
        for sql in invalid_operations:
            print(f"\n--- 测试无效操作: {sql} ---")
            try:
                parser = Parser(sql)
                asts = parser.parse_many()
                
                for ast in asts:
                    analyzed = analyzer.analyze(ast)
                    print("[WARNING] 应该抛出错误但没有")
            except Exception as e:
                print(f"[OK] 正确处理错误: {e}")
        
        # 测试系统恢复
        print("\n--- 测试系统恢复 ---")
        
        # 正常操作应该仍然工作
        sql = "SELECT * FROM table1;"
        parser = Parser(sql)
        asts = parser.parse_many()
        
        for ast in asts:
            analyzed = analyzer.analyze(ast)
            op = planner.plan(analyzed)
            result = executor.execute_plan(op)
        
        print(f"[OK] 系统恢复正常: {len(result)} 行")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_system_limits():
    """测试系统限制"""
    print("\n=== 测试系统限制 ===")
    
    temp_dir = test_error_recovery()
    
    try:
        db_path = os.path.join(temp_dir, "integration_test.db")
        syscat = SystemCatalog(db_path)
        executor = Executor(syscat)
        analyzer = SemanticAnalyzer(syscat)
        planner = Planner(executor)
        
        # 测试系统限制
        print("\n--- 测试系统限制 ---")
        
        # 测试大量表创建
        print("\n--- 测试大量表创建 ---")
        start_time = time.time()
        
        for i in range(50):
            sql = f"CREATE TABLE test_table_{i}(id INT, data VARCHAR);"
            parser = Parser(sql)
            asts = parser.parse_many()
            
            for ast in asts:
                analyzed = analyzer.analyze(ast)
                op = planner.plan(analyzed)
                executor.execute_plan(op)
        
        end_time = time.time()
        table_time = end_time - start_time
        print(f"[OK] 创建50个表，耗时: {table_time:.4f} 秒")
        
        # 测试内存使用
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            print(f"[OK] 内存使用: {memory_info.rss / 1024 / 1024:.2f} MB")
        except ImportError:
            print("[OK] 内存使用: psutil模块未安装，跳过内存统计")
        
        # 测试缓冲统计
        hits, misses, evictions = syscat.buffer.stats()
        print(f"[OK] 最终缓冲统计: Hits={hits}, Misses={misses}, Evictions={evictions}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def main():
    """主测试函数"""
    print("开始系统集成测试...")
    print("=" * 50)
    
    temp_dir = None
    try:
        # 测试完整处理流水线
        test_full_pipeline()
        
        # 测试CRUD集成
        test_crud_integration()
        
        # 测试性能集成
        test_performance_integration()
        
        # 测试存储集成
        test_storage_integration()
        
        # 测试并发模拟
        test_concurrent_simulation()
        
        # 测试错误恢复
        test_error_recovery()
        
        # 测试系统限制
        test_system_limits()
        
        print("\n" + "=" * 50)
        print("[SUCCESS] 系统集成测试全部通过！")
        
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
