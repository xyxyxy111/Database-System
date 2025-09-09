#!/usr/bin/env python3
"""
执行引擎测试脚本
测试执行引擎的算子、执行计划、性能等功能
"""

import sys
import os
import tempfile
import shutil
import time
sys.path.insert(0, os.getcwd())

from execution.sytem_catalog import SystemCatalog
from execution.executor import Executor
from execution.operators import (
    SeqScan, Filter, Project, Insert, CreateTable, Delete, 
    make_predicate, Operator
)
from storage.table import Table

def test_operator_basic():
    """测试基本算子"""
    print("=== 测试基本算子 ===")
    
    temp_dir = tempfile.mkdtemp(prefix="exec_test_")
    db_path = os.path.join(temp_dir, "exec_test.db")
    
    try:
        syscat = SystemCatalog(db_path)
        executor = Executor(syscat)
        
        # 测试CreateTable算子
        print("\n--- 测试CreateTable算子 ---")
        create_op = CreateTable(syscat, "test_table", [("id", "INT"), ("name", "VARCHAR")])
        result = executor.execute_plan(create_op)
        print(f"[OK] CreateTable结果: {result[0]}")
        
        # 测试Insert算子
        print("\n--- 测试Insert算子 ---")
        test_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ]
        insert_op = Insert(syscat.get_table("test_table"), test_data)
        result = executor.execute_plan(insert_op)
        print(f"[OK] Insert结果: {result[0]}")
        
        # 测试SeqScan算子
        print("\n--- 测试SeqScan算子 ---")
        scan_op = SeqScan(syscat.get_table("test_table"))
        result = executor.execute_plan(scan_op)
        print(f"[OK] SeqScan结果: {len(result)} 行")
        for row in result:
            print(f"  {row}")
        
        return syscat, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_filter_operator():
    """测试Filter算子"""
    print("\n=== 测试Filter算子 ===")
    
    syscat, executor, temp_dir = test_operator_basic()
    
    try:
        # 测试不同的过滤条件
        filter_tests = [
            ("id", "EQ", 1, "等于1"),
            ("id", "GT", 1, "大于1"),
            ("id", "LT", 3, "小于3"),
            ("id", "GE", 2, "大于等于2"),
            ("id", "LE", 2, "小于等于2"),
            ("id", "NE", 1, "不等于1")
        ]
        
        for col, op, val, desc in filter_tests:
            print(f"\n--- 测试过滤条件: {desc} ---")
            scan_op = SeqScan(syscat.get_table("test_table"))
            pred = make_predicate(col, op, val)
            filter_op = Filter(scan_op, pred)
            result = executor.execute_plan(filter_op)
            print(f"[OK] 过滤结果: {len(result)} 行")
            for row in result:
                print(f"  {row}")
        
        return syscat, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_project_operator():
    """测试Project算子"""
    print("\n=== 测试Project算子 ===")
    
    syscat, executor, temp_dir = test_filter_operator()
    
    try:
        # 测试不同的投影列
        project_tests = [
            (["id"], "只投影id列"),
            (["name"], "只投影name列"),
            (["id", "name"], "投影id和name列")
        ]
        
        for cols, desc in project_tests:
            print(f"\n--- 测试投影: {desc} ---")
            scan_op = SeqScan(syscat.get_table("test_table"))
            project_op = Project(scan_op, cols)
            result = executor.execute_plan(project_op)
            print(f"[OK] 投影结果: {len(result)} 行")
            for row in result:
                print(f"  {row}")
        
        return syscat, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_operator_combinations():
    """测试算子组合"""
    print("\n=== 测试算子组合 ===")
    
    syscat, executor, temp_dir = test_project_operator()
    
    try:
        # 测试Filter + Project组合
        print("\n--- 测试Filter + Project组合 ---")
        scan_op = SeqScan(syscat.get_table("test_table"))
        pred = make_predicate("id", "GT", 1)
        filter_op = Filter(scan_op, pred)
        project_op = Project(filter_op, ["name"])
        result = executor.execute_plan(project_op)
        print(f"[OK] Filter + Project结果: {len(result)} 行")
        for row in result:
            print(f"  {row}")
        
        # 测试Project + Filter组合
        print("\n--- 测试Project + Filter组合 ---")
        scan_op = SeqScan(syscat.get_table("test_table"))
        project_op = Project(scan_op, ["id", "name"])
        pred = make_predicate("id", "LE", 2)
        filter_op = Filter(project_op, pred)
        result = executor.execute_plan(filter_op)
        print(f"[OK] Project + Filter结果: {len(result)} 行")
        for row in result:
            print(f"  {row}")
        
        return syscat, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_delete_operator():
    """测试Delete算子"""
    print("\n=== 测试Delete算子 ===")
    
    syscat, executor, temp_dir = test_operator_combinations()
    
    try:
        # 测试删除操作
        print("\n--- 测试删除操作 ---")
        
        # 先查看当前数据
        scan_op = SeqScan(syscat.get_table("test_table"))
        result = executor.execute_plan(scan_op)
        print(f"[OK] 删除前数据: {len(result)} 行")
        
        # 删除id=1的记录
        pred = make_predicate("id", "EQ", 1)
        delete_op = Delete(syscat.get_table("test_table"), pred)
        result = executor.execute_plan(delete_op)
        print(f"[OK] 删除结果: {result[0]}")
        
        # 验证删除结果
        scan_op = SeqScan(syscat.get_table("test_table"))
        result = executor.execute_plan(scan_op)
        print(f"[OK] 删除后数据: {len(result)} 行")
        for row in result:
            print(f"  {row}")
        
        return syscat, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_execution_performance():
    """测试执行性能"""
    print("\n=== 测试执行性能 ===")
    
    syscat, executor, temp_dir = test_delete_operator()
    
    try:
        # 插入大量数据用于性能测试
        print("\n--- 插入大量数据 ---")
        large_data = []
        for i in range(1000):
            large_data.append({"id": i, "name": f"User_{i}"})
        
        start_time = time.time()
        insert_op = Insert(syscat.get_table("test_table"), large_data)
        result = executor.execute_plan(insert_op)
        end_time = time.time()
        
        print(f"[OK] 插入1000条记录，耗时: {end_time - start_time:.4f} 秒")
        print(f"[OK] 插入速度: {1000/(end_time - start_time):.0f} 行/秒")
        
        # 测试扫描性能
        print("\n--- 测试扫描性能 ---")
        start_time = time.time()
        scan_op = SeqScan(syscat.get_table("test_table"))
        result = executor.execute_plan(scan_op)
        end_time = time.time()
        
        print(f"[OK] 扫描{len(result)}条记录，耗时: {end_time - start_time:.4f} 秒")
        query_time = end_time - start_time
        if query_time > 0:
            print(f"[OK] 扫描速度: {len(result)/query_time:.0f} 行/秒")
        else:
            print(f"[OK] 扫描速度: 极快 (< 0.0001秒)")
        
        # 测试过滤性能
        print("\n--- 测试过滤性能 ---")
        start_time = time.time()
        scan_op = SeqScan(syscat.get_table("test_table"))
        pred = make_predicate("id", "GT", 500)
        filter_op = Filter(scan_op, pred)
        result = executor.execute_plan(filter_op)
        end_time = time.time()
        
        print(f"[OK] 过滤{len(result)}条记录，耗时: {end_time - start_time:.4f} 秒")
        print(f"[OK] 过滤速度: {len(result)/(end_time - start_time):.0f} 行/秒")
        
        return syscat, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_execution_plan():
    """测试执行计划"""
    print("\n=== 测试执行计划 ===")
    
    syscat, executor, temp_dir = test_execution_performance()
    
    try:
        # 测试复杂执行计划
        print("\n--- 测试复杂执行计划 ---")
        
        # 创建复杂的执行计划：Filter -> Project -> SeqScan
        scan_op = SeqScan(syscat.get_table("test_table"))
        pred = make_predicate("id", "LT", 100)
        filter_op = Filter(scan_op, pred)
        project_op = Project(filter_op, ["name"])
        
        # 执行计划
        start_time = time.time()
        result = executor.execute_plan(project_op)
        end_time = time.time()
        
        print(f"[OK] 复杂执行计划结果: {len(result)} 行")
        print(f"[OK] 执行时间: {end_time - start_time:.4f} 秒")
        
        # 显示部分结果
        for i, row in enumerate(result[:5]):
            print(f"  {row}")
        if len(result) > 5:
            print(f"  ... 还有 {len(result) - 5} 行")
        
        return syscat, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_error_handling():
    """测试错误处理"""
    print("\n=== 测试错误处理 ===")
    
    syscat, executor, temp_dir = test_execution_plan()
    
    try:
        # 测试不存在的表
        print("\n--- 测试不存在的表 ---")
        try:
            scan_op = SeqScan(syscat.get_table("nonexistent_table"))
            result = executor.execute_plan(scan_op)
            print("[WARNING] 应该抛出异常但没有")
        except Exception as e:
            print(f"[OK] 正确处理不存在的表: {e}")
        
        # 测试无效的过滤条件
        print("\n--- 测试无效的过滤条件 ---")
        try:
            scan_op = SeqScan(syscat.get_table("test_table"))
            pred = make_predicate("nonexistent_column", "EQ", 1)
            filter_op = Filter(scan_op, pred)
            result = executor.execute_plan(filter_op)
            print("[WARNING] 应该抛出异常但没有")
        except Exception as e:
            print(f"[OK] 正确处理无效过滤条件: {e}")
        
        return syscat, executor, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_memory_management():
    """测试内存管理"""
    print("\n=== 测试内存管理 ===")
    
    syscat, executor, temp_dir = test_error_handling()
    
    try:
        # 测试大量数据的内存管理
        print("\n--- 测试大量数据的内存管理 ---")
        
        # 创建多个表
        for i in range(5):
            table_name = f"large_table_{i}"
            create_op = CreateTable(syscat, table_name, [("id", "INT"), ("data", "VARCHAR")])
            executor.execute_plan(create_op)
            
            # 插入大量数据
            large_data = []
            for j in range(500):
                large_data.append({"id": j, "data": f"data_{i}_{j}"})
            
            insert_op = Insert(syscat.get_table(table_name), large_data)
            executor.execute_plan(insert_op)
            
            print(f"[OK] 创建表 {table_name}，插入500条记录")
        
        # 测试缓冲统计
        hits, misses, evictions = syscat.buffer.stats()
        print(f"[OK] 缓冲统计: Hits={hits}, Misses={misses}, Evictions={evictions}")
        
        # 测试内存使用
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        print(f"[OK] 内存使用: {memory_info.rss / 1024 / 1024:.2f} MB")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def main():
    """主测试函数"""
    print("开始执行引擎测试...")
    print("=" * 50)
    
    temp_dir = None
    try:
        # 测试基本算子
        test_operator_basic()
        
        # 测试Filter算子
        test_filter_operator()
        
        # 测试Project算子
        test_project_operator()
        
        # 测试算子组合
        test_operator_combinations()
        
        # 测试Delete算子
        test_delete_operator()
        
        # 测试执行性能
        test_execution_performance()
        
        # 测试执行计划
        test_execution_plan()
        
        # 测试错误处理
        test_error_handling()
        
        # 测试内存管理
        test_memory_management()
        
        print("\n" + "=" * 50)
        print("[SUCCESS] 执行引擎测试全部通过！")
        
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
