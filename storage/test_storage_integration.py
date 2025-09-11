#!/usr/bin/env python3
"""
存储系统集成测试脚本
测试页式存储、文件I/O、缓冲管理的综合功能
"""

import sys
import os
import tempfile
import shutil
import time
sys.path.insert(0, os.getcwd())

from storage.disk_manager import DiskManager
from storage.buffer_manager import BufferManager
from storage.table import Table
from storage.page import Page, PAGE_SIZE

def test_storage_integration():
    """测试存储系统集成功能"""
    print("=== 测试存储系统集成功能 ===")
    
    # 创建临时文件
    temp_dir = tempfile.mkdtemp(prefix="mini_db_integration_test_")
    db_path = os.path.join(temp_dir, "integration_test.db")
    
    try:
        # 创建存储组件
        disk = DiskManager(db_path)
        buffer = BufferManager(disk, capacity=5)  # 中等容量
        table = Table(buffer, "test_table")
        
        print(f"[OK] 创建存储组件")
        print(f"  - 磁盘管理器: {db_path}")
        print(f"  - 缓冲管理器: 容量 {buffer.capacity}")
        print(f"  - 表: {table.name}")
        
        return disk, buffer, table, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_table_operations():
    """测试表操作"""
    print("\n=== 测试表操作 ===")
    
    disk, buffer, table, temp_dir = test_storage_integration()
    
    try:
        # 插入大量数据
        print("\n--- 插入大量数据 ---")
        start_time = time.time()
        
        row_count = 100
        for i in range(row_count):
            row = {
                "id": i,
                "name": f"User_{i}",
                "age": 20 + (i % 50),
                "city": f"City_{i % 10}",
                "score": 60 + (i % 40)
            }
            table.insert(row)
            
            if (i + 1) % 20 == 0:
                print(f"[OK] 已插入 {i + 1} 行")
        
        end_time = time.time()
        insert_time = end_time - start_time
        print(f"[OK] 插入完成: {row_count} 行，耗时 {insert_time:.4f} 秒")
        
        # 获取缓冲统计
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 插入后缓冲统计 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        return disk, buffer, table, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_table_scan():
    """测试表扫描"""
    print("\n=== 测试表扫描 ===")
    
    disk, buffer, table, temp_dir = test_table_operations()
    
    try:
        # 全表扫描
        print("\n--- 全表扫描 ---")
        start_time = time.time()
        
        scanned_rows = list(table.scan())
        scan_count = len(scanned_rows)
        
        end_time = time.time()
        scan_time = end_time - start_time
        
        print(f"[OK] 扫描完成: {scan_count} 行，耗时 {scan_time:.4f} 秒")
        print(f"[OK] 扫描速度: {scan_count/scan_time:.0f} 行/秒")
        
        # 获取缓冲统计
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 扫描后缓冲统计 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        # 验证数据完整性
        print("\n--- 验证数据完整性 ---")
        expected_ids = set(range(100))
        actual_ids = set(row["id"] for row in scanned_rows)
        
        if expected_ids == actual_ids:
            print("[OK] 数据完整性验证通过")
        else:
            missing = expected_ids - actual_ids
            extra = actual_ids - expected_ids
            print(f"[ERROR] 数据完整性验证失败")
            if missing:
                print(f"  缺失ID: {sorted(missing)}")
            if extra:
                print(f"  多余ID: {sorted(extra)}")
        
        return disk, buffer, table, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_table_delete():
    """测试表删除"""
    print("\n=== 测试表删除 ===")
    
    disk, buffer, table, temp_dir = test_table_scan()
    
    try:
        # 删除部分数据
        print("\n--- 删除部分数据 ---")
        start_time = time.time()
        
        # 删除年龄大于40的记录
        deleted_count = table.delete(lambda row: row.get("age", 0) > 40)
        
        end_time = time.time()
        delete_time = end_time - start_time
        
        print(f"[OK] 删除完成: {deleted_count} 行，耗时 {delete_time:.4f} 秒")
        
        # 验证删除结果
        print("\n--- 验证删除结果 ---")
        remaining_rows = list(table.scan())
        remaining_count = len(remaining_rows)
        
        print(f"[OK] 剩余行数: {remaining_count}")
        
        # 检查是否还有年龄大于40的记录
        old_records = [row for row in remaining_rows if row.get("age", 0) > 40]
        if len(old_records) == 0:
            print("[OK] 删除验证通过")
        else:
            print(f"[ERROR] 删除验证失败，仍有 {len(old_records)} 条年龄大于40的记录")
        
        return disk, buffer, table, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_persistence():
    """测试持久化"""
    print("\n=== 测试持久化 ===")
    
    disk, buffer, table, temp_dir = test_table_delete()
    
    try:
        # 刷新所有数据
        print("\n--- 刷新所有数据 ---")
        buffer.flush_all()
        print("[OK] 数据刷新完成")
        
        # 获取文件信息
        file_size = os.path.getsize(disk.file_path)
        page_count = disk.num_pages()
        expected_size = page_count * PAGE_SIZE
        
        print(f"[OK] 文件信息:")
        print(f"  - 文件大小: {file_size} 字节")
        print(f"  - 页数: {page_count}")
        print(f"  - 期望大小: {expected_size} 字节")
        print(f"  - 大小检查: {'通过' if file_size == expected_size else '失败'}")
        
        # 模拟重启
        print("\n--- 模拟数据库重启 ---")
        new_disk = DiskManager(disk.file_path)
        new_buffer = BufferManager(new_disk, capacity=5)
        new_table = Table(new_buffer, "test_table")
        
        # 验证数据恢复
        restored_rows = list(new_table.scan())
        restored_count = len(restored_rows)
        
        print(f"[OK] 重启后数据恢复: {restored_count} 行")
        
        # 验证数据一致性
        if restored_count == len(list(table.scan())):
            print("[OK] 数据一致性验证通过")
        else:
            print("[ERROR] 数据一致性验证失败")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_performance_benchmark():
    """测试性能基准"""
    print("\n=== 测试性能基准 ===")
    
    temp_dir = test_persistence()
    
    try:
        db_path = os.path.join(temp_dir, "perf_benchmark.db")
        disk = DiskManager(db_path)
        buffer = BufferManager(disk, capacity=10)
        table = Table(buffer, "perf_table")
        
        # 性能测试参数
        test_sizes = [100, 500, 1000]
        
        for size in test_sizes:
            print(f"\n--- 性能测试: {size} 行 ---")
            
            # 插入性能测试
            start_time = time.time()
            for i in range(size):
                row = {
                    "id": i,
                    "name": f"User_{i}",
                    "age": 20 + (i % 50),
                    "data": f"data_{i}" * 10  # 增加数据大小
                }
                table.insert(row)
            end_time = time.time()
            insert_time = end_time - start_time
            
            # 扫描性能测试
            start_time = time.time()
            scanned_rows = list(table.scan())
            end_time = time.time()
            scan_time = end_time - start_time
            
            # 缓冲统计
            hits, misses, evictions = buffer.stats()
            hit_rate = hits / (hits + misses) * 100 if (hits + misses) > 0 else 0
            
            print(f"[OK] 插入性能: {size/insert_time:.0f} 行/秒")
            print(f"[OK] 扫描性能: {len(scanned_rows)/scan_time:.0f} 行/秒")
            print(f"[OK] 缓冲命中率: {hit_rate:.1f}%")
            print(f"[OK] 缓冲统计: Hits={hits}, Misses={misses}, Evictions={evictions}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_stress_test():
    """测试压力测试"""
    print("\n=== 测试压力测试 ===")
    
    temp_dir = test_performance_benchmark()
    
    try:
        db_path = os.path.join(temp_dir, "stress_test.db")
        disk = DiskManager(db_path)
        buffer = BufferManager(disk, capacity=20)  # 较大容量
        table = Table(buffer, "stress_table")
        
        # 压力测试：大量数据操作
        print("\n--- 压力测试: 大量数据操作 ---")
        
        # 插入大量数据
        large_size = 2000
        start_time = time.time()
        
        for i in range(large_size):
            row = {
                "id": i,
                "name": f"User_{i}",
                "age": 20 + (i % 50),
                "city": f"City_{i % 20}",
                "score": 60 + (i % 40),
                "data": f"data_{i}" * 5
            }
            table.insert(row)
            
            if (i + 1) % 500 == 0:
                print(f"[OK] 已插入 {i + 1} 行")
        
        end_time = time.time()
        insert_time = end_time - start_time
        
        print(f"[OK] 压力测试插入完成: {large_size} 行，耗时 {insert_time:.4f} 秒")
        print(f"[OK] 插入速度: {large_size/insert_time:.0f} 行/秒")
        
        # 多次扫描测试
        print("\n--- 多次扫描测试 ---")
        scan_times = []
        for i in range(5):
            start_time = time.time()
            scanned_rows = list(table.scan())
            end_time = time.time()
            scan_time = end_time - start_time
            scan_times.append(scan_time)
            print(f"[OK] 扫描 {i+1}: {len(scanned_rows)} 行，耗时 {scan_time:.4f} 秒")
        
        avg_scan_time = sum(scan_times) / len(scan_times)
        print(f"[OK] 平均扫描时间: {avg_scan_time:.4f} 秒")
        
        # 最终缓冲统计
        hits, misses, evictions = buffer.stats()
        hit_rate = hits / (hits + misses) * 100 if (hits + misses) > 0 else 0
        
        print(f"[OK] 最终缓冲统计:")
        print(f"  - 命中率: {hit_rate:.1f}%")
        print(f"  - 命中次数: {hits}")
        print(f"  - 未命中次数: {misses}")
        print(f"  - 逐出次数: {evictions}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def main():
    """主测试函数"""
    print("开始存储系统集成测试...")
    print("=" * 50)
    
    temp_dir = None
    try:
        # 集成功能测试
        test_storage_integration()
        
        # 表操作测试
        test_table_operations()
        
        # 表扫描测试
        test_table_scan()
        
        # 表删除测试
        test_table_delete()
        
        # 持久化测试
        test_persistence()
        
        # 性能基准测试
        test_performance_benchmark()
        
        # 压力测试
        test_stress_test()
        
        print("\n" + "=" * 50)
        print("[OK] 存储系统集成测试全部通过！")
        
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
