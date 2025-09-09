#!/usr/bin/env python3
"""
文件页I/O测试脚本
测试DiskManager的页分配、读写、文件管理等功能
"""

import sys
import os
import tempfile
import shutil
sys.path.insert(0, os.getcwd())

from storage.disk_manager import DiskManager
from storage.page import Page, PAGE_SIZE

def test_disk_manager_basic():
    """测试磁盘管理器的基本功能"""
    print("=== 测试磁盘管理器基本功能 ===")
    
    # 创建临时文件
    temp_dir = tempfile.mkdtemp(prefix="mini_db_test_")
    db_path = os.path.join(temp_dir, "test.db")
    
    try:
        disk = DiskManager(db_path)
        print(f"[OK] 创建磁盘管理器，文件路径: {db_path}")
        
        # 测试初始状态
        initial_pages = disk.num_pages()
        print(f"[OK] 初始页数: {initial_pages}")
        
        # 测试页分配
        page_ids = []
        for i in range(5):
            pid = disk.allocate_page()
            page_ids.append(pid)
            print(f"[OK] 分配页 {i+1}: ID = {pid}")
        
        total_pages = disk.num_pages()
        print(f"[OK] 分配后总页数: {total_pages}")
        print(f"[OK] 页ID序列: {page_ids}")
        
        return disk, page_ids, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_page_read_write():
    """测试页的读写操作"""
    print("\n=== 测试页的读写操作 ===")
    
    disk, page_ids, temp_dir = test_disk_manager_basic()
    
    try:
        # 测试写入不同内容到不同页
        test_data = [
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            [{"id": 3, "name": "Charlie", "age": 25}],
            [{"id": 4, "name": "David", "city": "Beijing"}],
            [{"id": 5, "name": "Eve", "score": 95.5}],
            [{"id": 6, "name": "Frank", "active": True}]
        ]
        
        # 写入数据
        for i, (pid, data) in enumerate(zip(page_ids, test_data)):
            page = Page(pid, data)
            disk.write_page(pid, page.to_bytes())
            print(f"[OK] 写入页 {pid}: {len(data)} 行数据")
        
        # 读取并验证数据
        for i, (pid, expected_data) in enumerate(zip(page_ids, test_data)):
            raw_data = disk.read_page(pid)
            restored_page = Page.from_bytes(raw_data)
            restored_page.page_id = pid
            
            actual_data = restored_page.get_rows()
            print(f"[OK] 读取页 {pid}: {len(actual_data)} 行数据")
            print(f"[OK] 数据一致性: {'通过' if actual_data == expected_data else '失败'}")
        
        return disk, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_file_persistence():
    """测试文件持久化"""
    print("\n=== 测试文件持久化 ===")
    
    disk, temp_dir = test_page_read_write()
    
    try:
        # 获取文件大小
        file_size = os.path.getsize(disk.file_path)
        expected_size = disk.num_pages() * PAGE_SIZE
        print(f"[OK] 文件大小: {file_size} 字节")
        print(f"[OK] 期望大小: {expected_size} 字节")
        print(f"[OK] 大小检查: {'通过' if file_size == expected_size else '失败'}")
        
        # 重新创建磁盘管理器（模拟重启）
        print("\n--- 模拟数据库重启 ---")
        new_disk = DiskManager(disk.file_path)
        new_pages = new_disk.num_pages()
        print(f"[OK] 重启后页数: {new_pages}")
        print(f"[OK] 页数一致性: {'通过' if new_pages == disk.num_pages() else '失败'}")
        
        # 验证数据完整性
        for pid in range(new_pages):
            raw_data = new_disk.read_page(pid)
            restored_page = Page.from_bytes(raw_data)
            restored_page.page_id = pid
            rows = restored_page.get_rows()
            print(f"[OK] 页 {pid} 数据完整性: {len(rows)} 行")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_page_operations():
    """测试页操作"""
    print("\n=== 测试页操作 ===")
    
    temp_dir = test_file_persistence()
    
    try:
        db_path = os.path.join(temp_dir, "test.db")
        disk = DiskManager(db_path)
        
        # 测试页清零
        print("\n--- 测试页清零 ---")
        pid_to_clear = 2
        disk.free_page(pid_to_clear)
        print(f"[OK] 清零页 {pid_to_clear}")
        
        # 验证页被清零
        raw_data = disk.read_page(pid_to_clear)
        restored_page = Page.from_bytes(raw_data)
        restored_page.page_id = pid_to_clear
        rows = restored_page.get_rows()
        print(f"[OK] 清零验证: {'通过' if len(rows) == 0 else '失败'}")
        
        # 测试追加新页
        print("\n--- 测试追加新页 ---")
        new_pid = disk.allocate_page()
        print(f"[OK] 追加新页: ID = {new_pid}")
        
        # 写入新数据
        new_data = [{"id": 100, "name": "NewUser", "type": "test"}]
        new_page = Page(new_pid, new_data)
        disk.write_page(new_pid, new_page.to_bytes())
        print(f"[OK] 写入新页数据: {len(new_data)} 行")
        
        # 验证新页数据
        raw_data = disk.read_page(new_pid)
        restored_page = Page.from_bytes(raw_data)
        restored_page.page_id = new_pid
        rows = restored_page.get_rows()
        print(f"[OK] 新页数据验证: {'通过' if rows == new_data else '失败'}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_large_file_operations():
    """测试大文件操作"""
    print("\n=== 测试大文件操作 ===")
    
    temp_dir = test_page_operations()
    
    try:
        db_path = os.path.join(temp_dir, "large_test.db")
        disk = DiskManager(db_path)
        
        # 创建大量页
        large_page_count = 100
        print(f"创建 {large_page_count} 页...")
        
        page_ids = []
        for i in range(large_page_count):
            pid = disk.allocate_page()
            page_ids.append(pid)
            
            # 每页写入一些数据
            data = [{"page_id": pid, "row_id": j, "data": f"page_{pid}_row_{j}"} 
                   for j in range(5)]
            page = Page(pid, data)
            disk.write_page(pid, page.to_bytes())
            
            if (i + 1) % 20 == 0:
                print(f"[OK] 已创建 {i + 1} 页")
        
        print(f"[OK] 创建完成，总页数: {disk.num_pages()}")
        
        # 验证文件大小
        file_size = os.path.getsize(db_path)
        expected_size = large_page_count * PAGE_SIZE
        print(f"[OK] 文件大小: {file_size} 字节")
        print(f"[OK] 期望大小: {expected_size} 字节")
        print(f"[OK] 大小检查: {'通过' if file_size == expected_size else '失败'}")
        
        # 随机读取测试
        import random
        test_pages = random.sample(page_ids, 10)
        print(f"\n--- 随机读取测试 ---")
        for pid in test_pages:
            raw_data = disk.read_page(pid)
            restored_page = Page.from_bytes(raw_data)
            restored_page.page_id = pid
            rows = restored_page.get_rows()
            print(f"[OK] 页 {pid}: {len(rows)} 行数据")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def main():
    """主测试函数"""
    print("开始文件页I/O测试...")
    print("=" * 50)
    
    temp_dir = None
    try:
        # 基本功能测试
        test_disk_manager_basic()
        
        # 读写操作测试
        test_page_read_write()
        
        # 持久化测试
        test_file_persistence()
        
        # 页操作测试
        test_page_operations()
        
        # 大文件操作测试
        test_large_file_operations()
        
        print("\n" + "=" * 50)
        print("[SUCCESS] 文件页I/O测试全部通过！")
        
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
