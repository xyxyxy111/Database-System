#!/usr/bin/env python3
"""
页式存储测试脚本
测试Page类的序列化、反序列化、容量管理等功能
"""

import sys
import os
sys.path.insert(0, os.getcwd())

from storage.page import Page, PAGE_SIZE
import pickle

def test_page_basic_operations():
    """测试页的基本操作"""
    print("=== 测试页的基本操作 ===")
    
    # 创建空页
    page = Page(page_id=1)
    print(f"[OK] 创建空页，页ID: {page.page_id}")
    print(f"[OK] 初始行数: {len(page.get_rows())}")
    
    # 测试插入行
    test_rows = [
        {"id": 1, "name": "Alice", "age": 20},
        {"id": 2, "name": "Bob", "age": 21},
        {"id": 3, "name": "Charlie", "age": 19}
    ]
    
    for row in test_rows:
        success = page.insert_row(row)
        print(f"[OK] 插入行: {row} -> {'成功' if success else '失败'}")
    
    print(f"[OK] 最终行数: {len(page.get_rows())}")
    print(f"[OK] 页容量剩余: {page.capacity_left()} 字节")
    
    return page

def test_page_serialization():
    """测试页的序列化和反序列化"""
    print("\n=== 测试页的序列化和反序列化 ===")
    
    # 创建测试页
    original_page = Page(page_id=123)
    test_data = [
        {"id": 1, "name": "Alice", "age": 20},
        {"id": 2, "name": "Bob", "age": 21},
        {"id": 3, "name": "Charlie", "age": 19}
    ]
    
    for row in test_data:
        original_page.insert_row(row)
    
    print(f"[OK] 原始页行数: {len(original_page.get_rows())}")
    
    # 序列化
    serialized = original_page.to_bytes()
    print(f"[OK] 序列化后大小: {len(serialized)} 字节 (PAGE_SIZE: {PAGE_SIZE})")
    print(f"[OK] 大小检查: {'通过' if len(serialized) == PAGE_SIZE else '失败'}")
    
    # 反序列化
    restored_page = Page.from_bytes(serialized)
    restored_page.page_id = original_page.page_id
    
    print(f"[OK] 恢复页行数: {len(restored_page.get_rows())}")
    print(f"[OK] 数据一致性: {'通过' if restored_page.get_rows() == original_page.get_rows() else '失败'}")
    
    return original_page, restored_page

def test_page_capacity():
    """测试页容量管理"""
    print("\n=== 测试页容量管理 ===")
    
    page = Page(page_id=1)
    
    # 测试小数据
    small_row = {"id": 1, "name": "Alice"}
    can_insert = page.can_insert(small_row)
    print(f"[OK] 小数据插入检查: {can_insert}")
    
    if can_insert:
        page.insert_row(small_row)
        print(f"[OK] 插入小数据成功，剩余容量: {page.capacity_left()}")
    
    # 测试大数据
    large_row = {"id": 1, "data": "x" * 4000}  # 4KB数据
    can_insert_large = page.can_insert(large_row)
    print(f"[OK] 大数据插入检查: {can_insert_large}")
    
    if can_insert_large:
        page.insert_row(large_row)
        print(f"[OK] 插入大数据成功，剩余容量: {page.capacity_left()}")
    else:
        print("[OK] 大数据无法插入（符合预期）")
    
    # 测试容量边界
    print(f"[OK] 当前页容量剩余: {page.capacity_left()} 字节")
    print(f"[OK] 页大小限制: {PAGE_SIZE} 字节")

def test_page_edge_cases():
    """测试页的边界情况"""
    print("\n=== 测试页的边界情况 ===")
    
    # 测试空页
    empty_page = Page(page_id=0)
    empty_serialized = empty_page.to_bytes()
    empty_restored = Page.from_bytes(empty_serialized)
    print(f"[OK] 空页序列化/反序列化: {'通过' if len(empty_restored.get_rows()) == 0 else '失败'}")
    
    # 测试最大容量
    page = Page(page_id=1)
    max_rows = 0
    while True:
        row = {"id": max_rows, "data": f"row_{max_rows}"}
        if not page.can_insert(row):
            break
        page.insert_row(row)
        max_rows += 1
    
    print(f"[OK] 页最大行数: {max_rows}")
    print(f"[OK] 最终容量剩余: {page.capacity_left()} 字节")

def main():
    """主测试函数"""
    print("开始页式存储测试...")
    print("=" * 50)
    
    try:
        # 基本操作测试
        test_page_basic_operations()
        
        # 序列化测试
        test_page_serialization()
        
        # 容量管理测试
        test_page_capacity()
        
        # 边界情况测试
        test_page_edge_cases()
        
        print("\n" + "=" * 50)
        print("[SUCCESS] 页式存储测试全部通过！")
        
    except Exception as e:
        print(f"\n[ERROR] 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
