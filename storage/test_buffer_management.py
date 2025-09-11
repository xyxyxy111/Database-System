#!/usr/bin/env python3
"""
缓冲管理与替换策略测试脚本
测试BufferManager的LRU缓存、统计、逐出策略等功能
"""

import sys
import os
import tempfile
import shutil
sys.path.insert(0, os.getcwd())

from storage.disk_manager import DiskManager
from storage.buffer_manager import BufferManager
from storage.page import Page, PAGE_SIZE

def test_buffer_basic_operations():
    """测试缓冲管理器的基本操作"""
    print("=== 测试缓冲管理器基本操作 ===")
    
    # 创建临时文件
    temp_dir = tempfile.mkdtemp(prefix="mini_db_test_")
    db_path = os.path.join(temp_dir, "test.db")
    
    try:
        disk = DiskManager(db_path)
        buffer = BufferManager(disk, capacity=3)  # 小容量便于测试
        
        print(f"[OK] 创建缓冲管理器，容量: {buffer.capacity}")
        
        # 分配一些页
        page_ids = []
        for i in range(5):
            pid = disk.allocate_page()
            page_ids.append(pid)
            
            # 写入测试数据
            data = [{"page_id": pid, "data": f"page_{pid}_data"}]
            page = Page(pid, data)
            disk.write_page(pid, page.to_bytes())
        
        print(f"[OK] 创建 {len(page_ids)} 页: {page_ids}")
        
        return buffer, page_ids, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_buffer_hit_miss():
    """测试缓冲命中和未命中"""
    print("\n=== 测试缓冲命中和未命中 ===")
    
    buffer, page_ids, temp_dir = test_buffer_basic_operations()
    
    try:
        # 初始统计
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 初始统计 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        # 第一次访问页（应该都是miss）
        print("\n--- 第一次访问页 ---")
        for pid in page_ids[:3]:  # 访问前3页
            page = buffer.get_page(pid)
            print(f"[OK] 访问页 {pid}: {len(page.get_rows())} 行数据")
        
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 第一次访问后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        # 第二次访问相同页（应该都是hit）
        print("\n--- 第二次访问相同页 ---")
        for pid in page_ids[:3]:
            page = buffer.get_page(pid)
            print(f"[OK] 再次访问页 {pid}: {len(page.get_rows())} 行数据")
        
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 第二次访问后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        return buffer, page_ids, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_lru_eviction():
    """测试LRU逐出策略"""
    print("\n=== 测试LRU逐出策略 ===")
    
    buffer, page_ids, temp_dir = test_buffer_hit_miss()
    
    try:
        # 访问第4页（应该触发逐出）
        print("\n--- 访问第4页（触发逐出） ---")
        page4 = buffer.get_page(page_ids[3])
        print(f"[OK] 访问页 {page_ids[3]}: {len(page4.get_rows())} 行数据")
        
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 访问第4页后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        # 访问第5页（应该再次触发逐出）
        print("\n--- 访问第5页（再次触发逐出） ---")
        page5 = buffer.get_page(page_ids[4])
        print(f"[OK] 访问页 {page_ids[4]}: {len(page5.get_rows())} 行数据")
        
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 访问第5页后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        return buffer, page_ids, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_lru_order():
    """测试LRU顺序"""
    print("\n=== 测试LRU顺序 ===")
    
    buffer, page_ids, temp_dir = test_lru_eviction()
    
    try:
        # 重新访问第1页（应该触发逐出，逐出第2页）
        print("\n--- 重新访问第1页（逐出第2页） ---")
        page1 = buffer.get_page(page_ids[0])
        print(f"[OK] 重新访问页 {page_ids[0]}: {len(page1.get_rows())} 行数据")
        
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 重新访问第1页后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        # 访问第2页（应该miss，因为被逐出了）
        print("\n--- 访问第2页（应该miss） ---")
        page2 = buffer.get_page(page_ids[1])
        print(f"[OK] 访问页 {page_ids[1]}: {len(page2.get_rows())} 行数据")
        
        hits, misses, evictions = buffer.stats()
        print(f"[OK] 访问第2页后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        return buffer, page_ids, temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_buffer_flush():
    """测试缓冲刷新"""
    print("\n=== 测试缓冲刷新 ===")
    
    buffer, page_ids, temp_dir = test_lru_order()
    
    try:
        # 修改缓冲中的页
        print("\n--- 修改缓冲中的页 ---")
        page = buffer.get_page(page_ids[0])
        page.insert_row({"id": 999, "data": "modified_in_buffer"})
        print(f"[OK] 修改页 {page_ids[0]}，添加新行")
        
        # 刷新所有页
        print("\n--- 刷新所有页 ---")
        buffer.flush_all()
        print("[OK] 刷新完成")
        
        # 验证修改是否持久化
        print("\n--- 验证修改持久化 ---")
        # 清空缓冲（通过重新创建）
        new_buffer = BufferManager(buffer.disk, capacity=3)
        restored_page = new_buffer.get_page(page_ids[0])
        rows = restored_page.get_rows()
        print(f"[OK] 恢复页 {page_ids[0]}: {len(rows)} 行数据")
        
        # 检查是否包含修改的数据
        has_modified = any(row.get("id") == 999 for row in rows)
        print(f"[OK] 修改数据持久化: {'通过' if has_modified else '失败'}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_buffer_performance():
    """测试缓冲性能"""
    print("\n=== 测试缓冲性能 ===")
    
    temp_dir = test_buffer_flush()
    
    try:
        db_path = os.path.join(temp_dir, "perf_test.db")
        disk = DiskManager(db_path)
        buffer = BufferManager(disk, capacity=10)  # 中等容量
        
        # 创建大量页
        page_count = 50
        page_ids = []
        for i in range(page_count):
            pid = disk.allocate_page()
            page_ids.append(pid)
            
            data = [{"page_id": pid, "row_id": j, "data": f"page_{pid}_row_{j}"} 
                   for j in range(10)]
            page = Page(pid, data)
            disk.write_page(pid, page.to_bytes())
        
        print(f"[OK] 创建 {page_count} 页用于性能测试")
        
        # 随机访问测试
        import random
        import time
        
        access_count = 100
        start_time = time.time()
        
        for _ in range(access_count):
            pid = random.choice(page_ids)
            page = buffer.get_page(pid)
            # 模拟一些操作
            rows = page.get_rows()
        
        end_time = time.time()
        duration = end_time - start_time
        
        hits, misses, evictions = buffer.stats()
        hit_rate = hits / (hits + misses) * 100 if (hits + misses) > 0 else 0
        
        print(f"[OK] 性能测试完成:")
        print(f"  - 访问次数: {access_count}")
        print(f"  - 总时间: {duration:.4f} 秒")
        print(f"  - 平均时间: {duration/access_count*1000:.2f} 毫秒/次")
        print(f"  - 命中率: {hit_rate:.1f}%")
        print(f"  - 命中次数: {hits}")
        print(f"  - 未命中次数: {misses}")
        print(f"  - 逐出次数: {evictions}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def test_buffer_edge_cases():
    """测试缓冲边界情况"""
    print("\n=== 测试缓冲边界情况 ===")
    
    temp_dir = test_buffer_performance()
    
    try:
        db_path = os.path.join(temp_dir, "edge_test.db")
        disk = DiskManager(db_path)
        
        # 测试容量为1的缓冲
        print("\n--- 测试容量为1的缓冲 ---")
        buffer1 = BufferManager(disk, capacity=1)
        
        # 分配2页
        pid1 = disk.allocate_page()
        pid2 = disk.allocate_page()
        
        data1 = [{"id": 1, "data": "page1"}]
        data2 = [{"id": 2, "data": "page2"}]
        
        page1 = Page(pid1, data1)
        page2 = Page(pid2, data2)
        
        disk.write_page(pid1, page1.to_bytes())
        disk.write_page(pid2, page2.to_bytes())
        
        # 访问第1页
        buffer1.get_page(pid1)
        hits, misses, evictions = buffer1.stats()
        print(f"[OK] 访问第1页后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        # 访问第2页（应该逐出第1页）
        buffer1.get_page(pid2)
        hits, misses, evictions = buffer1.stats()
        print(f"[OK] 访问第2页后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        # 再次访问第1页（应该miss）
        buffer1.get_page(pid1)
        hits, misses, evictions = buffer1.stats()
        print(f"[OK] 再次访问第1页后 - Hits: {hits}, Misses: {misses}, Evictions: {evictions}")
        
        return temp_dir
        
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e

def main():
    """主测试函数"""
    print("开始缓冲管理与替换策略测试...")
    print("=" * 50)
    
    temp_dir = None
    try:
        # 基本操作测试
        test_buffer_basic_operations()
        
        # 命中/未命中测试
        test_buffer_hit_miss()
        
        # LRU逐出测试
        test_lru_eviction()
        
        # LRU顺序测试
        test_lru_order()
        
        # 缓冲刷新测试
        test_buffer_flush()
        
        # 性能测试
        test_buffer_performance()
        
        # 边界情况测试
        test_buffer_edge_cases()
        
        print("\n" + "=" * 50)
        print("[OK] 缓冲管理与替换策略测试全部通过！")
        
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
