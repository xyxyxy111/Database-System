#!/usr/bin/env python3
"""
存储系统测试运行脚本
运行所有存储相关的测试
"""

import sys
import os
import subprocess
import time

def run_test(test_name, test_file):
    """运行单个测试"""
    print(f"\n{'='*60}")
    print(f"运行测试: {test_name}")
    print(f"测试文件: {test_file}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        # 运行测试
        result = subprocess.run([sys.executable, test_file], 
                              capture_output=True, text=True, timeout=300)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            print(f"[PASS] {test_name} 测试通过 (耗时: {duration:.2f}秒)")
            if result.stdout:
                print("输出:")
                print(result.stdout)
        else:
            print(f"[FAIL] {test_name} 测试失败 (耗时: {duration:.2f}秒)")
            if result.stderr:
                print("错误信息:")
                print(result.stderr)
            if result.stdout:
                print("输出:")
                print(result.stdout)
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"[TIMEOUT] {test_name} 测试超时 (超过5分钟)")
        return False
    except Exception as e:
        print(f"[ERROR] {test_name} 测试异常: {e}")
        return False

def main():
    """主函数"""
    print("存储系统测试套件")
    print("=" * 60)
    
    # 测试列表
    tests = [
        ("页式存储测试", "test_page_storage.py"),
        ("文件页I/O测试", "test_file_io.py"),
        ("缓冲管理测试", "test_buffer_management.py"),
        ("存储系统集成测试", "test_storage_integration.py"),
    ]
    
    # 检查测试文件是否存在
    missing_files = []
    for test_name, test_file in tests:
        if not os.path.exists(test_file):
            missing_files.append(test_file)
    
    if missing_files:
        print(f"[ERROR] 缺少测试文件: {missing_files}")
        return
    
    # 运行测试
    passed = 0
    failed = 0
    
    for test_name, test_file in tests:
        success = run_test(test_name, test_file)
        if success:
            passed += 1
        else:
            failed += 1
    
    # 输出总结
    print(f"\n{'='*60}")
    print("测试总结")
    print(f"{'='*60}")
    print(f"总测试数: {len(tests)}")
    print(f"通过: {passed}")
    print(f"失败: {failed}")
    print(f"成功率: {passed/len(tests)*100:.1f}%")
    
    if failed == 0:
        print("\n[SUCCESS] 所有测试都通过了！")
    else:
        print(f"\n[WARNING] 有 {failed} 个测试失败，请检查错误信息")

if __name__ == "__main__":
    main()
