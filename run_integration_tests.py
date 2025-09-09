#!/usr/bin/env python3
"""
集成测试运行脚本
运行所有集成测试：CRUD、执行引擎、查询语言、系统集成
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
                              capture_output=True, text=True, timeout=600)
        
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
        print(f"[TIMEOUT] {test_name} 测试超时 (超过10分钟)")
        return False
    except Exception as e:
        print(f"[ERROR] {test_name} 测试异常: {e}")
        return False

def main():
    """主函数"""
    print("数据库系统集成测试套件")
    print("=" * 60)
    
    # 测试列表
    tests = [
        ("核心CRUD操作测试", "test_crud_operations.py"),
        ("执行引擎测试", "test_execution_engine.py"),
        ("查询语言测试", "test_query_language.py"),
        ("系统集成测试", "test_system_integration.py"),
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
        print("\n[SUCCESS] 所有集成测试都通过了！")
        print("\n数据库系统功能验证:")
        print("✓ 核心CRUD操作 (CREATE, INSERT, SELECT, DELETE)")
        print("✓ 执行引擎 (算子、执行计划、性能)")
        print("✓ 查询语言 (词法分析、语法分析、语义分析)")
        print("✓ 系统集成 (存储引擎、缓冲管理、持久化)")
    else:
        print(f"\n[WARNING] 有 {failed} 个测试失败，请检查错误信息")

if __name__ == "__main__":
    main()
