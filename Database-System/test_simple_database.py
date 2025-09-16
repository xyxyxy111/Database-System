"""简化数据库引擎测试 - 避开复杂语法"""

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database import dbEngine


def test_simple_database():
    """测试数据库引擎的基础功能"""
    
    print("MiniDB 简化功能测试")
    print("=" * 30)
    
    # 测试数据库文件
    db_path = "simple_test_db.db"
    
    # 清理现有文件
    if os.path.exists(db_path):
        os.remove(db_path)
    
    try:
        with dbEngine(db_path, buffer_size=8) as db:
            
            # 测试1: 创建简单表
            print("1. 创建简单表...")
            sql = "CREATE TABLE users (id INT, name VARCHAR(50), age INT)"
            result = db.execute_sql(sql)
            print(f"   创建表: {'成功' if result.success else '失败'}")
            if not result.success:
                print(f"   错误: {result.message}")
            
            # 测试2: 插入数据
            if result.success:
                print("2. 插入测试数据...")
                
                insert_sqls = [
                    "INSERT INTO users VALUES (1, 'Alice', 25)",
                    "INSERT INTO users VALUES (2, 'Bob', 30)" 
                ]
                
                for sql in insert_sqls:
                    result = db.execute_sql(sql)
                    print(f"   插入: {'成功' if result.success else '失败'}")
                    if not result.success:
                        print(f"   错误: {result.message}")
                
                # 测试3: 查询数据
                print("3. 查询数据...")
                result = db.execute_sql("SELECT id, name, age FROM users")
                print(f"   查询: {'成功' if result.success else '失败'}")
                
                if result.success:
                    print(f"   结果: {result.data}")
                else:
                    print(f"   错误: {result.message}")
                
                # 测试4: 数据库信息
                print("4. 数据库信息...")
                db_info = db.get_database_info()
                print(f"   表数量: {db_info['tables']}")
                print(f"   记录数: {db_info['total_records']}")
                
                # 性能统计
                try:
                    perf_stats = db.get_performance_stats()
                    print(f"   缓存命中率: {perf_stats['cache_hit_rate']:.2%}")
                except Exception as e:
                    print(f"   性能统计错误: {e}")
            
            print("\n✓ 简化功能测试完成")
            
    except Exception as e:
        print(f"\n✗ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # 清理文件
    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    test_simple_database()
