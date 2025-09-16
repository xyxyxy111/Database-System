"""简化的存储引擎测试"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.storage_engine import StorageEngine


def test_storage_engine():
    """测试存储引擎的基本功能"""
    
    db_path = "simple_test.db"
    
    # 清理测试文件
    if os.path.exists(db_path):
        os.remove(db_path)
    
    print("简化存储引擎测试")
    print("=" * 30)
    
    try:
        # 创建存储引擎
        storage = StorageEngine(db_path, buffer_size=4)
        
        # 测试1: 创建表
        print("1. 测试创建表...")
        success = storage.create_table("test_table", 
                                     ["id", "name"], 
                                     ["INT", "VARCHAR"])
        print(f"   创建表: {'成功' if success else '失败'}")
        
        # 测试2: 获取表信息
        print("2. 测试获取表信息...")
        table_info = storage.get_table_info("test_table")
        if table_info:
            print(f"   表名: {table_info.table_name}")
            print(f"   列名: {table_info.column_names}")
            print(f"   列类型: {table_info.column_types}")
        else:
            print("   获取表信息失败")
        
        # 测试3: 插入数据  
        print("3. 测试插入数据...")
        test_records = [
            [1, "Alice"],
            [2, "Bob"],
            [3, "Charlie"]
        ]
        
        for i, record in enumerate(test_records):
            success = storage.insert_record("test_table", record)
            print(f"   插入记录{i+1}: {'成功' if success else '失败'}")
        
        # 测试4: 查询数据
        print("4. 测试查询数据...")
        records = storage.scan_table("test_table")
        print(f"   查询到 {len(records)} 条记录:")
        for record in records:
            print(f"     {record.values}")
        
        # 测试5: 获取统计信息
        print("5. 测试统计信息...")
        stats = storage.get_statistics()
        print(f"   表数量: {stats['tables_count']}")
        print(f"   记录总数: {stats['total_records']}")
        print(f"   页面总数: {stats['total_pages']}")
        
        # 关闭存储引擎
        storage.close()
        
        print("\n✓ 存储引擎测试成功完成!")
        
    except Exception as e:
        print(f"\n✗ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_storage_engine()
