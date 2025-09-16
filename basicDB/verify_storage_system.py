"""存储系统全面验证脚本"""

import os
import sys
import json
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from storage import StorageEngine, Record, TableMetadata


def test_comprehensive_storage():
    """全面测试存储系统功能"""
    
    print("存储系统全面验证")
    print("=" * 50)
    
    # 测试数据库文件
    db_path = "comprehensive_test.db"
    
    # 清理测试文件
    if os.path.exists(db_path):
        os.remove(db_path)
    
    tests_passed = 0
    tests_total = 0
    
    try:
        with StorageEngine(db_path, buffer_size=16) as storage:
            
            # 测试1: 创建多个表
            print("\n1. 测试创建多个表...")
            tests_total += 1
            
            tables = [
                ("users", ["id", "name", "email", "age"], ["INT", "VARCHAR", "VARCHAR", "INT"]),
                ("products", ["pid", "name", "price"], ["INT", "VARCHAR", "FLOAT"]),
                ("orders", ["oid", "user_id", "product_id", "quantity"], ["INT", "INT", "INT", "INT"])
            ]
            
            for table_name, columns, types in tables:
                success = storage.create_table(table_name, columns, types)
                print(f"   创建表 {table_name}: {'成功' if success else '失败'}")
                if not success:
                    raise Exception(f"创建表 {table_name} 失败")
            
            # 检查所有表
            all_tables = storage.get_all_tables()
            print(f"   总表数: {len(all_tables)}")
            if len(all_tables) == 3:
                tests_passed += 1
                print("   ✓ 创建多表测试通过")
            
            # 测试2: 插入大量数据
            print("\n2. 测试插入大量数据...")
            tests_total += 1
            
            # 插入用户数据
            users_data = [
                [i, f"User{i}", f"user{i}@example.com", 20 + i % 50]
                for i in range(1, 101)  # 100个用户
            ]
            
            success_count = 0
            for user_data in users_data:
                if storage.insert_record("users", user_data):
                    success_count += 1
            
            print(f"   成功插入用户记录: {success_count}/{len(users_data)}")
            
            # 插入产品数据
            products_data = [
                [i, f"Product{i}", round(10.0 + i * 1.5, 2)]
                for i in range(1, 51)  # 50个产品
            ]
            
            for product_data in products_data:
                storage.insert_record("products", product_data)
            
            # 插入订单数据
            orders_data = [
                [i, (i % 100) + 1, (i % 50) + 1, i % 5 + 1]
                for i in range(1, 201)  # 200个订单
            ]
            
            for order_data in orders_data:
                storage.insert_record("orders", order_data)
            
            if success_count >= 95:  # 允许少量失败
                tests_passed += 1
                print("   ✓ 大量数据插入测试通过")
            
            # 测试3: 查询和数据一致性
            print("\n3. 测试数据查询和一致性...")
            tests_total += 1
            
            users_records = storage.scan_table("users")
            products_records = storage.scan_table("products")
            orders_records = storage.scan_table("orders")
            
            print(f"   查询到用户记录: {len(users_records)}")
            print(f"   查询到产品记录: {len(products_records)}")
            print(f"   查询到订单记录: {len(orders_records)}")
            
            # 验证数据一致性
            if len(users_records) >= 95 and len(products_records) == 50 and len(orders_records) == 200:
                # 检查第一个用户的数据
                first_user = users_records[0]
                if first_user.values[0] == 1 and "User1" in first_user.values[1]:
                    tests_passed += 1
                    print("   ✓ 数据查询和一致性测试通过")
                else:
                    print(f"   ✗ 数据不一致: {first_user.values}")
            else:
                print("   ✗ 查询记录数不符合预期")
            
            # 测试4: 表信息和统计
            print("\n4. 测试表信息和统计...")
            tests_total += 1
            
            for table_name in ["users", "products", "orders"]:
                table_info = storage.get_table_info(table_name)
                if table_info:
                    print(f"   表 {table_name}: {table_info.record_count} 条记录, {len(table_info.page_ids)} 个页面")
                else:
                    print(f"   ✗ 无法获取表 {table_name} 的信息")
            
            stats = storage.get_statistics()
            print(f"   系统统计:")
            print(f"     表数量: {stats['tables_count']}")
            print(f"     总记录数: {stats['total_records']}")
            print(f"     总页面数: {stats['total_pages']}")
            print(f"     数据库大小: {stats['database_size']} 字节")
            print(f"     缓存命中率: {stats['buffer_stats']['hit_rate']:.2%}")
            
            if stats['tables_count'] == 3 and stats['total_records'] >= 345:
                tests_passed += 1
                print("   ✓ 统计信息测试通过")
            
            # 测试5: 重复创建和错误处理
            print("\n5. 测试错误处理...")
            tests_total += 1
            
            # 尝试创建重复的表
            duplicate_result = storage.create_table("users", ["test"], ["VARCHAR"])
            print(f"   重复创建表: {'成功' if duplicate_result else '正确拒绝'}")
            
            # 尝试插入不存在的表
            invalid_insert = storage.insert_record("nonexistent", [1, "test"])
            print(f"   插入不存在表: {'成功' if invalid_insert else '正确拒绝'}")
            
            # 查询不存在的表
            invalid_query = storage.scan_table("nonexistent")
            print(f"   查询不存在表: {len(invalid_query)} 条记录")
            
            if not duplicate_result and not invalid_insert and len(invalid_query) == 0:
                tests_passed += 1
                print("   ✓ 错误处理测试通过")
        
        # 测试6: 持久化和重新加载
        print("\n6. 测试数据持久化...")
        tests_total += 1
        
        # 重新打开数据库
        with StorageEngine(db_path, buffer_size=16) as storage2:
            tables_after_reload = storage2.get_all_tables()
            stats_after_reload = storage2.get_statistics()
            
            print(f"   重新加载后表数量: {len(tables_after_reload)}")
            print(f"   重新加载后总记录数: {stats_after_reload['total_records']}")
            
            if len(tables_after_reload) == 3 and stats_after_reload['total_records'] >= 345:
                tests_passed += 1
                print("   ✓ 数据持久化测试通过")
            
            # 测试继续插入数据
            additional_users = [[201, "NewUser", "new@example.com", 25]]
            for user in additional_users:
                storage2.insert_record("users", user)
            
            final_stats = storage2.get_statistics()
            print(f"   最终记录数: {final_stats['total_records']}")
    
    except Exception as e:
        print(f"\n✗ 测试过程中出现异常: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    # 输出测试结果
    print(f"\n测试结果总结:")
    print(f"通过: {tests_passed}/{tests_total}")
    print(f"成功率: {tests_passed/tests_total*100:.1f}%")
    
    if tests_passed == tests_total:
        print("\n🎉 存储系统全面验证成功!")
        return True
    else:
        print(f"\n❌ 存储系统验证失败，{tests_total-tests_passed} 个测试未通过")
        return False


def main():
    """主函数"""
    success = test_comprehensive_storage()
    
    # 清理测试文件
    test_files = ["comprehensive_test.db", "simple_test.db", "debug_test.db", "test_storage.db"]
    cleaned = 0
    for file_name in test_files:
        if os.path.exists(file_name):
            try:
                os.remove(file_name)
                cleaned += 1
            except OSError:
                pass
    
    if cleaned > 0:
        print(f"\n🧹 清理了 {cleaned} 个测试文件")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = main()
