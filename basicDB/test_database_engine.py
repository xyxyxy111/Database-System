"""数据库引擎完整测试"""

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database import dbEngine


def test_database_engine():
    """测试数据库引擎的完整功能"""
    
    print("MiniDB 数据库引擎完整测试")
    print("=" * 40)
    
    # 测试数据库文件
    db_path = "test_database_engine.db"
    
    # 清理现有文件
    if os.path.exists(db_path):
        os.remove(db_path)
    
    tests_passed = 0
    tests_total = 0
    
    try:
        with dbEngine(db_path, buffer_size=16) as db:
            
            # 测试1: 基本表操作
            print("\n1. 测试基本表操作...")
            tests_total += 1
            
            # 创建用户表
            sql = """
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                email VARCHAR,
                age INT DEFAULT 25
            )
            """
            result = db.execute_sql(sql)
            print(f"   创建用户表: {'成功' if result.success else '失败'}")
            print(f"   消息: {result.message}")
            
            if result.success:
                # 检查表是否存在
                exists = db.table_exists("users")
                print(f"   表存在检查: {'通过' if exists else '失败'}")
                
                # 获取表信息
                table_info = db.get_table_info("users")
                if table_info:
                    print(f"   表信息: {table_info['name']}, {len(table_info['columns'])} 列")
                    tests_passed += 1
                    print("   ✓ 基本表操作测试通过")
                else:
                    print("   ✗ 获取表信息失败")
            else:
                print("   ✗ 表创建失败")
            
            # 测试2: 数据插入
            print("\n2. 测试数据插入...")
            tests_total += 1
            
            insert_sqls = [
                "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@test.com', 28)",
                "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@test.com', 32)",
                "INSERT INTO users (id, name, email) VALUES (3, 'Charlie', 'charlie@test.com')"
            ]
            
            insert_success = 0
            for sql in insert_sqls:
                result = db.execute_sql(sql)
                if result.success:
                    insert_success += 1
                    print(f"   插入成功: {result.affected_rows} 条记录")
                else:
                    print(f"   插入失败: {result.message}")
            
            if insert_success == len(insert_sqls):
                tests_passed += 1
                print("   ✓ 数据插入测试通过")
            else:
                print(f"   ✗ 数据插入测试失败: {insert_success}/{len(insert_sqls)}")
            
            # 测试3: 数据查询
            print("\n3. 测试数据查询...")
            tests_total += 1
            
            # 查询所有用户
            result = db.execute_sql("SELECT * FROM users")
            print(f"   全表查询: {'成功' if result.success else '失败'}")
            
            if result.success:
                print(f"   查询到 {len(result.data)} 条记录")
                for i, user in enumerate(result.data, 1):
                    print(f"     用户{i}: {user}")
                
                # 测试条件查询
                result2 = db.execute_sql("SELECT name, age FROM users WHERE age > 28")
                print(f"   条件查询: {'成功' if result2.success else '失败'}")
                
                if result2.success:
                    print(f"   条件查询结果: {result2.data}")
                    tests_passed += 1
                    print("   ✓ 数据查询测试通过")
                else:
                    print(f"   ✗ 条件查询失败: {result2.message}")
            else:
                print(f"   ✗ 查询失败: {result.message}")
            
            # 测试4: 多表操作
            print("\n4. 测试多表操作...")
            tests_total += 1
            
            # 创建产品表
            create_products = """
            CREATE TABLE products (
                pid INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                price FLOAT,
                category VARCHAR DEFAULT 'General'
            )
            """
            
            result = db.execute_sql(create_products)
            print(f"   创建产品表: {'成功' if result.success else '失败'}")
            
            if result.success:
                # 批量插入产品
                product_inserts = [
                    "INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics')",
                    "INSERT INTO products VALUES (2, 'Book', 29.99, 'Education')",
                    "INSERT INTO products (pid, name, price) VALUES (3, 'Coffee', 4.99)"
                ]
                
                results = db.execute_batch(product_inserts)
                success_count = sum(1 for r in results if r.success)
                print(f"   批量插入: {success_count}/{len(product_inserts)} 成功")
                
                # 检查所有表
                all_tables = db.get_all_tables()
                print(f"   总表数: {len(all_tables)}")
                print(f"   表列表: {all_tables}")
                
                if len(all_tables) == 2 and success_count == len(product_inserts):
                    tests_passed += 1
                    print("   ✓ 多表操作测试通过")
                else:
                    print("   ✗ 多表操作测试失败")
            else:
                print(f"   ✗ 创建产品表失败: {result.message}")
            
            # 测试5: 数据库信息和统计
            print("\n5. 测试数据库信息...")
            tests_total += 1
            
            # 数据库基本信息
            db_info = db.get_database_info()
            print(f"   数据库名: {db_info['name']}")
            print(f"   表数量: {db_info['tables']}")
            print(f"   总记录数: {db_info['total_records']}")
            print(f"   数据库大小: {db_info['database_size']} 字节")
            
            # 性能统计
            perf_stats = db.get_performance_stats()
            print(f"   缓存命中率: {perf_stats['cache_hit_rate']:.2%}")
            print(f"   缓存命中次数: {perf_stats['cache_hits']}")
            
            # 表详细信息
            for table_name in db.get_all_tables():
                table_info = db.get_table_info(table_name)
                if table_info:
                    print(f"   表 {table_name}: {table_info['record_count']} 条记录, {table_info['page_count']} 个页面")
            
            if db_info['tables'] == 2 and db_info['total_records'] >= 6:
                tests_passed += 1
                print("   ✓ 数据库信息测试通过")
            else:
                print("   ✗ 数据库信息测试失败")
            
            # 测试6: 错误处理
            print("\n6. 测试错误处理...")
            tests_total += 1
            
            # 重复创建表
            result = db.execute_sql("CREATE TABLE users (id INT)")
            print(f"   重复创建表: {'正确拒绝' if not result.success else '错误允许'}")
            
            # 查询不存在的表
            result = db.execute_sql("SELECT * FROM nonexistent")
            print(f"   查询不存在表: {'正确拒绝' if not result.success else '错误允许'}")
            
            # 无效SQL
            result = db.execute_sql("INVALID SQL STATEMENT")
            print(f"   无效SQL: {'正确拒绝' if not result.success else '错误允许'}")
            
            # 空SQL
            result = db.execute_sql("")
            print(f"   空SQL: {'正确拒绝' if not result.success else '错误允许'}")
            
            tests_passed += 1
            print("   ✓ 错误处理测试通过")
            
    except Exception as e:
        print(f"\n✗ 测试过程中出现异常: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    # 输出测试结果
    print(f"\n测试结果总结:")
    print(f"通过: {tests_passed}/{tests_total}")
    print(f"成功率: {tests_passed/tests_total*100:.1f}%")
    
    # 测试7: 数据持久化
    print(f"\n7. 测试数据持久化...")
    try:
        with dbEngine(db_path, buffer_size=16) as db2:
            tables_after_reload = db2.get_all_tables()
            db_info_after = db2.get_database_info()
            
            print(f"   重新加载后表数量: {len(tables_after_reload)}")
            print(f"   重新加载后总记录数: {db_info_after['total_records']}")
            
            # 验证数据完整性
            result = db2.execute_sql("SELECT COUNT(*) as count FROM users")
            if result.success and result.data:
                print(f"   用户表记录数验证: {result.data}")
            
            if len(tables_after_reload) == 2 and db_info_after['total_records'] >= 6:
                print("   ✓ 数据持久化测试通过")
                tests_passed += 1
                tests_total += 1
            else:
                print("   ✗ 数据持久化测试失败")
                tests_total += 1
                
    except Exception as e:
        print(f"   ✗ 持久化测试异常: {str(e)}")
        tests_total += 1
    
    # 最终结果
    print(f"\n最终测试结果:")
    print(f"通过: {tests_passed}/{tests_total}")
    print(f"成功率: {tests_passed/tests_total*100:.1f}%")
    
    if tests_passed == tests_total:
        print("\n🎉 数据库引擎完整测试成功!")
        return True
    else:
        print(f"\n❌ 数据库引擎测试失败，{tests_total-tests_passed} 个测试未通过")
        return False


def main():
    """主函数"""
    success = test_database_engine()
    
    # 清理测试文件
    test_files = ["test_database_engine.db"]
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
