"""MiniDB 演示程序"""

import os

from database import dbEngine


def main():
    """MiniDB 基本功能演示"""

    print("🎉 欢迎使用 MiniDB 数据库系统!")
    print("=" * 50)

    # 数据库文件路径
    db_path = "demo.db"

    # 清理现有文件
    if os.path.exists(db_path):
        os.remove(db_path)

    try:
        # 启动数据库引擎
        with dbEngine(db_path, buffer_size=16) as db:
            print(f"✅ 数据库已启动: {db_path}")

            # 演示SQL操作
            demo_sqls = [
                # 创建用户表
                "CREATE TABLE users (id INT, name VARCHAR(50), age INT)",
                # 插入测试数据
                "INSERT INTO users VALUES (1, 'Alice', 25)",
                "INSERT INTO users VALUES (2, 'Bob', 30)",
                "INSERT INTO users VALUES (3, 'Charlie', 35)",
                # 查询数据
                "SELECT id, name, age FROM users",
            ]

            for i, sql in enumerate(demo_sqls, 1):
                print(f"\n📝 执行SQL #{i}:")
                print(f"   {sql}")

                result = db.execute_sql(sql)

                if result.success:
                    print(f"   ✅ 成功: {result.message}")
                    if result.data:
                        print("   📊 查询结果:")
                        for j, row in enumerate(result.data, 1):
                            print(f"      第{j}行: {row}")
                    if result.affected_rows > 0:
                        print(f"   📈 影响行数: {result.affected_rows}")
                else:
                    print(f"   ❌ 失败: {result.message}")

            # 显示数据库统计信息
            print(f"\n📊 数据库统计信息:")
            info = db.get_database_info()
            print(f"   表数量: {info['tables']}")
            print(f"   总记录数: {info['total_records']}")

            # 显示性能统计
            try:
                perf = db.get_performance_stats()
                print(f"   缓存命中率: {perf['cache_hit_rate']:.2%}")
                print(f"   总页面访问: {perf['total_page_accesses']}")
            except Exception as e:
                print(f"   性能统计获取失败: {e}")

            print(f"\n🎊 演示完成！数据库功能正常工作。")

    except Exception as e:
        print(f"❌ 演示失败: {e}")
        import traceback

        traceback.print_exc()

    # 清理演示文件
    if os.path.exists(db_path):
        os.remove(db_path)
        print("🧹 演示文件已清理")


if __name__ == "__main__":
    main()
