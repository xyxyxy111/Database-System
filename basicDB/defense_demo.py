"""

运行方式：
    python defense_demo.py

演示内容顺序：
1. 环境初始化与数据库创建
2. 语法/错误诊断示例
3. 基础DDL & DML（CREATE / INSERT / SELECT）
4. WHERE 条件与常量折叠 + 谓词下推（展示优化前后）
5. UPDATE / DELETE 操作
6. JOIN 示例
7. ORDER BY 示例
8. 聚合函数（COUNT / SUM / AVG / MAX / MIN / DISTINCT）
9. 事务演示（BEGIN / 回滚 / 提交）
10. 缓存/存储/性能/优化统计展示
11. 可选：备份与数据库信息

说明：
- 输出统一带有分隔线，便于在答辩中快速定位阶段。
- 失败时继续演示（除非结构性错误）。
"""

from __future__ import annotations

import os
from textwrap import dedent

from database import dbEngine


def print_section(title: str):
    print("\n" + "=" * 80)
    print(f"▶ {title}")
    print("=" * 80)


def safe_exec(engine: dbEngine, sql: str, label: str | None = None):
    sql_clean = " ".join(line.strip() for line in sql.strip().splitlines())
    if label:
        print(f"\n--- {label} ---")
    print(f"SQL> {sql_clean}")
    result = engine.execute_sql(sql)
    if result.success:
        if result.data:
            print(
                "✔ 成功 | 行数="
                f"{len(result.data)} | 耗时={result.execution_time:.4f}s"
            )
            for i, row in enumerate(result.data[:10], 1):
                print(f"  #{i}: {row}")
            if len(result.data) > 10:
                print("  ... (更多结果省略)")
        else:
            print(
                "✔ 成功 | "
                f"{result.message} | 耗时={result.execution_time:.4f}s | "
                f"影响行数={result.affected_rows}"
            )
    else:
        print(f"✘ 失败: {result.message}")
    return result


def main():
    print_section("1. 初始化数据库")
    db_path = "defense_demo.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    engine = dbEngine(db_path)
    print(f"数据库创建: {db_path}")

    # 2. 语法/错误诊断示例
    print_section("2. 语法错误与诊断示例")
    safe_exec(engine, "SELET * FROM users;", "拼写错误示例")

    # 3. 建表与插入
    print_section("3. 建表与基础数据加载")
    safe_exec(
        engine,
        dedent(
            """
            CREATE TABLE users (
                id INT,
                name VARCHAR(50),
                age INT
            )
            """
        ),
        "创建 users 表",
    )
    for row in [
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35),
        (4, "David", 28),
        (5, "Eve", 40),
    ]:
        safe_exec(
            engine,
            f"INSERT INTO users VALUES ({row[0]}, '{row[1]}', {row[2]})",
        )

    safe_exec(engine, "SELECT * FROM users", "基础 SELECT")

    # 4. WHERE + 优化展示
    print_section("4. WHERE 条件 + 常量折叠 / 谓词下推")
    complex_sql = "SELECT * FROM users WHERE age > 20 + 5 AND age > 25"  # 可折叠
    safe_exec(engine, complex_sql, "优化前后计划演示（查看控制台计划输出如已集成）")

    # 5. UPDATE / DELETE
    print_section("5. UPDATE / DELETE 操作")
    safe_exec(engine, "UPDATE users SET age = 29 WHERE name = 'David'")
    safe_exec(engine, "DELETE FROM users WHERE name = 'Eve'")
    safe_exec(engine, "SELECT * FROM users", "查看更新/删除结果")

    # 6. JOIN 示例
    print_section("6. JOIN 示例")
    safe_exec(
        engine,
        dedent(
            """
            CREATE TABLE orders (
                id INT,
                user_id INT,
                amount INT
            )
            """
        ),
        "创建 orders 表",
    )
    for row in [
        (100, 1, 150),
        (101, 1, 200),
        (102, 2, 50),
        (103, 3, 300),
    ]:
        safe_exec(
            engine,
            f"INSERT INTO orders VALUES ({row[0]}, {row[1]}, {row[2]})",
        )

    join_sql = dedent(
        """
        SELECT users.name, orders.amount
        FROM users JOIN orders ON users.id = orders.user_id
        WHERE orders.amount > 100
        ORDER BY orders.amount
        """
    )
    safe_exec(engine, join_sql, "JOIN + 过滤 + 排序")

    # 7. ORDER BY 独立演示
    print_section("7. ORDER BY 演示")
    safe_exec(engine, "SELECT * FROM users ORDER BY age DESC")

    # 8. 聚合函数
    print_section("8. 聚合函数演示")
    for agg_sql in [
        "SELECT COUNT(*) FROM users",
        "SELECT MAX(age) FROM users",
        "SELECT MIN(age) FROM users",
        "SELECT AVG(age) FROM users",
        "SELECT SUM(age) FROM users",
    ]:
        safe_exec(engine, agg_sql)

    # 9. 事务演示
    print_section("9. 事务演示 (BEGIN / ROLLBACK / COMMIT)")
    safe_exec(engine, "BEGIN")
    safe_exec(engine, "INSERT INTO users VALUES (6, 'TempUser', 99)")
    safe_exec(engine, "ROLLBACK")
    safe_exec(engine, "SELECT * FROM users WHERE name = 'TempUser'", "验证回滚")

    safe_exec(engine, "BEGIN")
    safe_exec(engine, "INSERT INTO users VALUES (7, 'Grace', 27)")
    safe_exec(engine, "COMMIT")
    safe_exec(engine, "SELECT * FROM users WHERE name = 'Grace'", "验证提交")

    # 10. 统计与信息
    print_section("10. 系统统计信息展示")
    print("数据库信息:", engine.get_database_info())
    print("性能统计:", engine.get_performance_stats())
    if hasattr(engine, "get_optimization_report"):
        print("优化统计:", engine.get_optimization_report())
    if hasattr(engine, "get_performance_report"):
        print("执行性能报告:", engine.get_performance_report())

    # 11. 可选：备份
    print_section("11. 备份示例 (可选)")
    backup_path = "defense_demo_backup.db"
    engine.backup_database(backup_path)
    print(f"备份文件存在: {os.path.exists(backup_path)}")

    print_section("✅ 演示完成")


if __name__ == "__main__":
    main()
