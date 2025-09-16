"""调试 WHERE 条件处理"""

import os

from database import dbEngine


def debug_where():
    db_path = "debug_where.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    try:
        with dbEngine(db_path) as db:
            # 创建表
            db.execute_sql("CREATE TABLE test (id INT, age INT)")

            # 插入数据
            db.execute_sql("INSERT INTO test VALUES (1, 20)")
            db.execute_sql("INSERT INTO test VALUES (2, 25)")
            db.execute_sql("INSERT INTO test VALUES (3, 18)")

            # 先用编译器测试
            from compiler import PlanGenerator, SQLLexer, SQLParser
            from compiler.catalog import Catalog, ColumnInfo

            sql = "SELECT id, age FROM test WHERE age > 20"
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            print(f"Tokens: {[str(t) for t in tokens[:10]]}")

            parser = SQLParser(tokens)
            ast = parser.parse()
            print(f"AST: {ast}")

            catalog = Catalog()
            catalog.create_table(
                "test", [ColumnInfo("id", "INT"), ColumnInfo("age", "INT")]
            )

            plan_gen = PlanGenerator(catalog)
            plans = plan_gen.generate(ast)
            print(f"执行计划: {plans[0].to_dict()}")

    except Exception as e:
        print(f"错误: {e}")
        import traceback

        traceback.print_exc()

    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    debug_where()
