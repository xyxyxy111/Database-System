"""
MiniDB 主程序入口
提供命令行界面，用于执行SQL语句
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from compiler import Catalog, PlanGenerator, SemanticAnalyzer, SQLLexer, SQLParser


class MiniDBCompiler:
    """MiniDB SQL编译器"""

    def __init__(self):
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        self.plan_generator = PlanGenerator(self.catalog)

    def compile_sql(self, sql: str) -> dict:
        """编译SQL语句，返回编译结果"""
        result = {
            "success": False,
            "tokens": None,
            "ast": None,
            "semantic_errors": [],
            "execution_plans": [],
            "error_message": None,
        }

        try:
            # 1. 词法分析
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            result["tokens"] = tokens

            # 2. 语法分析
            parser = SQLParser(tokens)
            ast = parser.parse()
            result["ast"] = ast

            # 3. 语义分析
            success, errors = self.semantic_analyzer.analyze(ast)
            result["semantic_errors"] = errors

            if success:
                # 4. 执行计划生成
                plans = self.plan_generator.generate(ast)
                result["execution_plans"] = plans
                result["success"] = True
            else:
                result["error_message"] = "语义分析失败"

        except Exception as e:
            result["error_message"] = str(e)

        return result

    def print_compilation_result(self, result: dict, verbose: bool = True):
        """打印编译结果"""
        if result["success"]:
            print("✓ SQL编译成功!")

            if verbose:
                # 打印Token信息
                if result["tokens"]:
                    print(
                        f"\n词法分析: 生成了 {len([t for t in result['tokens'] if t.type.name != 'EOF'])} 个Token"
                    )

                # 打印AST信息
                if result["ast"]:
                    print(f"语法分析: 解析了 {len(result['ast'].statements)} 个语句")

                # 打印执行计划
                if result["execution_plans"]:
                    print("\n执行计划:")
                    print("-" * 40)
                    for i, plan in enumerate(result["execution_plans"], 1):
                        print(f"Plan {i}: {plan.operator_type}")
                        if plan.properties:
                            for key, value in plan.properties.items():
                                print(f"  {key}: {value}")
                        print()
        else:
            print("✗ SQL编译失败!")
            if result["error_message"]:
                print(f"错误: {result['error_message']}")

            if result["semantic_errors"]:
                print("语义错误:")
                for error in result["semantic_errors"]:
                    print(f"  - {error}")


def interactive_mode():
    """交互模式"""
    print("MiniDB SQL编译器 - 交互模式")
    print("输入SQL语句进行编译，输入'exit'或'quit'退出")
    print("输入'help'查看帮助信息")
    print("-" * 50)

    compiler = MiniDBCompiler()
    # 初始化数据库引擎
    from database import DatabaseEngine

    db_path = "minidb_interactive.db"
    # 保留历史数据，避免每次交互模式启动清空数据库
    db = DatabaseEngine(db_path, buffer_size=16)
    while True:
        try:
            sql = input("MiniDB> ").strip()
            if sql.lower() in ["exit", "quit"]:
                break
            elif sql.lower() == "help":
                print_help()
                continue
            elif sql.lower() == "show tables":
                tables = compiler.catalog.get_all_tables()
                if tables:
                    print("表列表:")
                    for table in tables:
                        table_info = compiler.catalog.get_table_info(table)
                        columns = [
                            f"{col.name}({col.data_type})" for col in table_info.columns
                        ]
                        print(f"  {table}: {', '.join(columns)}")
                else:
                    print("没有表")
                continue
            elif not sql:
                continue
            # 检查是否为 SELECT 查询
            if sql.strip().lower().startswith("select"):
                result = db.execute_sql(sql)
                print("\n--- 查询反馈 ---")
                if result.success:
                    print("✓ 查询成功!")
                    if result.data:
                        print("📊 查询结果:")
                        for i, row in enumerate(result.data, 1):
                            print(f"  第{i}行: {row}")
                    else:
                        print("(无数据返回)")
                else:
                    print(f"✗ 查询失败: {result.message}")
                print("--- 反馈结束 ---\n")
            else:
                result = compiler.compile_sql(sql)
                print("\n--- 编译反馈 ---")
                if result["success"]:
                    print("✓ SQL编译成功!")
                    if result["tokens"]:
                        print(
                            f"词法分析: {len([t for t in result['tokens'] if t.type.name != 'EOF'])} 个Token"
                        )
                        print(
                            "Tokens:",
                            [str(t) for t in result["tokens"] if t.type.name != "EOF"],
                        )
                    if result["ast"]:
                        print(f"语法分析: {len(result['ast'].statements)} 个语句")
                        print("AST:", result["ast"])
                    if result["execution_plans"]:
                        print("执行计划:")
                        for i, plan in enumerate(result["execution_plans"], 1):
                            print(f"  Plan {i}: {plan.operator_type}")
                            if plan.properties:
                                for key, value in plan.properties.items():
                                    print(f"    {key}: {value}")
                    # 非 SELECT 语句也尝试实际执行
                    exec_result = db.execute_sql(sql)
                    if exec_result.success:
                        print(f"✅ 执行成功: {exec_result.message}")
                        if getattr(exec_result, "affected_rows", 0) > 0:
                            print(f"影响行数: {exec_result.affected_rows}")
                    else:
                        print(f"❌ 执行失败: {exec_result.message}")
                else:
                    print("✗ SQL编译失败!")
                    if result["error_message"]:
                        print(f"错误: {result['error_message']}")
                    if result["semantic_errors"]:
                        print("语义错误:")
                        for error in result["semantic_errors"]:
                            print(f"  - {error}")
                print("--- 反馈结束 ---\n")
        except KeyboardInterrupt:
            print("\n再见!")
            break
        except EOFError:
            print("\n再见!")
            break


def batch_mode(sql_file: str):
    """批处理模式"""
    if not os.path.exists(sql_file):
        print(f"错误: 文件 '{sql_file}' 不存在")
        return

    print(f"MiniDB SQL编译器 - 批处理模式")
    print(f"处理文件: {sql_file}")
    print("-" * 50)

    compiler = MiniDBCompiler()

    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        result = compiler.compile_sql(sql_content)
        compiler.print_compilation_result(result, verbose=True)

    except Exception as e:
        print(f"读取文件失败: {e}")


def print_help():
    """打印帮助信息"""
    print(
        """
MiniDB SQL编译器帮助信息:

支持的SQL语句:
  - CREATE TABLE table_name(col1 type1, col2 type2, ...)
  - INSERT INTO table_name(col1, col2, ...) VALUES (val1, val2, ...)
  - SELECT col1, col2, ... FROM table_name [WHERE condition]
  - DELETE FROM table_name [WHERE condition]

支持的数据类型:
  - INT/INTEGER: 整数类型
  - VARCHAR(n): 可变长字符串
  - CHAR(n): 固定长字符串

特殊命令:
  - help: 显示帮助信息
  - show tables: 显示所有表
  - exit/quit: 退出程序

示例:
  CREATE TABLE student(id INT, name VARCHAR(50), age INT);
  INSERT INTO student VALUES (1, 'Alice', 20);
  SELECT id, name FROM student WHERE age > 18;
"""
    )


def main():
    """主函数"""
    if len(sys.argv) == 1:
        # 交互模式
        interactive_mode()
    elif len(sys.argv) == 2:
        if sys.argv[1] in ["-h", "--help"]:
            print_help()
        else:
            # 批处理模式
            batch_mode(sys.argv[1])
    else:
        print("用法:")
        print("  python main.py                # 交互模式")
        print("  python main.py <sql_file>     # 批处理模式")
        print("  python main.py --help         # 显示帮助")


if __name__ == "__main__":
    main()
