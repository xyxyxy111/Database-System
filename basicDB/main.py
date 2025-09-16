import os
import sys
from pathlib import Path

from compiler import dbCompiler
# 初始化数据库引擎
from database import dbEngine
db_path = "minidb_interactive.db"
if os.path.exists(db_path):
    os.remove(db_path)

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def interactive():
    """命令行交互"""
    print("=" * 50)
    print("命令行交互")

    #编译器实例化
    compiler = dbCompiler()
    #数据库引擎实例化
    db = dbEngine(db_path, buffer_size=16)

    while True:
        try:
            sql = input("BasicDB> ").strip().lower()
            if not sql:
                continue
            if sql in ["exit", "quit"]:
                break
            elif sql == "help":
                show_help()
                continue
            #SHOW命令
            elif sql == "show tables":
                tables = compiler.catalog.get_tables()
                if tables:
                    for table in tables:#遍历所有表
                        table_info = compiler.catalog.get_table_info(table)#获取表详细信息
                        columns = [f"{col.name}({col.data_type})" for col in table_info.columns]#提取列信息
                        print(f"  {table}: {', '.join(columns)}")
                else:
                    print("NULL!!!!!!!!!!")
                continue
            # SELECT命令
            if sql.strip().lower().startswith("select"):
                result = db.execute_sql(sql)
                if result.success:
                    if result.data:
                        for i, row in enumerate(result.data, 1):
                            print(f"  第{i}行: {row}")
                    else:
                        print("NULL!!!!!!!!!!")
                else:
                    print(f"WRONG: {result.message}")

            #非SELECT语句
            else:
                result = compiler.compile_sql(sql)
                if result["success"]:
                    if result["tokens"]:
                        non_eof_tokens = [t for t in result["tokens"] if t.type.name != "EOF"]
                        print(f"词法分析: {len(non_eof_tokens)} 个Token")
                    if result["ast"]:
                        print(f"语法分析: {len(result['ast'].statements)} 个语句")
                    #执行SQL语句
                    execute_result = db.execute_sql(sql)
                    if execute_result.success:
                        print(f"SUCCESS: {execute_result.message}")
                    else:
                        print(f"WRONG: {execute_result.message}")
                else:
                    print("WRONG!!!!!!!!!!!!")
        except KeyboardInterrupt:
            print("\n欢迎下次使用")
            break


def process_file(sql_file: str):
    """批处理模式"""
    if not os.path.exists(sql_file):
        print(f"错误: 文件 '{sql_file}' 不存在")
        return

    print(f"MiniDB SQL编译器 - 批处理模式")
    print(f"处理文件: {sql_file}")
    print("-" * 50)

    compiler = dbCompiler()

    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        result = compiler.compile_sql(sql_content)
        compiler.print_compilation_result(result, verbose=True)

    except Exception as e:
        print(f"读取文件失败: {e}")


def show_help():
    """打印帮助"""
    print(
        """
        =================================================================================================
        支持的SQL语句:
          - CREATE TABLE table_name(col1 type1, col2 type2, ...)
          - INSERT INTO table_name(col1, col2, ...) VALUES (val1, val2, ...)
          - SELECT col1, col2, ... FROM table_name [WHERE condition]
          - DELETE FROM table_name [WHERE condition]
          - UPDATE table_name SET column1 = value1, column2 = value2, ... [WHERE condition];
        
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
          MiniDB> CREATE TABLE students (id INT, name VARCHAR(50), age INT);
            MiniDB> INSERT INTO students VALUES (1, 'Alice', 20);
            MiniDB> INSERT INTO students VALUES (2, 'Bob', 22);
            MiniDB> SELECT * FROM students;
            MiniDB> UPDATE students SET age = 21 WHERE name = 'Alice';
            MiniDB> SELECT * FROM students WHERE age > 20;
            MiniDB> DELETE FROM students WHERE age < 21;
        =================================================================================================
"""
    )


def main():
    if len(sys.argv) == 1:
        interactive()# 交互模式
    if len(sys.argv) == 2:
        if sys.argv[1] in ["-h", "--help"]:
            show_help()
        elif sys.argv[1] in ["-g", "--gui", "gui"]:
            # GUI模式
            try:
                from gui_app import main as gui_main
                gui_main()
            except ImportError as e:
                print(f"GUI模式启动失败: {e}")
                print("回退到交互模式...")
                interactive()
        else:
            process_file(sys.argv[1])# 处理文件


if __name__ == "__main__":
    main()
