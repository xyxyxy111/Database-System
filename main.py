"""
数据库系统主程序 (Main Program)
==============================

本模块是数据库系统的主入口程序。
它集成了SQL编译器、执行引擎和存储引擎，提供完整的数据库功能。

主要功能：
1. SQL语句解析和执行
2. 多语句支持（分号分隔）
3. 文件执行支持（@file.sql）
4. 调试功能（--debug-pipeline）
5. 统计信息（--stats）
6. 错误处理和异常管理

使用方式：
python main.py <数据库文件> <SQL语句或@文件>
python main.py demo.db "SELECT * FROM users;"
python main.py demo.db @script.sql --debug-pipeline --stats
"""

import sys
import argparse
from typing import Any, Dict, List

from compiler.lexer import tokenize
from compiler.parser import Parser, Select, Insert, CreateTable, Delete
from compiler.sematic_analyzer import SemanticAnalyzer, SemanticError, Analyzed
from compiler.planner import Planner
from execution.sytem_catalog import SystemCatalog
from execution.executor import Executor
from execution.operators import Operator


def ast_to_dict(ast: Any) -> Dict[str, Any]:
    """
    将AST节点转换为字典格式，便于调试输出
    
    参数:
        ast: AST节点
        
    返回:
        Dict[str, Any]: AST的字典表示
    """
    if isinstance(ast, Select):
        return {
            "type": "Select", 
            "columns": ast.columns, 
            "table": ast.table, 
            "where": ast.where
        }
    if isinstance(ast, Insert):
        return {
            "type": "Insert", 
            "table": ast.table, 
            "columns": ast.columns, 
            "values": ast.values
        }
    if isinstance(ast, CreateTable):
        return {
            "type": "CreateTable", 
            "table": ast.table, 
            "columns": ast.columns
        }
    if isinstance(ast, Delete):
        return {
            "type": "Delete", 
            "table": ast.table, 
            "where": ast.where
        }
    return {"type": type(ast).__name__}


def analyzed_to_dict(a: Analyzed) -> Dict[str, Any]:
    """
    将语义分析结果转换为字典格式，便于调试输出
    
    参数:
        a (Analyzed): 语义分析结果
        
    返回:
        Dict[str, Any]: 分析结果的字典表示
    """
    return {"kind": a.kind, "payload": a.payload}


def op_summary(op: Operator) -> str:
    """
    获取算子的简要描述，用于调试输出
    
    参数:
        op (Operator): 执行算子
        
    返回:
        str: 算子的简要描述
    """
    return op.__class__.__name__


def run_sqls(db_file: str, sql: str, debug: bool = False, show_stats: bool = False) -> List[Dict[str, Any]]:
    """
    执行SQL语句的主函数
    
    参数:
        db_file (str): 数据库文件路径
        sql (str): SQL语句字符串
        debug (bool): 是否显示调试信息
        show_stats (bool): 是否显示统计信息
        
    返回:
        List[Dict[str, Any]]: 执行结果列表
        
    功能:
        1. 初始化系统组件（目录、执行器、分析器）
        2. 词法分析：将SQL字符串分解为Token
        3. 语法分析：将Token解析为AST
        4. 语义分析：检查语义正确性
        5. 查询规划：生成执行计划
        6. 执行：运行执行计划并收集结果
        7. 统计：显示缓冲管理统计信息
    """
    # 初始化系统组件
    syscat = SystemCatalog(db_file)  # 系统目录
    executor = Executor(syscat)      # 执行器
    analyzer = SemanticAnalyzer(syscat)  # 语义分析器

    # 词法分析
    if debug:
        toks = tokenize(sql)
        print("[Tokens]")
        for t in toks:
            print(t)

    # 语法分析
    parser = Parser(sql)
    asts = parser.parse_many()

    if debug:
        print("[AST]")
        for ast in asts:
            print(ast_to_dict(ast))

    # 执行所有语句
    rows: List[Dict[str, Any]] = []
    for ast in asts:
        # 语义分析
        analyzed = analyzer.analyze(ast)
        if debug:
            print("[Analyzed]")
            print(analyzed_to_dict(analyzed))
        
        # 查询规划
        planner = Planner(executor)
        op = planner.plan(analyzed)
        if debug:
            print("[PlanRoot]", op_summary(op))
        
        # 执行
        res = executor.execute_plan(op)
        rows.extend(res)

    # 显示统计信息
    if show_stats:
        hits, misses, evictions = syscat.buffer.stats()
        print(f"[BufferStats] hits={hits}, misses={misses}, evictions={evictions}")

    return rows


def main() -> None:
    """
    主函数
    
    解析命令行参数，执行SQL语句，处理异常。
    """
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Mini DB System")
    parser.add_argument("db_file", help="database file path")
    parser.add_argument("sql", help="SQL string or @file.sql")
    parser.add_argument("--debug-pipeline", dest="debug", action="store_true", 
                       help="print tokens/AST/analyzed/plan")
    parser.add_argument("--stats", dest="stats", action="store_true", 
                       help="print buffer manager stats")
    args = parser.parse_args()

    db_file = args.db_file
    sql_arg = args.sql
    
    # 处理文件输入（@file.sql）
    if sql_arg.startswith("@"):
        with open(sql_arg[1:], "r", encoding="utf-8") as f:
            sql = f.read()
    else:
        sql = sql_arg

    print(f"执行SQL: {sql}")
    print(f"数据库文件: {db_file}")

    try:
        # 执行SQL语句
        rows = run_sqls(db_file, sql, debug=args.debug, show_stats=args.stats)
        print(f"执行结果: {rows}")
        
        # 输出结果
        for r in rows:
            print(r)
            
    except SemanticError as e:
        print(f"SemanticError: {e}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
