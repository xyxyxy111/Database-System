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
    if isinstance(ast, Select):
        return {"type": "Select", "columns": ast.columns, "table": ast.table, "where": ast.where}
    if isinstance(ast, Insert):
        return {"type": "Insert", "table": ast.table, "columns": ast.columns, "values": ast.values}
    if isinstance(ast, CreateTable):
        return {"type": "CreateTable", "table": ast.table, "columns": ast.columns}
    if isinstance(ast, Delete):
        return {"type": "Delete", "table": ast.table, "where": ast.where}
    return {"type": type(ast).__name__}


def analyzed_to_dict(a: Analyzed) -> Dict[str, Any]:
    return {"kind": a.kind, "payload": a.payload}


def op_summary(op: Operator) -> str:
    # 简要展示根算子类型；如需更详细，可在各算子上实现 explain()。
    return op.__class__.__name__


def run_sqls(db_file: str, sql: str, debug: bool = False, show_stats: bool = False) -> List[Dict[str, Any]]:
    syscat = SystemCatalog(db_file)
    executor = Executor(syscat)
    analyzer = SemanticAnalyzer(syscat)

    if debug:
        toks = tokenize(sql)
        print("[Tokens]")
        for t in toks:
            print(t)

    parser = Parser(sql)
    asts = parser.parse_many()

    if debug:
        print("[AST]")
        for ast in asts:
            print(ast_to_dict(ast))

    rows: List[Dict[str, Any]] = []
    for ast in asts:
        analyzed = analyzer.analyze(ast)
        if debug:
            print("[Analyzed]")
            print(analyzed_to_dict(analyzed))
        planner = Planner(executor)
        op = planner.plan(analyzed)
        if debug:
            print("[PlanRoot]", op_summary(op))
        res = executor.execute_plan(op)
        rows.extend(res)

    if show_stats:
        hits, misses, evictions = syscat.buffer.stats()
        print(f"[BufferStats] hits={hits}, misses={misses}, evictions={evictions}")

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Mini DB System")
    parser.add_argument("db_file", help="database file path")
    parser.add_argument("sql", nargs=argparse.REMAINDER, help="SQL string or @file.sql")
    parser.add_argument("--debug-pipeline", dest="debug", action="store_true", help="print tokens/AST/analyzed/plan")
    parser.add_argument("--stats", dest="stats", action="store_true", help="print buffer manager stats")
    args = parser.parse_args()

    if not args.sql:
        print("Usage: python main.py <db_file> <SQL or @file.sql> [--debug-pipeline] [--stats]")
        return

    db_file = args.db_file
    sql_arg = " ".join(args.sql)
    if sql_arg.startswith("@"):
        with open(sql_arg[1:], "r", encoding="utf-8") as f:
            sql = f.read()
    else:
        sql = sql_arg

    try:
        rows = run_sqls(db_file, sql, debug=args.debug, show_stats=args.stats)
        for r in rows:
            print(r)
    except SemanticError as e:
        print(f"SemanticError: {e}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
