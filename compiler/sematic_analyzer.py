from typing import Any, Dict, List, Optional, Tuple

from .parser import Select as ASTSelect, Insert as ASTInsert, CreateTable as ASTCreate, Delete as ASTDelete, AST
from execution.sytem_catalog import SystemCatalog

class SemanticError(Exception):
    pass

class Analyzed:
    def __init__(self, kind: str, payload: Dict[str, Any]) -> None:
        self.kind = kind
        self.payload = payload

class SemanticAnalyzer:
    def __init__(self, catalog: SystemCatalog) -> None:
        self.catalog = catalog

    def _ensure_table_exists(self, table: str) -> None:
        if not self.catalog.table_exists(table):
            raise SemanticError(f"table '{table}' does not exist")

    def analyze(self, ast: AST) -> Analyzed:
        if isinstance(ast, ASTCreate):
            # 重复表检查
            if self.catalog.table_exists(ast.table):
                raise SemanticError(f"table '{ast.table}' already exists")
            # 类型合法性已在语法层初步保证，这里直接通过
            return Analyzed("create_table", {"table": ast.table, "columns": ast.columns})

        if isinstance(ast, ASTInsert):
            self._ensure_table_exists(ast.table)
            schema = self.catalog.get_schema(ast.table)
            schema_cols = [c for c, _ in schema]
            if len(ast.columns) != len(ast.values):
                raise SemanticError("columns and values length mismatch")
            # 列存在与顺序检查
            for c in ast.columns:
                if c not in schema_cols:
                    raise SemanticError(f"unknown column '{c}' for table '{ast.table}'")
            # 类型检查
            row: Dict[str, Any] = {}
            for c, v in zip(ast.columns, ast.values):
                idx = schema_cols.index(c)
                typ = schema[idx][1].upper()
                if typ == "INT":
                    if not isinstance(v, int):
                        raise SemanticError(f"column '{c}' expects INT, got {type(v).__name__}")
                elif typ == "VARCHAR":
                    if not isinstance(v, str):
                        raise SemanticError(f"column '{c}' expects VARCHAR, got {type(v).__name__}")
                row[c] = v
            return Analyzed("insert", {"table": ast.table, "row": row})

        if isinstance(ast, ASTSelect):
            self._ensure_table_exists(ast.table)
            schema = self.catalog.get_schema(ast.table)
            schema_cols = [c for c, _ in schema]
            cols = ast.columns
            if cols != ["*"]:
                for c in cols:
                    if c not in schema_cols:
                        raise SemanticError(f"unknown column '{c}' for table '{ast.table}'")
            where = None
            if ast.where is not None:
                col, op, val = ast.where
                if col not in schema_cols:
                    raise SemanticError(f"unknown column '{col}' in WHERE")
                where = (col, op, val)
            return Analyzed("select", {"table": ast.table, "columns": cols, "where": where})

        if isinstance(ast, ASTDelete):
            self._ensure_table_exists(ast.table)
            where = None
            if ast.where is not None:
                schema_cols = [c for c, _ in self.catalog.get_schema(ast.table)]
                col, op, val = ast.where
                if col not in schema_cols:
                    raise SemanticError(f"unknown column '{col}' in WHERE")
                where = (col, op, val)
            return Analyzed("delete", {"table": ast.table, "where": where})

        raise SemanticError("unsupported AST")
