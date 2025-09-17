"""
SQL 语义分析组件
主要功能：对解析生成的抽象语法树(AST)进行合法性验证，包括表、字段是否存在及数据类型校验。
"""

from typing import Any, Dict, List, Optional, Tuple

from .ast_nodes import (
    ASTVisitor,
    BinaryExpression,
    ColumnDef,
    CreateTableStatement,
    DataType,
    DeleteStatement,
    DropTableStatement,
    Identifier,
    InsertStatement,
    JoinClause,
    Literal,
    OrderByClause,
    SelectStatement,
    SortExpression,
    SQLProgram,
    UpdateStatement,
)
from .catalog import Catalog, ColumnInfo


class SemanticError(Exception):
    """语义错误封装"""

    def __init__(self, error_type: str, message: str, line: int, column: int):
        self.error_type: str = error_type
        self.message: str = message
        self.line: int = line
        self.column: int = column
        super().__init__(f"[{error_type}, line {line}:{column}, {message}]")


class SemanticAnalyzer(ASTVisitor):
    """语义分析器：对 AST 节点进行检查"""

    def __init__(self, catalog: Optional[Catalog] = None):
        self.catalog: Catalog = catalog or Catalog()
        self.errors: List[SemanticError] = []
        self.current_table: Optional[str] = None

    def analyze(self, ast: SQLProgram) -> Tuple[bool, List[SemanticError]]:
        """入口：执行语义验证"""
        self.errors.clear()
        try:
            ast.accept(self)
            return len(self.errors) == 0, self.errors
        except Exception as e:
            # 未捕获异常包装成语义错误
            self.errors.append(SemanticError("INTERNAL_ERROR", str(e), 0, 0))
            return False, self.errors

    def add_error(self, error_type: str, message: str, line: int, column: int):
        """记录错误"""
        self.errors.append(SemanticError(error_type, message, line, column))

    # ----------------- 语句级节点 -----------------

    def visit_sql_program(self, node: SQLProgram):
        """遍历 SQL 程序中的每条语句"""
        for stmt in node.statements:
            stmt.accept(self)

    def visit_create_table_statement(self, node: CreateTableStatement):
        """处理 CREATE TABLE"""
        if self.catalog.table_exists(node.table_name):
            self.add_error(
                "TABLE_ALREADY_EXISTS",
                f"Table '{node.table_name}' already exists",
                node.line,
                node.column,
            )
            return

        col_names = [c.name.upper() for c in node.columns]
        duplicates = {name for name in col_names if col_names.count(name) > 1}
        if duplicates:
            self.add_error(
                "DUPLICATE_COLUMN",
                f"Duplicate column(s): {', '.join(duplicates)}",
                node.line,
                node.column,
            )
            return

        for col in node.columns:
            col.accept(self)

        if not any(err.line == node.line for err in self.errors):
            cols: List[ColumnInfo] = [
                ColumnInfo(name=c.name, data_type=c.data_type.type_name, size=c.data_type.size)
                for c in node.columns
            ]
            self.catalog.create_table(node.table_name, cols)

    def visit_insert_statement(self, node: InsertStatement):
        """处理 INSERT"""
        if not self.catalog.table_exists(node.table_name):
            self.add_error(
                "TABLE_NOT_EXISTS",
                f"Table '{node.table_name}' does not exist",
                node.line,
                node.column,
            )
            return

        table_info = self.catalog.get_table_info(node.table_name)
        self.current_table = node.table_name

        target_cols: List[ColumnInfo]
        if node.columns:
            target_cols = []
            for col in node.columns:
                col_info = table_info.get_column(col)
                if not col_info:
                    self.add_error(
                        "COLUMN_NOT_EXISTS",
                        f"Column '{col}' not found in table '{node.table_name}'",
                        node.line,
                        node.column,
                    )
                    return
                target_cols.append(col_info)
        else:
            target_cols = table_info.columns

        for value_row in node.values:
            if len(value_row) != len(target_cols):
                self.add_error(
                    "VALUE_COUNT_MISMATCH",
                    f"Expected {len(target_cols)} values, got {len(value_row)}",
                    node.line,
                    node.column,
                )
                continue
            for val, col in zip(value_row, target_cols):
                if not self.is_type_compatible(val, col):
                    self.add_error(
                        "TYPE_MISMATCH",
                        f"Incompatible type for column '{col.name}': {col.data_type}",
                        val.line,
                        val.column,
                    )

        self.current_table = None

    def visit_select_statement(self, node: SelectStatement):
        """处理 SELECT"""
        if not self.catalog.table_exists(node.from_table):
            self.add_error(
                "TABLE_NOT_EXISTS",
                f"Table '{node.from_table}' does not exist",
                node.line,
                node.column,
            )
            return

        self.current_table = node.from_table
        table_info = self.catalog.get_table_info(node.from_table)

        for item in node.select_list:
            if isinstance(item, Identifier) and item.name != "*":
                if not table_info.get_column(item.name):
                    self.add_error(
                        "COLUMN_NOT_EXISTS",
                        f"Column '{item.name}' not found in '{node.from_table}'",
                        item.line,
                        item.column,
                    )

        if node.where_clause:
            node.where_clause.accept(self)
        if node.order_by_clause:
            node.order_by_clause.accept(self)
        for join in node.join_clauses:
            join.accept(self)

        self.current_table = None

    def visit_delete_statement(self, node: DeleteStatement):
        """处理 DELETE"""
        if not self.catalog.table_exists(node.table_name):
            self.add_error(
                "TABLE_NOT_EXISTS",
                f"Table '{node.table_name}' does not exist",
                node.line,
                node.column,
            )
            return

        self.current_table = node.table_name
        if node.where_clause:
            node.where_clause.accept(self)
        self.current_table = None

    def visit_update_statement(self, node: UpdateStatement):
        """处理 UPDATE"""
        if not self.catalog.table_exists(node.table_name):
            self.add_error(
                "TABLE_NOT_EXISTS",
                f"Table '{node.table_name}' does not exist",
                node.line,
                node.column,
            )
            return

        self.current_table = node.table_name
        table_info = self.catalog.get_table_info(node.table_name)

        for col, expr in node.assignments:
            if not self.catalog.column_exists(node.table_name, col):
                self.add_error(
                    "COLUMN_NOT_EXISTS",
                    f"Column '{col}' not found in '{node.table_name}'",
                    node.line,
                    node.column,
                )
                continue

            expr.accept(self)
            col_info = next((c for c in table_info.columns if c.name == col), None)
            if col_info:
                val_type = self.get_expression_type(expr)
                if val_type and not self.are_types_compatible(col_info.data_type, val_type):
                    self.add_error(
                        "TYPE_MISMATCH",
                        f"Column '{col}' expects {col_info.data_type}, got {val_type}",
                        node.line,
                        node.column,
                    )

        if node.where_clause:
            node.where_clause.accept(self)

        self.current_table = None

    def visit_drop_table_statement(self, node: DropTableStatement):
        """处理 DROP TABLE"""
        if not self.catalog.table_exists(node.table_name):
            self.add_error(
                "TABLE_NOT_EXISTS",
                f"Table '{node.table_name}' does not exist",
                node.line,
                node.column,
            )
            return
        self.catalog.drop_table(node.table_name)

    # ----------------- 表达式节点 -----------------

    def visit_binary_expression(self, node: BinaryExpression):
        """处理二元表达式"""
        node.left.accept(self)
        node.right.accept(self)
        l_type, r_type = self.get_expression_type(node.left), self.get_expression_type(node.right)
        if l_type and r_type and not self.are_types_compatible(l_type, r_type):
            self.add_error(
                "TYPE_MISMATCH",
                f"Type mismatch: {l_type} vs {r_type}",
                node.line,
                node.column,
            )

    def visit_identifier(self, node: Identifier):
        """处理标识符"""
        if self.current_table:
            table_info = self.catalog.get_table_info(self.current_table)
            if table_info and not table_info.get_column(node.name):
                self.add_error(
                    "COLUMN_NOT_EXISTS",
                    f"Column '{node.name}' not found in table '{self.current_table}'",
                    node.line,
                    node.column,
                )

    def visit_literal(self, node: Literal):
        """字面量无需额外检查"""
        pass

    def visit_column_def(self, node: ColumnDef):
        """处理列定义"""
        node.data_type.accept(self)

    def visit_data_type(self, node: DataType):
        """处理数据类型定义"""
        valid_types = {"INT", "INTEGER", "VARCHAR", "CHAR"}
        if node.type_name not in valid_types:
            self.add_error("INVALID_DATA_TYPE", f"Invalid type: {node.type_name}", node.line, node.column)
        if node.type_name in {"VARCHAR", "CHAR"}:
            if not node.size or node.size <= 0:
                self.add_error("INVALID_SIZE", f"Invalid size for {node.type_name}", node.line, node.column)

    # ----------------- 其他节点 -----------------

    def visit_order_by_clause(self, node: OrderByClause):
        """处理 ORDER BY"""
        for expr in node.expressions:
            expr.accept(self)

    def visit_sort_expression(self, node: SortExpression):
        """处理排序表达式"""
        node.expression.accept(self)
        if node.direction and node.direction.upper() not in {"ASC", "DESC"}:
            self.add_error("INVALID_SORT_DIRECTION", f"Bad sort direction: {node.direction}", node.line, node.column)

    def visit_join_clause(self, node: JoinClause):
        """处理 JOIN"""
        if not self.catalog.table_exists(node.table_name):
            self.add_error("TABLE_NOT_EXISTS", f"Table '{node.table_name}' not found", node.line, node.column)
            return
        if node.on_condition:
            prev = self.current_table
            node.on_condition.accept(self)
            self.current_table = prev

    def visit_begin_statement(self, node): pass
    def visit_commit_statement(self, node): pass
    def visit_rollback_statement(self, node): pass

    # ----------------- 工具函数 -----------------

    def is_type_compatible(self, literal: Literal, column: ColumnInfo) -> bool:
        """检查字面量是否可赋值到列"""
        lit_type = self.get_literal_type(literal)
        col_type = column.data_type.upper()
        if lit_type == "INT" and col_type in {"INT", "INTEGER"}:
            return True
        if lit_type == "STRING" and col_type in {"VARCHAR", "CHAR"}:
            return not (column.size and len(str(literal.value)) > column.size)
        return False

    def get_literal_type(self, literal: Literal) -> str:
        """推断字面量类型"""
        return "INT" if isinstance(literal.value, int) else "STRING" if isinstance(literal.value, str) else "UNKNOWN"

    def get_expression_type(self, expr) -> Optional[str]:
        """推断表达式类型"""
        if isinstance(expr, Literal):
            return self.get_literal_type(expr)
        if isinstance(expr, Identifier) and self.current_table:
            col_info = self.catalog.get_column_info(self.current_table, expr.name)
            return col_info.data_type if col_info else None
        return None

    def are_types_compatible(self, type1: str, type2: str) -> bool:
        """判断两种类型是否兼容"""
        return (
            (type1 in {"INT", "INTEGER"} and type2 in {"INT", "INTEGER"}) or
            (type1 in {"STRING", "VARCHAR", "CHAR"} and type2 in {"STRING", "VARCHAR", "CHAR"}) or
            type1 == type2
        )

    def print_errors(self):
        """输出错误列表"""
        if not self.errors:
            print("语义分析成功，无错误。")
        else:
            print("语义分析检测到以下问题：")
            for idx, err in enumerate(self.errors, 1):
                print(f"{idx}. {err}")
