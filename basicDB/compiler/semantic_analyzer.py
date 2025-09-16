"""
SQL语义分析器
对抽象语法树执行语义验证，包括验证表/列是否存在、类型是否匹配等
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
    """语义分析错误"""

    def __init__(self, error_type: str, message: str, line: int, column: int):
        self.error_type = error_type
        self.message = message
        self.line = line
        self.column = column

        full_message = f"[{error_type}, line {line}:{column}, {message}]"
        super().__init__(full_message)


class SemanticAnalyzer(ASTVisitor):
    """语义分析器"""

    def __init__(self, catalog: Optional[Catalog] = None):
        self.catalog = catalog or Catalog()
        self.errors: List[SemanticError] = []
        self.current_table: Optional[str] = None

    def analyze(self, ast: SQLProgram) -> Tuple[bool, List[SemanticError]]:
        """执行语义分析"""
        self.errors.clear()

        try:
            ast.accept(self)
            return len(self.errors) == 0, self.errors
        except Exception as e:
            # 未预期的错误也转换为语义错误
            error = SemanticError("INTERNAL_ERROR", str(e), 0, 0)
            self.errors.append(error)
            return False, self.errors

    def add_error(self, error_type: str, message: str, line: int, column: int):
        """添加语义错误"""
        error = SemanticError(error_type, message, line, column)
        self.errors.append(error)

    def visit_sql_program(self, node: SQLProgram):
        """访问SQL程序节点"""
        for statement in node.statements:
            statement.accept(self)

    def visit_create_table_statement(self, node: CreateTableStatement):
        """访问CREATE TABLE语句节点"""
        # 检查表是否已存在
        if self.catalog.table_exists(node.table_name):
            self.add_error(
                "TABLE_ALREADY_EXISTS",
                f"Table '{node.table_name}' already exists",
                node.line,
                node.column,
            )
            return

        # 检查列名是否重复
        column_names = [col.name.upper() for col in node.columns]
        duplicate_columns = []
        seen = set()

        for name in column_names:
            if name in seen:
                duplicate_columns.append(name)
            else:
                seen.add(name)

        if duplicate_columns:
            self.add_error(
                "DUPLICATE_COLUMN",
                f"Duplicate column name(s): {', '.join(duplicate_columns)}",
                node.line,
                node.column,
            )
            return

        # 验证数据类型
        for col_def in node.columns:
            col_def.accept(self)

        # 如果没有错误，将表添加到catalog
        if not any(error.line == node.line for error in self.errors):
            columns = []
            for col_def in node.columns:
                col_info = ColumnInfo(
                    name=col_def.name,
                    data_type=col_def.data_type.type_name,
                    size=col_def.data_type.size,
                )
                columns.append(col_info)
            self.catalog.create_table(node.table_name, columns)

    def visit_insert_statement(self, node: InsertStatement):
        """访问INSERT语句节点"""
        # 检查表是否存在
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

        # 获取目标列信息
        target_columns = []
        if node.columns:
            # 指定了列名列表
            for col_name in node.columns:
                col_info = table_info.get_column(col_name)
                if not col_info:
                    self.add_error(
                        "COLUMN_NOT_EXISTS",
                        f"Column '{col_name}' does not exist in table '{node.table_name}'",
                        node.line,
                        node.column,
                    )
                    return
                target_columns.append(col_info)
        else:
            # 未指定列名，使用表的所有列
            target_columns = table_info.columns

        # 检查每个值行
        for i, value_row in enumerate(node.values):
            # 检查值的数量
            if len(value_row) != len(target_columns):
                self.add_error(
                    "VALUE_COUNT_MISMATCH",
                    f"Value count mismatch: expected {len(target_columns)} values, got {len(value_row)}",
                    node.line,
                    node.column,
                )
                continue

            # 检查每个值的类型兼容性
            for j, (value, target_col) in enumerate(zip(value_row, target_columns)):
                if not self.is_type_compatible(value, target_col):
                    self.add_error(
                        "TYPE_MISMATCH",
                        f"Type mismatch for column '{target_col.name}': expected {target_col.data_type}, got {self.get_literal_type(value)}",
                        value.line,
                        value.column,
                    )

        self.current_table = None

    def visit_select_statement(self, node: SelectStatement):
        """访问SELECT语句节点"""
        # 检查FROM表是否存在
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

        # 检查选择的列是否存在
        for select_item in node.select_list:
            if isinstance(select_item, Identifier):
                # 特殊处理 * 符号
                if select_item.name == "*":
                    continue  # * 表示选择所有列，跳过验证
                if not table_info.get_column(select_item.name):
                    self.add_error(
                        "COLUMN_NOT_EXISTS",
                        f"Column '{select_item.name}' does not exist in table '{node.from_table}'",
                        select_item.line,
                        select_item.column,
                    )

        # 检查WHERE条件
        if node.where_clause:
            node.where_clause.accept(self)

        # 检查ORDER BY条件
        if node.order_by_clause:
            node.order_by_clause.accept(self)

        # 检查JOIN子句
        for join_clause in node.join_clauses:
            join_clause.accept(self)

        self.current_table = None

    def visit_delete_statement(self, node: DeleteStatement):
        """访问DELETE语句节点"""
        # 检查表是否存在
        if not self.catalog.table_exists(node.table_name):
            self.add_error(
                "TABLE_NOT_EXISTS",
                f"Table '{node.table_name}' does not exist",
                node.line,
                node.column,
            )
            return

        self.current_table = node.table_name

        # 检查WHERE条件
        if node.where_clause:
            node.where_clause.accept(self)

        self.current_table = None

    def visit_update_statement(self, node: UpdateStatement):
        """访问UPDATE语句节点"""
        # 检查表是否存在
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

        # 检查列是否存在及类型匹配
        for column_name, value_expr in node.assignments:
            if not self.catalog.column_exists(node.table_name, column_name):
                self.add_error(
                    "COLUMN_NOT_EXISTS",
                    f"Column '{column_name}' does not exist in table '{node.table_name}'",
                    node.line,
                    node.column,
                )
                continue

            # 检查值表达式
            value_expr.accept(self)

            # 类型兼容性检查
            column_info = next(
                (col for col in table_info.columns if col.name == column_name), None
            )
            if column_info:
                value_type = self.get_expression_type(value_expr)
                if value_type and not self.are_types_compatible(
                    column_info.data_type, value_type
                ):
                    self.add_error(
                        "TYPE_MISMATCH",
                        f"Type mismatch for column '{column_name}': expected {column_info.data_type}, got {value_type}",
                        node.line,
                        node.column,
                    )

        # 检查WHERE条件
        if node.where_clause:
            node.where_clause.accept(self)

        self.current_table = None


    def visit_drop_table_statement(self, node: DropTableStatement):
        """验证DROP TABLE语句"""
        if not self.catalog.table_exists(node.table_name):
            self.record_error(
                "TABLE_NOT_EXISTS",
                f"表 '{node.table_name}' 不存在",
                node.line,
                node.column,
            )
            return

        # 从catalog中删除表
        self.catalog.drop_table(node.table_name)


    def visit_binary_expression(self, node: BinaryExpression):
        """访问二元表达式节点"""
        # 递归检查左右操作数
        node.left.accept(self)
        node.right.accept(self)

        # 类型兼容性检查
        left_type = self.get_expression_type(node.left)
        right_type = self.get_expression_type(node.right)

        if left_type and right_type:
            if not self.are_types_compatible(left_type, right_type):
                self.add_error(
                    "TYPE_MISMATCH",
                    f"Type mismatch in comparison: {left_type} vs {right_type}",
                    node.line,
                    node.column,
                )

    def visit_identifier(self, node: Identifier):
        """访问标识符节点"""
        if self.current_table:
            table_info = self.catalog.get_table_info(self.current_table)
            if table_info and not table_info.get_column(node.name):
                self.add_error(
                    "COLUMN_NOT_EXISTS",
                    f"Column '{node.name}' does not exist in table '{self.current_table}'",
                    node.line,
                    node.column,
                )

    def visit_literal(self, node: Literal):
        """访问字面量节点"""
        # 字面量本身不需要特殊检查
        pass

    def visit_column_def(self, node: ColumnDef):
        """访问列定义节点"""
        # 验证数据类型
        node.data_type.accept(self)

    def visit_data_type(self, node: DataType):
        """访问数据类型节点"""
        # 检查数据类型是否有效
        valid_types = {"INT", "INTEGER", "VARCHAR", "CHAR"}

        if node.type_name not in valid_types:
            self.add_error(
                "INVALID_DATA_TYPE",
                f"Invalid data type: {node.type_name}",
                node.line,
                node.column,
            )

        # 检查VARCHAR和CHAR是否指定了长度
        if node.type_name in ["VARCHAR", "CHAR"] and not node.size:
            self.add_error(
                "MISSING_SIZE",
                f"Data type {node.type_name} requires size specification",
                node.line,
                node.column,
            )
        elif node.type_name in ["VARCHAR", "CHAR"] and node.size and node.size <= 0:
            self.add_error(
                "INVALID_SIZE",
                f"Invalid size for {node.type_name}: {node.size}",
                node.line,
                node.column,
            )

    def is_type_compatible(self, literal: Literal, column: ColumnInfo) -> bool:
        """检查字面量类型与列类型是否兼容"""
        literal_type = self.get_literal_type(literal)
        column_type = column.data_type.upper()

        if literal_type == "INT" and column_type in ["INT", "INTEGER"]:
            return True
        elif literal_type == "STRING" and column_type in ["VARCHAR", "CHAR"]:
            # 检查字符串长度
            if column.size and len(str(literal.value)) > column.size:
                return False
            return True

        return False

    def get_literal_type(self, literal: Literal) -> str:
        """获取字面量的类型"""
        if isinstance(literal.value, int):
            return "INT"
        elif isinstance(literal.value, str):
            return "STRING"
        else:
            return "UNKNOWN"

    def get_expression_type(self, expr) -> Optional[str]:
        """获取表达式的类型"""
        if isinstance(expr, Literal):
            return self.get_literal_type(expr)
        elif isinstance(expr, Identifier) and self.current_table:
            col_info = self.catalog.get_column_info(self.current_table, expr.name)
            return col_info.data_type if col_info else None

        return None

    def are_types_compatible(self, type1: str, type2: str) -> bool:
        """检查两种类型是否兼容"""
        # 数值类型兼容
        numeric_types = {"INT", "INTEGER"}
        if type1 in numeric_types and type2 in numeric_types:
            return True

        # 字符串类型兼容
        string_types = {"STRING", "VARCHAR", "CHAR"}
        if type1 in string_types and type2 in string_types:
            return True

        return type1 == type2

    def visit_order_by_clause(self, node: OrderByClause):
        """访问ORDER BY子句节点"""
        # 验证每个排序表达式
        for sort_expr in node.expressions:
            sort_expr.accept(self)

    def visit_sort_expression(self, node: SortExpression):
        """访问排序表达式节点"""
        # 验证排序表达式中的列是否存在
        node.expression.accept(self)

        # 检查排序方向是否有效
        if node.direction and node.direction.upper() not in ["ASC", "DESC"]:
            self.add_error(
                "INVALID_SORT_DIRECTION",
                f"Invalid sort direction: {node.direction}",
                node.line if hasattr(node, "line") else 0,
                node.column if hasattr(node, "column") else 0,
            )

    def visit_join_clause(self, node: JoinClause):
        """访问JOIN子句节点"""
        # 检查JOIN的表是否存在
        if not self.catalog.table_exists(node.table_name):
            self.add_error(
                "TABLE_NOT_EXISTS",
                f"Table '{node.table_name}' does not exist",
                node.line,
                node.column,
            )
            return

        # 验证JOIN条件表达式
        if node.on_condition:
            # 暂时保存当前表，因为JOIN条件可能涉及多个表
            original_table = self.current_table
            node.on_condition.accept(self)
            self.current_table = original_table

    def visit_begin_statement(self, node):
        """访问BEGIN语句节点 - 事务语句不需要语义检查"""
        pass

    def visit_commit_statement(self, node):
        """访问COMMIT语句节点 - 事务语句不需要语义检查"""
        pass

    def visit_rollback_statement(self, node):
        """访问ROLLBACK语句节点 - 事务语句不需要语义检查"""
        pass

    def print_errors(self):
        """打印语义错误"""
        if not self.errors:
            print("语义分析通过，没有发现错误。")
        else:
            print("语义分析发现以下错误:")
            print("=" * 60)
            for i, error in enumerate(self.errors, 1):
                print(f"{i}. {error}")


def main():
    """测试用例"""
    from .lexer import SQLLexer
    from .parser import SQLParser

    # 测试正确的SQL
    sql_correct = """
    CREATE TABLE student(id INT, name VARCHAR(50), age INT);
    INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
    SELECT id, name FROM student WHERE age > 18;
    """

    # 测试错误的SQL
    sql_errors = """
    CREATE TABLE student(id INT, name VARCHAR(50), age INT);
    INSERT INTO student(id, name, age, score) VALUES (1, 'Alice', 20, 95);
    SELECT id, name, grade FROM student WHERE age > 'invalid';
    DELETE FROM nonexistent WHERE id = 1;
    """

    for sql_name, sql in [("正确示例", sql_correct), ("错误示例", sql_errors)]:
        print(f"\n{sql_name}:")
        print("=" * 40)
        print(sql)

        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()

            parser = SQLParser(tokens)
            ast = parser.parse()

            analyzer = SemanticAnalyzer()
            success, errors = analyzer.analyze(ast)

            if success:
                print("语义分析通过!")
            else:
                print("语义分析发现错误:")
                for error in errors:
                    print(f"  {error}")

        except Exception as e:
            print(f"分析失败: {e}")


if __name__ == "__main__":
    main()
