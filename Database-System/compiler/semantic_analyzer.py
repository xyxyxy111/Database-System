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


class SemanticException(Exception):
    """语义分析过程中出现的异常"""

    def __init__(self, error_type: str, msg: str, line_num: int, col_num: int):
        self.error_type = error_type
        self.msg = msg
        self.line_num = line_num
        self.col_num = col_num

        full_msg = f"[{error_type}, line {line_num}:{col_num}, {msg}]"
        super().__init__(full_msg)


class SemanticAnalyzer(ASTVisitor):
    """执行SQL语义分析"""

    def __init__(self, catalog: Optional[Catalog] = None):
        self.catalog = catalog or Catalog()
        self.error_list: List[SemanticException] = []
        self.cur_table: Optional[str] = None

    def analyze(self, ast: SQLProgram) -> Tuple[bool, List[SemanticException]]:
        """对AST进行语义验证"""
        self.error_list.clear()

        try:
            ast.accept(self)
            return len(self.error_list) == 0, self.error_list
        except Exception as e:
            # 捕获未预期异常并转换为语义错误
            err = SemanticException("INTERNAL_ERROR", str(e), 0, 0)
            self.error_list.append(err)
            return False, self.error_list

    def record_error(self, err_type: str, msg: str, line_num: int, col_num: int):
        """记录语义错误"""
        err = SemanticException(err_type, msg, line_num, col_num)
        self.error_list.append(err)

    def visit_sql_program(self, node: SQLProgram):
        """处理SQL程序节点"""
        for stmt in node.statements:
            stmt.accept(self)

    def visit_create_table_statement(self, node: CreateTableStatement):
        """验证CREATE TABLE语句"""
        # 验证表名是否已存在
        if self.catalog.table_exists(node.table_name):
            self.record_error(
                "TABLE_ALREADY_EXISTS",
                f"表 '{node.table_name}' 已存在",
                node.line,
                node.column,
            )
            return

        # 检查列名重复
        col_names = [col.name.upper() for col in node.columns]
        dup_cols = []
        visited = set()

        for name in col_names:
            if name in visited:
                dup_cols.append(name)
            else:
                visited.add(name)

        if dup_cols:
            self.record_error(
                "DUPLICATE_COLUMN",
                f"重复的列名: {', '.join(dup_cols)}",
                node.line,
                node.column,
            )
            return

        # 验证各列定义
        for col_def in node.columns:
            col_def.accept(self)

        # 若无错误则添加到目录
        if not any(err.line_num == node.line for err in self.error_list):
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
        """验证INSERT语句"""
        # 确认表存在
        if not self.catalog.table_exists(node.table_name):
            self.record_error(
                "TABLE_NOT_EXISTS",
                f"表 '{node.table_name}' 不存在",
                node.line,
                node.column,
            )
            return

        tbl_info = self.catalog.get_table_info(node.table_name)
        self.cur_table = node.table_name

        # 获取目标列信息
        target_cols = []
        if node.columns:
            # 验证指定列名
            for col_name in node.columns:
                col_info = tbl_info.get_column(col_name)
                if not col_info:
                    self.record_error(
                        "COLUMN_NOT_EXISTS",
                        f"表 '{node.table_name}' 中不存在列 '{col_name}'",
                        node.line,
                        node.column,
                    )
                    return
                target_cols.append(col_info)
        else:
            # 使用全部列
            target_cols = tbl_info.columns

        # 检查每个值行
        for idx, value_row in enumerate(node.values):
            # 验证值数量匹配
            if len(value_row) != len(target_cols):
                self.record_error(
                    "VALUE_COUNT_MISMATCH",
                    f"值数量不匹配: 需要 {len(target_cols)} 个值，实际为 {len(value_row)}",
                    node.line,
                    node.column,
                )
                continue

            # 检查类型兼容性
            for j, (val, target_col) in enumerate(zip(value_row, target_cols)):
                if not self.check_type_compatibility(val, target_col):
                    self.record_error(
                        "TYPE_MISMATCH",
                        f"列 '{target_col.name}' 类型不匹配: 需要 {target_col.data_type}，实际为 {self.get_literal_type(val)}",
                        val.line,
                        val.column,
                    )

        self.cur_table = None

    def visit_select_statement(self, node: SelectStatement):
        """验证SELECT语句"""
        # 验证FROM表存在
        if not self.catalog.table_exists(node.from_table):
            self.record_error(
                "TABLE_NOT_EXISTS",
                f"表 '{node.from_table}' 不存在",
                node.line,
                node.column,
            )
            return

        self.cur_table = node.from_table
        tbl_info = self.catalog.get_table_info(node.from_table)

        # 验证选择列存在
        for item in node.select_list:
            if isinstance(item, Identifier):
                if item.name == "*":
                    continue  # 通配符跳过验证
                if not tbl_info.get_column(item.name):
                    self.record_error(
                        "COLUMN_NOT_EXISTS",
                        f"表 '{node.from_table}' 中不存在列 '{item.name}'",
                        item.line,
                        item.column,
                    )

        # 验证WHERE条件
        if node.where_clause:
            node.where_clause.accept(self)

        # 验证ORDER BY
        if node.order_by_clause:
            node.order_by_clause.accept(self)

        # 验证JOIN子句
        for join in node.join_clauses:
            join.accept(self)

        self.cur_table = None

    def visit_delete_statement(self, node: DeleteStatement):
        """验证DELETE语句"""
        if not self.catalog.table_exists(node.table_name):
            self.record_error(
                "TABLE_NOT_EXISTS",
                f"表 '{node.table_name}' 不存在",
                node.line,
                node.column,
            )
            return

        self.cur_table = node.table_name

        if node.where_clause:
            node.where_clause.accept(self)

        self.cur_table = None

    def visit_update_statement(self, node: UpdateStatement):
        """验证UPDATE语句"""
        if not self.catalog.table_exists(node.table_name):
            self.record_error(
                "TABLE_NOT_EXISTS",
                f"表 '{node.table_name}' 不存在",
                node.line,
                node.column,
            )
            return

        self.cur_table = node.table_name
        tbl_info = self.catalog.get_table_info(node.table_name)

        for col_name, val_expr in node.assignments:
            if not self.catalog.column_exists(node.table_name, col_name):
                self.record_error(
                    "COLUMN_NOT_EXISTS",
                    f"表 '{node.table_name}' 中不存在列 '{col_name}'",
                    node.line,
                    node.column,
                )
                continue

            val_expr.accept(self)

            col_info = next(
                (col for col in tbl_info.columns if col.name == col_name), None
            )
            if col_info:
                val_type = self.get_expr_type(val_expr)
                if val_type and not self.check_types_compatible(
                    col_info.data_type, val_type
                ):
                    self.record_error(
                        "TYPE_MISMATCH",
                        f"列 '{col_name}' 类型不匹配: 需要 {col_info.data_type}，实际为 {val_type}",
                        node.line,
                        node.column,
                    )

        if node.where_clause:
            node.where_clause.accept(self)

        self.cur_table = None

    def visit_binary_expression(self, node: BinaryExpression):
        """验证二元表达式"""
        node.left.accept(self)
        node.right.accept(self)

        left_type = self.get_expr_type(node.left)
        right_type = self.get_expr_type(node.right)

        if left_type and right_type:
            if not self.check_types_compatible(left_type, right_type):
                self.record_error(
                    "TYPE_MISMATCH",
                    f"比较操作类型不匹配: {left_type} 与 {right_type}",
                    node.line,
                    node.column,
                )

    def visit_identifier(self, node: Identifier):
        """验证标识符"""
        if self.cur_table:
            tbl_info = self.catalog.get_table_info(self.cur_table)
            if tbl_info and not tbl_info.get_column(node.name):
                self.record_error(
                    "COLUMN_NOT_EXISTS",
                    f"表 '{self.cur_table}' 中不存在列 '{node.name}'",
                    node.line,
                    node.column,
                )

    def visit_literal(self, node: Literal):
        """验证字面量"""
        pass

    def visit_column_def(self, node: ColumnDef):
        """验证列定义"""
        node.data_type.accept(self)

    def visit_data_type(self, node: DataType):
        """验证数据类型"""
        valid_types = {"INT", "INTEGER", "VARCHAR", "CHAR"}

        if node.type_name not in valid_types:
            self.record_error(
                "INVALID_DATA_TYPE",
                f"无效数据类型: {node.type_name}",
                node.line,
                node.column,
            )

        if node.type_name in ["VARCHAR", "CHAR"] and not node.size:
            self.record_error(
                "MISSING_SIZE",
                f"{node.type_name} 类型需要指定大小",
                node.line,
                node.column,
            )
        elif node.type_name in ["VARCHAR", "CHAR"] and node.size and node.size <= 0:
            self.record_error(
                "INVALID_SIZE",
                f"{node.type_name} 类型大小无效: {node.size}",
                node.line,
                node.column,
            )

    def check_type_compatibility(self, literal: Literal, column: ColumnInfo) -> bool:
        """检查字面量与列类型兼容性"""
        literal_type = self.get_literal_type(literal)
        col_type = column.data_type.upper()

        if literal_type == "INT" and col_type in ["INT", "INTEGER"]:
            return True
        elif literal_type == "STRING" and col_type in ["VARCHAR", "CHAR"]:
            if column.size and len(str(literal.value)) > column.size:
                return False
            return True

        return False

    def get_literal_type(self, literal: Literal) -> str:
        """获取字面量类型"""
        if isinstance(literal.value, int):
            return "INT"
        elif isinstance(literal.value, str):
            return "STRING"
        else:
            return "UNKNOWN"

    def get_expr_type(self, expr) -> Optional[str]:
        """获取表达式类型"""
        if isinstance(expr, Literal):
            return self.get_literal_type(expr)
        elif isinstance(expr, Identifier) and self.cur_table:
            col_info = self.catalog.get_column_info(self.cur_table, expr.name)
            return col_info.data_type if col_info else None

        return None

    def check_types_compatible(self, type1: str, type2: str) -> bool:
        """检查类型兼容性"""
        numeric_types = {"INT", "INTEGER"}
        if type1 in numeric_types and type2 in numeric_types:
            return True

        string_types = {"STRING", "VARCHAR", "CHAR"}
        if type1 in string_types and type2 in string_types:
            return True

        return type1 == type2

    def visit_order_by_clause(self, node: OrderByClause):
        """验证ORDER BY子句"""
        for sort_expr in node.expressions:
            sort_expr.accept(self)

    def visit_sort_expression(self, node: SortExpression):
        """验证排序表达式"""
        node.expression.accept(self)

        if node.direction and node.direction.upper() not in ["ASC", "DESC"]:
            self.record_error(
                "INVALID_SORT_DIRECTION",
                f"无效排序方向: {node.direction}",
                node.line if hasattr(node, "line") else 0,
                node.column if hasattr(node, "column") else 0,
            )

    def visit_join_clause(self, node: JoinClause):
        """验证JOIN子句"""
        if not self.catalog.table_exists(node.table_name):
            self.record_error(
                "TABLE_NOT_EXISTS",
                f"表 '{node.table_name}' 不存在",
                node.line,
                node.column,
            )
            return

        if node.on_condition:
            orig_table = self.cur_table
            node.on_condition.accept(self)
            self.cur_table = orig_table

    def print_errors(self):
        """输出语义错误"""
        if not self.error_list:
            print("语义验证通过。")
        else:
            print("发现语义错误:")
            print("=" * 50)
            for i, err in enumerate(self.error_list, 1):
                print(f"{i}. {err}")


def main():
    """测试功能"""
    from .lexer import SQLLexer
    from .parser import SQLParser

    correct_sql = """
    CREATE TABLE student(id INT, name VARCHAR(50), age INT);
    INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
    SELECT id, name FROM student WHERE age > 18;
    """

    error_sql = """
    CREATE TABLE student(id INT, name VARCHAR(50), age INT);
    INSERT INTO student(id, name, age, score) VALUES (1, 'Alice', 20, 95);
    SELECT id, name, grade FROM student WHERE age > 'invalid';
    DELETE FROM nonexistent WHERE id = 1;
    """

    for test_name, sql_text in [("正确示例", correct_sql), ("错误示例", error_sql)]:
        print(f"\n{test_name}:")
        print("=" * 35)
        print(sql_text)

        try:
            lexer = SQLLexer(sql_text)
            tokens = lexer.tokenize()

            parser = SQLParser(tokens)
            ast = parser.parse()

            analyzer = SemanticAnalyzer()
            success, errors = analyzer.analyze(ast)

            if success:
                print("语义验证成功!")
            else:
                print("语义验证发现错误:")
                for err in errors:
                    print(f"  {err}")

        except Exception as e:
            print(f"分析过程出错: {e}")


if __name__ == "__main__":
    main()