"""
抽象语法树节点 (AST)
该模块定义了 SQL 解析后使用的语法树结构
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional


class ASTNode(ABC):
    """AST 节点基类"""

    def __init__(self, line: int = 0, column: int = 0):
        self.line = line
        self.column = column

    @abstractmethod
    def accept(self, visitor):
        """访问者模式入口"""
        raise NotImplementedError("子类必须实现 accept 方法")

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} at ({self.line},{self.column})>"


class Statement(ASTNode):
    """SQL 语句基类"""
    pass


class Expression(ASTNode):
    """表达式基类"""
    pass


class DataType(ASTNode):
    """SQL 数据类型"""

    def __init__(self, type_name: str, size: Optional[int] = None, line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.type_name: str = type_name.upper()
        self.size: Optional[int] = size

    def accept(self, visitor):
        return visitor.visit_data_type(self)

    def __repr__(self) -> str:
        return f"DataType({self.type_name}{f'({self.size})' if self.size else ''})"


class Identifier(Expression):
    """标识符"""

    def __init__(self, name: str, line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.name = name

    def accept(self, visitor):
        return visitor.visit_identifier(self)

    def __repr__(self) -> str:
        return f"Identifier('{self.name}')"


class Literal(Expression):
    """字面量"""

    def __init__(self, value: Any, data_type: str, line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.value = value
        self.data_type = data_type

    def accept(self, visitor):
        return visitor.visit_literal(self)

    def __repr__(self) -> str:
        return f"Literal(value={self.value}, type={self.data_type})"


class ColumnDef(ASTNode):
    """列定义"""

    def __init__(self, name: str, data_type: DataType, line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.name = name
        self.data_type = data_type

    def accept(self, visitor):
        return visitor.visit_column_def(self)

    def __repr__(self) -> str:
        return f"ColumnDef({self.name}:{self.data_type})"


class BinaryExpression(Expression):
    """二元表达式"""

    def __init__(self, left: Expression, operator: str, right: Expression, line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.left = left
        self.operator = operator
        self.right = right

    def accept(self, visitor):
        return visitor.visit_binary_expression(self)

    def __repr__(self) -> str:
        return f"BinaryExpression({self.left} {self.operator} {self.right})"


class CreateTableStatement(Statement):
    """CREATE TABLE"""

    def __init__(self, table_name: str, columns: List[ColumnDef], line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.table_name = table_name
        self.columns = columns

    def accept(self, visitor):
        return visitor.visit_create_table_statement(self)

    def __repr__(self) -> str:
        return f"CreateTableStatement(table={self.table_name}, cols={len(self.columns)})"


class InsertStatement(Statement):
    """INSERT"""

    def __init__(self, table_name: str, columns: Optional[List[str]], values: List[List[Expression]], line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def accept(self, visitor):
        return visitor.visit_insert_statement(self)

    def __repr__(self) -> str:
        return f"InsertStatement(table={self.table_name}, columns={self.columns}, values={self.values})"


class SelectStatement(Statement):
    """SELECT"""

    def __init__(
        self,
        select_list: List[Expression],
        from_table: str,
        where_clause: Optional[Expression] = None,
        order_by_clause: Optional["OrderByClause"] = None,
        join_clauses: Optional[List["JoinClause"]] = None,
        line: int = 0,
        column: int = 0,
    ):
        super().__init__(line, column)
        self.select_list = select_list
        self.from_table = from_table
        self.where_clause = where_clause
        self.order_by_clause = order_by_clause
        self.join_clauses = join_clauses or []

    def accept(self, visitor):
        return visitor.visit_select_statement(self)

    def __repr__(self) -> str:
        return f"SelectStatement({self.select_list} FROM {self.from_table})"


class DeleteStatement(Statement):
    """DELETE"""

    def __init__(self, table_name: str, where_clause: Optional[Expression] = None, line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.table_name = table_name
        self.where_clause = where_clause

    def accept(self, visitor):
        return visitor.visit_delete_statement(self)

    def __repr__(self) -> str:
        return f"DeleteStatement({self.table_name}, where={self.where_clause})"


class UpdateStatement(Statement):
    """UPDATE"""

    def __init__(self, table_name: str, assignments: List[tuple], where_clause: Optional[Expression] = None, line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.table_name = table_name
        self.assignments = assignments
        self.where_clause = where_clause

    def accept(self, visitor):
        return visitor.visit_update_statement(self)

    def __repr__(self) -> str:
        return f"UpdateStatement({self.table_name}, assigns={self.assignments}, where={self.where_clause})"


class SQLProgram(ASTNode):
    """SQL 程序"""

    def __init__(self, statements: List[Statement], line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.statements = statements

    def accept(self, visitor):
        return visitor.visit_sql_program(self)

    def __repr__(self) -> str:
        return f"SQLProgram({len(self.statements)} statements)"


class JoinType:
    """JOIN 类型"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class JoinClause(ASTNode):
    """JOIN 子句"""

    def __init__(self, join_type: str, table_name: str, on_condition: Expression, line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.join_type = join_type
        self.table_name = table_name
        self.on_condition = on_condition

    def accept(self, visitor):
        return visitor.visit_join_clause(self)

    def __repr__(self) -> str:
        return f"JoinClause({self.join_type} {self.table_name} ON {self.on_condition})"


class OrderByClause(ASTNode):
    """ORDER BY"""

    def __init__(self, expressions: List["SortExpression"], line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.expressions = expressions

    def accept(self, visitor):
        return visitor.visit_order_by_clause(self)

    def __repr__(self) -> str:
        return f"OrderByClause({self.expressions})"


class SortExpression(ASTNode):
    """排序项"""

    def __init__(self, expression: Expression, direction: str = "ASC", line: int = 0, column: int = 0):
        super().__init__(line, column)
        self.expression = expression
        self.direction = direction

    def accept(self, visitor):
        return visitor.visit_sort_expression(self)

    def __repr__(self) -> str:
        return f"SortExpression({self.expression}, {self.direction})"


class ASTVisitor(ABC):
    """访问者接口"""

    @abstractmethod
    def visit_data_type(self, node: DataType): ...
    @abstractmethod
    def visit_identifier(self, node: Identifier): ...
    @abstractmethod
    def visit_literal(self, node: Literal): ...
    @abstractmethod
    def visit_column_def(self, node: ColumnDef): ...
    @abstractmethod
    def visit_binary_expression(self, node: BinaryExpression): ...
    @abstractmethod
    def visit_create_table_statement(self, node: CreateTableStatement): ...
    @abstractmethod
    def visit_insert_statement(self, node: InsertStatement): ...
    @abstractmethod
    def visit_select_statement(self, node: SelectStatement): ...
    @abstractmethod
    def visit_join_clause(self, node: JoinClause): ...
    @abstractmethod
    def visit_delete_statement(self, node: DeleteStatement): ...
    @abstractmethod
    def visit_update_statement(self, node: UpdateStatement): ...
    @abstractmethod
    def visit_sql_program(self, node: SQLProgram): ...
    @abstractmethod
    def visit_order_by_clause(self, node: OrderByClause): ...
    @abstractmethod
    def visit_sort_expression(self, node: SortExpression): ...
