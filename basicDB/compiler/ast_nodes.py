"""
抽象语法树 (AST) 定义模块
此文件用于描述 SQL 语句解析后的树形节点结构。
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional


class ASTNode(ABC):
    """所有语法树节点的基类"""

    def __init__(self, line: int = 0, column: int = 0) -> None:
        self.line: int = line
        self.column: int = column

    @abstractmethod
    def accept(self, visitor) -> Any:
        """访问者模式接口，由具体节点实现"""
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


class Statement(ASTNode):
    """SQL 语句节点抽象基类"""
    pass


class Expression(ASTNode):
    """表达式基类，例如字面量、运算式、函数调用等"""
    pass


class DataType(ASTNode):
    """SQL 数据类型定义"""

    def __init__(
        self, type_name: str, size: Optional[int] = None, line: int = 0, column: int = 0
    ) -> None:
        super().__init__(line, column)
        self.type_name: str = type_name.upper()
        self.size: Optional[int] = size

    def accept(self, visitor) -> Any:
        return visitor.visit_data_type(self)

    def __repr__(self) -> str:
        return f"DataType({self.type_name}({self.size}))" if self.size else f"DataType({self.type_name})"


class Identifier(Expression):
    """标识符节点，例如列名或表名"""

    def __init__(self, name: str, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.name: str = name

    def accept(self, visitor) -> Any:
        return visitor.visit_identifier(self)

    def __repr__(self) -> str:
        return f"Identifier({self.name})"


class Literal(Expression):
    """常量值节点"""

    def __init__(self, value: Any, data_type: str, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.value: Any = value
        self.data_type: str = data_type

    def accept(self, visitor) -> Any:
        return visitor.visit_literal(self)

    def __repr__(self) -> str:
        return f"Literal({self.value}, {self.data_type})"


class ColumnDef(ASTNode):
    """列的定义，包括列名与数据类型"""

    def __init__(self, name: str, data_type: DataType, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.name: str = name
        self.data_type: DataType = data_type

    def accept(self, visitor) -> Any:
        return visitor.visit_column_def(self)

    def __repr__(self) -> str:
        return f"ColumnDef({self.name}, {self.data_type})"


class BinaryExpression(Expression):
    """二元运算节点，例如 a > b"""

    def __init__(
        self,
        left: Expression,
        operator: str,
        right: Expression,
        line: int = 0,
        column: int = 0,
    ) -> None:
        super().__init__(line, column)
        self.left: Expression = left
        self.operator: str = operator
        self.right: Expression = right

    def accept(self, visitor) -> Any:
        return visitor.visit_binary_expression(self)

    def __repr__(self) -> str:
        return f"BinaryExpression({self.left} {self.operator} {self.right})"


class AggregateFunction(Expression):
    """聚合函数节点 (COUNT, SUM 等)"""

    def __init__(
        self,
        function_name: str,
        argument: Expression,
        distinct: bool = False,
        line: int = 0,
        column: int = 0,
    ) -> None:
        super().__init__(line, column)
        self.function_name: str = function_name.upper()
        self.argument: Expression = argument
        self.distinct: bool = distinct

    def accept(self, visitor) -> Any:
        return visitor.visit_aggregate_function(self)

    def __repr__(self) -> str:
        flag = "DISTINCT " if self.distinct else ""
        return f"AggregateFunction({self.function_name}({flag}{self.argument}))"


class FunctionCall(Expression):
    """通用函数调用节点"""

    def __init__(
        self,
        function_name: str,
        arguments: List[Expression],
        line: int = 0,
        column: int = 0,
    ) -> None:
        super().__init__(line, column)
        self.function_name: str = function_name.upper()
        self.arguments: List[Expression] = arguments

    def accept(self, visitor) -> Any:
        return visitor.visit_function_call(self)

    def __repr__(self) -> str:
        args = ", ".join(map(str, self.arguments))
        return f"FunctionCall({self.function_name}({args}))"


class CreateTableStatement(Statement):
    """CREATE TABLE 语句"""

    def __init__(self, table_name: str, columns: List[ColumnDef], line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.table_name: str = table_name
        self.columns: List[ColumnDef] = columns

    def accept(self, visitor) -> Any:
        return visitor.visit_create_table_statement(self)

    def __repr__(self) -> str:
        return f"CreateTableStatement({self.table_name}, {self.columns})"


class InsertStatement(Statement):
    """INSERT 语句"""

    def __init__(
        self,
        table_name: str,
        columns: Optional[List[str]],
        values: List[List[Expression]],
        line: int = 0,
        column: int = 0,
    ) -> None:
        super().__init__(line, column)
        self.table_name: str = table_name
        self.columns: Optional[List[str]] = columns
        self.values: List[List[Expression]] = values

    def accept(self, visitor) -> Any:
        return visitor.visit_insert_statement(self)

    def __repr__(self) -> str:
        return f"InsertStatement({self.table_name}, {self.columns}, {self.values})"


class SelectStatement(Statement):
    """SELECT 查询语句"""

    def __init__(
        self,
        select_list: List[Expression],
        from_table: str,
        where_clause: Optional[Expression] = None,
        order_by_clause: Optional["OrderByClause"] = None,
        join_clauses: Optional[List["JoinClause"]] = None,
        line: int = 0,
        column: int = 0,
    ) -> None:
        super().__init__(line, column)
        self.select_list: List[Expression] = select_list
        self.from_table: str = from_table
        self.where_clause: Optional[Expression] = where_clause
        self.order_by_clause: Optional["OrderByClause"] = order_by_clause
        self.join_clauses: List["JoinClause"] = join_clauses or []

    def accept(self, visitor) -> Any:
        return visitor.visit_select_statement(self)

    def __repr__(self) -> str:
        return f"SelectStatement({self.select_list}, {self.from_table}, {self.where_clause}, {self.order_by_clause})"


class DeleteStatement(Statement):
    """DELETE 语句"""

    def __init__(self, table_name: str, where_clause: Optional[Expression] = None, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.table_name: str = table_name
        self.where_clause: Optional[Expression] = where_clause

    def accept(self, visitor) -> Any:
        return visitor.visit_delete_statement(self)

    def __repr__(self) -> str:
        return f"DeleteStatement({self.table_name}, {self.where_clause})"


class UpdateStatement(Statement):
    """UPDATE 语句"""

    def __init__(
        self,
        table_name: str,
        assignments: List[tuple],
        where_clause: Optional[Expression] = None,
        line: int = 0,
        column: int = 0,
    ) -> None:
        super().__init__(line, column)
        self.table_name: str = table_name
        self.assignments: List[tuple] = assignments
        self.where_clause: Optional[Expression] = where_clause

    def accept(self, visitor) -> Any:
        return visitor.visit_update_statement(self)

    def __repr__(self) -> str:
        return f"UpdateStatement({self.table_name}, {self.assignments}, {self.where_clause})"


class DropTableStatement(Statement):
    """DROP TABLE 语句"""

    def __init__(self, table_name: str, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.table_name: str = table_name

    def accept(self, visitor) -> Any:
        return visitor.visit_drop_table_statement(self)

    def __repr__(self) -> str:
        return f"DropTableStatement({self.table_name})"


class BeginStatement(Statement):
    """BEGIN TRANSACTION 语句"""

    def __init__(self, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)

    def accept(self, visitor) -> Any:
        return visitor.visit_begin_statement(self)

    def __repr__(self) -> str:
        return "BeginStatement()"


class CommitStatement(Statement):
    """COMMIT 事务提交语句"""

    def __init__(self, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)

    def accept(self, visitor) -> Any:
        return visitor.visit_commit_statement(self)

    def __repr__(self) -> str:
        return "CommitStatement()"


class RollbackStatement(Statement):
    """ROLLBACK 回滚语句"""

    def __init__(self, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)

    def accept(self, visitor) -> Any:
        return visitor.visit_rollback_statement(self)

    def __repr__(self) -> str:
        return "RollbackStatement()"


class SQLProgram(ASTNode):
    """一个 SQL 程序，由多条语句组成"""

    def __init__(self, statements: List[Statement], line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.statements: List[Statement] = statements

    def accept(self, visitor) -> Any:
        return visitor.visit_sql_program(self)

    def __repr__(self) -> str:
        return f"SQLProgram({self.statements})"


class JoinType:
    """JOIN 类型常量"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class JoinClause(ASTNode):
    """JOIN 子句"""

    def __init__(self, join_type: str, table_name: str, on_condition: Expression, line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.join_type: str = join_type
        self.table_name: str = table_name
        self.on_condition: Expression = on_condition

    def accept(self, visitor) -> Any:
        return visitor.visit_join_clause(self)

    def __repr__(self) -> str:
        return f"JoinClause({self.join_type}, {self.table_name}, {self.on_condition})"


class OrderByClause(ASTNode):
    """ORDER BY 子句"""

    def __init__(self, expressions: List["SortExpression"], line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.expressions: List["SortExpression"] = expressions

    def accept(self, visitor) -> Any:
        return visitor.visit_order_by_clause(self)

    def __repr__(self) -> str:
        return f"OrderByClause({self.expressions})"


class SortExpression(ASTNode):
    """排序表达式"""

    def __init__(self, expression: Expression, direction: str = "ASC", line: int = 0, column: int = 0) -> None:
        super().__init__(line, column)
        self.expression: Expression = expression
        self.direction: str = direction  # ASC / DESC

    def accept(self, visitor) -> Any:
        return visitor.visit_sort_expression(self)

    def __repr__(self) -> str:
        return f"SortExpression({self.expression}, {self.direction})"


# ---------------- 访问者接口 ----------------
class ASTVisitor(ABC):
    """访问者接口：为每类节点提供 visit 方法"""

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
    def visit_join_clause(self, node: "JoinClause"): ...
    @abstractmethod
    def visit_delete_statement(self, node: DeleteStatement): ...
    @abstractmethod
    def visit_update_statement(self, node: "UpdateStatement"): ...
    def visit_drop_statement(self, node: "DropStatement"): ...
    @abstractmethod
    def visit_begin_statement(self, node: "BeginStatement"): ...
    @abstractmethod
    def visit_commit_statement(self, node: "CommitStatement"): ...
    @abstractmethod
    def visit_rollback_statement(self, node: "RollbackStatement"): ...
    @abstractmethod
    def visit_sql_program(self, node: SQLProgram): ...
    @abstractmethod
    def visit_order_by_clause(self, node: "OrderByClause"): ...
    @abstractmethod
    def visit_sort_expression(self, node: "SortExpression"): ...
