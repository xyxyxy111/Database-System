"""SQL 语法解析器 - 输入 Token 序列，输出 AST 抽象语法树"""

from typing import List, Optional

from .ast_nodes import (
    AggregateFunction,
    BeginStatement,
    BinaryExpression,
    ColumnDef,
    CommitStatement,
    CreateTableStatement,
    DataType,
    DeleteStatement,
    DropTableStatement,
    Expression,
    FunctionCall,
    Identifier,
    InsertStatement,
    JoinClause,
    Literal,
    OrderByClause,
    RollbackStatement,
    SelectStatement,
    SortExpression,
    SQLProgram,
    Statement,
    UpdateStatement,
)
from .lexer import SQLLexer, Token, TokenType


class ParseError(Exception):
    """在语法分析过程中触发的异常"""

    def __init__(self, message: str, token: Token, expected: Optional[str] = None):
        self.message: str = message
        self.token: Token = token
        self.expected: Optional[str] = expected
        location = f"line {token.line}, column {token.column}"

        if expected:
            detail = f"Parse error at {location}: {message}. Expected: {expected}, got '{token.value}'"
        else:
            detail = f"Parse error at {location}: {message}"

        super().__init__(detail)


class SQLParser:
    """SQL 语法分析器 - 使用递归下降法构建语法树"""

    def __init__(self, tokens: List[Token]):
        self.tokens: List[Token] = tokens
        self.position: int = 0
        self.current_token: Optional[Token] = tokens[0] if tokens else None

    # ---------------- 工具方法 ----------------

    def advance(self) -> None:
        """指针移动到下一个 Token，如果到达结尾则设置 EOF"""
        if self.position < len(self.tokens) - 1:
            self.position += 1
            self.current_token = self.tokens[self.position]
        else:
            self.current_token = Token(TokenType.EOF, "", 0, 0)

    def peek(self, offset: int = 1) -> Optional[Token]:
        """向前查看指定偏移位置的 Token"""
        pos = self.position + offset
        return self.tokens[pos] if pos < len(self.tokens) else None

    def match(self, *token_types: TokenType) -> bool:
        """检测当前 Token 是否属于指定的类型"""
        return self.current_token is not None and self.current_token.type in token_types

    def consume(self, token_type: TokenType, error_message: str = "") -> Token:
        """取出当前 Token，若类型不符则抛错"""
        if not self.match(token_type):
            expected = token_type.name
            msg = error_message or f"Expected {expected}"
            raise ParseError(msg, self.current_token, expected)

        token: Token = self.current_token
        self.advance()
        return token

    def consume_keyword(self, keyword: str) -> None:
        """消耗一个关键字 Token，若不匹配则抛出异常"""
        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == keyword):
            raise ParseError(f"Expected {keyword}", self.current_token, keyword)
        self.advance()

    # ---------------- 主入口 ----------------

    def parse(self) -> SQLProgram:
        """解析整个 SQL 输入，返回语法树对象"""
        stmts: List[Statement] = []

        while not self.match(TokenType.EOF):
            if self.current_token.type == TokenType.ERROR:
                raise ParseError(f"Lexical error: {self.current_token.value}", self.current_token)

            stmt = self.parse_statement()
            if stmt:
                stmts.append(stmt)

            # 跳过可选分号
            if self.match(TokenType.SEMICOLON):
                self.advance()

        return SQLProgram(stmts, 1, 1)

    def parse_statement(self) -> Optional[Statement]:
        """根据关键字类型分发到对应的解析函数"""
        if not self.current_token:
            return None

        if self.match(TokenType.KEYWORD):
            keyword = self.current_token.value.upper()

            if keyword == "CREATE":
                return self.parse_create_table()
            elif keyword == "INSERT":
                return self.parse_insert()
            elif keyword == "SELECT":
                return self.parse_select()
            elif keyword == "DELETE":
                return self.parse_delete()
            elif keyword == "UPDATE":
                return self.parse_update()
            elif keyword == "DROP":
                return self.parse_drop_table()
            elif keyword == "BEGIN":
                return self.parse_begin()
            elif keyword == "COMMIT":
                return self.parse_commit()
            elif keyword == "ROLLBACK":
                return self.parse_rollback()
            else:
                raise ParseError(f"Unsupported statement: {keyword}", self.current_token)
        else:
            raise ParseError("Expected statement", self.current_token)

    # ---------------- CREATE ----------------

    def parse_create_table(self) -> CreateTableStatement:
        """解析 CREATE TABLE 语句"""
        line, col = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected CREATE")

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "TABLE"):
            raise ParseError("Expected TABLE after CREATE", self.current_token, "TABLE")
        self.advance()

        table_name: str = self.consume(TokenType.IDENTIFIER, "Expected table name").value
        self.consume(TokenType.LEFT_PAREN, "Expected '(' after table name")

        columns: List[ColumnDef] = []
        while not self.match(TokenType.RIGHT_PAREN):
            col_def = self.parse_column_definition()
            columns.append(col_def)

            if self.match(TokenType.COMMA):
                self.advance()
            elif not self.match(TokenType.RIGHT_PAREN):
                raise ParseError("Expected ',' or ')'", self.current_token, ", or )")

        if not columns:
            raise ParseError("Table must contain at least one column", self.current_token)

        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after column definitions")

        return CreateTableStatement(table_name, columns, line, col)

    def parse_column_definition(self) -> ColumnDef:
        """解析列定义部分"""
        line, col = self.current_token.line, self.current_token.column

        column_name: str = self.consume(TokenType.IDENTIFIER, "Expected column name").value
        data_type: DataType = self.parse_data_type()

        return ColumnDef(column_name, data_type, line, col)

    def parse_data_type(self) -> DataType:
        """解析字段类型，如 INT、VARCHAR(50)"""
        line, col = self.current_token.line, self.current_token.column

        if not self.match(TokenType.KEYWORD):
            raise ParseError("Expected data type", self.current_token)

        type_name: str = self.current_token.value.upper()
        self.advance()

        size: Optional[int] = None
        if type_name in ["VARCHAR", "CHAR"] and self.match(TokenType.LEFT_PAREN):
            self.advance()
            size_token = self.consume(TokenType.INTEGER, "Expected size number")
            size = int(size_token.value)
            self.consume(TokenType.RIGHT_PAREN, "Expected ')' after size")

        return DataType(type_name, size, line, col)

    # ---------------- INSERT ----------------

    def parse_insert(self) -> InsertStatement:
        """解析 INSERT INTO ... VALUES 语句"""
        line, col = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected INSERT")

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "INTO"):
            raise ParseError("Expected INTO after INSERT", self.current_token, "INTO")
        self.advance()

        table_name: str = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        columns: Optional[List[str]] = None
        if self.match(TokenType.LEFT_PAREN):
            self.advance()
            columns = []

            while not self.match(TokenType.RIGHT_PAREN):
                col_name = self.consume(TokenType.IDENTIFIER, "Expected column name").value
                columns.append(col_name)

                if self.match(TokenType.COMMA):
                    self.advance()
                elif not self.match(TokenType.RIGHT_PAREN):
                    raise ParseError("Expected ',' or ')'", self.current_token)

            self.consume(TokenType.RIGHT_PAREN, "Expected ')' after column list")

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "VALUES"):
            raise ParseError("Expected VALUES", self.current_token, "VALUES")
        self.advance()

        rows: List[List[Literal]] = []
        self.consume(TokenType.LEFT_PAREN, "Expected '(' before values")

        row: List[Literal] = []
        while not self.match(TokenType.RIGHT_PAREN):
            val = self.parse_literal()
            row.append(val)

            if self.match(TokenType.COMMA):
                self.advance()
            elif not self.match(TokenType.RIGHT_PAREN):
                raise ParseError("Expected ',' or ')'", self.current_token)

        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after values")
        rows.append(row)

        return InsertStatement(table_name, columns, rows, line, col)

    # ---------------- SELECT ----------------

    def parse_select(self) -> SelectStatement:
        """解析 SELECT 语句"""
        line, col = self.current_token.line, self.current_token.column
        self.consume(TokenType.KEYWORD, "Expected SELECT")

        select_list: List[Expression] = []
        while True:
            if self.match(TokenType.IDENTIFIER):
                if (
                    self.peek()
                    and self.peek().type == TokenType.LEFT_PAREN
                    and self.current_token.value.upper() in ["COUNT", "SUM", "AVG", "MAX", "MIN"]
                ):
                    select_list.append(self.parse_aggregate_function())
                else:
                    name = self.current_token.value
                    select_list.append(Identifier(name, self.current_token.line, self.current_token.column))
                    self.advance()
            elif self.match(TokenType.KEYWORD) and self.current_token.value.upper() in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
                select_list.append(self.parse_aggregate_function())
            elif self.match(TokenType.STAR):
                select_list.append(Identifier("*", self.current_token.line, self.current_token.column))
                self.advance()
            else:
                raise ParseError("Expected column or function", self.current_token)

            if self.match(TokenType.COMMA):
                self.advance()
            else:
                break

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "FROM"):
            raise ParseError("Expected FROM", self.current_token, "FROM")
        self.advance()

        from_table: str = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        joins: List[JoinClause] = []
        while self.match(TokenType.KEYWORD) and self.current_token.value.upper() in ["JOIN", "INNER", "LEFT", "RIGHT", "FULL"]:
            joins.append(self.parse_join())

        where_clause: Optional[Expression] = None
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "WHERE":
            self.advance()
            where_clause = self.parse_expression()

        order_by: Optional[OrderByClause] = None
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "ORDER":
            order_by = self.parse_order_by()

        return SelectStatement(select_list, from_table, where_clause, order_by, joins, line, col)

    # ---------------- DELETE ----------------

    def parse_delete(self) -> DeleteStatement:
        """解析 DELETE 语句"""
        line, col = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected DELETE")

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "FROM"):
            raise ParseError("Expected FROM", self.current_token, "FROM")
        self.advance()

        table_name: str = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        condition: Optional[Expression] = None
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "WHERE":
            self.advance()
            condition = self.parse_expression()

        return DeleteStatement(table_name, condition, line, col)

    # ---------------- UPDATE ----------------

    def parse_update(self) -> UpdateStatement:
        """解析 UPDATE 语句"""
        line, col = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected UPDATE")
        table_name: str = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "SET"):
            raise ParseError("Expected SET", self.current_token, "SET")
        self.advance()

        assignments: List[tuple[str, Expression]] = []
        while True:
            col_name = self.consume(TokenType.IDENTIFIER, "Expected column").value
            self.consume(TokenType.EQUALS, "Expected '='")
            expr = self.parse_primary()
            assignments.append((col_name, expr))

            if self.match(TokenType.COMMA):
                self.advance()
            else:
                break

        condition: Optional[Expression] = None
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "WHERE":
            self.advance()
            condition = self.parse_expression()

        return UpdateStatement(table_name, assignments, condition, line, col)

    # ---------------- DROP ----------------

    def parse_drop_table(self) -> DropTableStatement:
        """解析 DROP TABLE 语句"""
        line, col = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected DROP")

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "TABLE"):
            raise ParseError("Expected TABLE", self.current_token, "TABLE")
        self.advance()

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value
        return DropTableStatement(table_name, line, col)

    # ---------------- 表达式 ----------------

    def parse_expression(self) -> Expression:
        """入口：解析逻辑表达式"""
        return self.parse_logical_or()

    def parse_logical_or(self) -> Expression:
        left = self.parse_logical_and()
        while self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "OR":
            op = self.current_token.value.upper()
            line, col = self.current_token.line, self.current_token.column
            self.advance()
            right = self.parse_logical_and()
            left = BinaryExpression(left, op, right, line, col)
        return left

    def parse_logical_and(self) -> Expression:
        left = self.parse_comparison()
        while self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "AND":
            op = self.current_token.value.upper()
            line, col = self.current_token.line, self.current_token.column
            self.advance()
            right = self.parse_comparison()
            left = BinaryExpression(left, op, right, line, col)
        return left

    def parse_comparison(self) -> Expression:
        left = self.parse_primary()
        if self.match(TokenType.EQUALS, TokenType.NOT_EQUALS, TokenType.LESS_THAN,
                      TokenType.GREATER_THAN, TokenType.LESS_EQUALS, TokenType.GREATER_EQUALS):
            op = self.current_token.value
            line, col = self.current_token.line, self.current_token.column
            self.advance()
            right = self.parse_primary()
            return BinaryExpression(left, op, right, line, col)
        return left

    def parse_primary(self) -> Expression:
        if self.match(TokenType.IDENTIFIER):
            name = self.current_token.value
            line, col = self.current_token.line, self.current_token.column
            self.advance()
            return Identifier(name, line, col)
        elif self.match(TokenType.KEYWORD) and self.current_token.value.upper() in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
            return self.parse_aggregate_function()
        elif self.match(TokenType.INTEGER, TokenType.STRING):
            return self.parse_literal()
        else:
            raise ParseError("Expected expression", self.current_token)

    # ---------------- 函数与字面量 ----------------

    def parse_aggregate_function(self) -> AggregateFunction:
        """解析聚合函数调用"""
        func_name = self.current_token.value.upper()
        line, col = self.current_token.line, self.current_token.column
        self.advance()

        self.consume(TokenType.LEFT_PAREN, "Expected '(' after function")

        distinct = False
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "DISTINCT":
            distinct = True
            self.advance()

        if func_name == "COUNT" and self.match(TokenType.STAR):
            arg = Literal("*", "WILDCARD", self.current_token.line, self.current_token.column)
            self.advance()
        else:
            arg = self.parse_expression()

        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after function")
        return AggregateFunction(func_name, arg, distinct, line, col)

    def parse_literal(self) -> Literal:
        """解析字面量"""
        if self.match(TokenType.INTEGER):
            val = int(self.current_token.value)
            line, col = self.current_token.line, self.current_token.column
            self.advance()
            return Literal(val, "INT", line, col)
        elif self.match(TokenType.STRING):
            val = self.current_token.value
            line, col = self.current_token.line, self.current_token.column
            self.advance()
            return Literal(val, "STRING", line, col)
        else:
            raise ParseError("Expected literal", self.current_token)

    # ---------------- ORDER / JOIN ----------------

    def parse_order_by(self) -> OrderByClause:
        """解析 ORDER BY 子句"""
        line, col = self.current_token.line, self.current_token.column
        self.consume_keyword("ORDER")
        self.consume_keyword("BY")

        sort_items: List[SortExpression] = []
        while True:
            expr = self.parse_primary()
            direction = "ASC"
            if self.match(TokenType.KEYWORD) and self.current_token.value.upper() in ["ASC", "DESC"]:
                direction = self.current_token.value.upper()
                self.advance()
            sort_items.append(SortExpression(expr, direction, expr.line, expr.column))

            if not self.match(TokenType.COMMA):
                break
            self.advance()

        return OrderByClause(sort_items, line, col)

    def parse_join(self) -> JoinClause:
        """解析 JOIN 子句"""
        line, col = self.current_token.line, self.current_token.column
        join_type = "INNER"

        if self.current_token.value.upper() in ["INNER", "LEFT", "RIGHT", "FULL"]:
            join_type = self.current_token.value.upper()
            self.advance()
            if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "JOIN":
                self.advance()
        elif self.current_token.value.upper() == "JOIN":
            self.advance()
        else:
            raise ParseError("Expected JOIN", self.current_token)

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table").value

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "ON"):
            raise ParseError("Expected ON after JOIN", self.current_token, "ON")
        self.advance()

        condition = self.parse_expression()
        return JoinClause(join_type, table_name, condition, line, col)

    # ---------------- 事务控制 ----------------

    def parse_begin(self) -> BeginStatement:
        line, col = self.current_token.line, self.current_token.column
        self.consume_keyword("BEGIN")
        return BeginStatement(line, col)

    def parse_commit(self) -> CommitStatement:
        line, col = self.current_token.line, self.current_token.column
        self.consume_keyword("COMMIT")
        return CommitStatement(line, col)

    def parse_rollback(self) -> RollbackStatement:
        line, col = self.current_token.line, self.current_token.column
        self.consume_keyword("ROLLBACK")
        return RollbackStatement(line, col)
