"""SQL 语法分析器模块
职责：基于词法器输出的 Token 列表，采用递归下降方法构建 AST（抽象语法树）
保持接口不变以兼容现有调用。
"""

from typing import List, Optional

from .ast_nodes import (
    BinaryExpression,
    ColumnDef,
    CreateTableStatement,
    DataType,
    DeleteStatement,
    Expression,
    Identifier,
    InsertStatement,
    JoinClause,
    Literal,
    OrderByClause,
    SelectStatement,
    SortExpression,
    SQLProgram,
    Statement,
    UpdateStatement,
)
from .lexer import SQLLexer, Token, TokenType


class ParseError(Exception):
    """语法解析过程中抛出的异常（包含位置信息与可选的期待项）"""

    def __init__(self, message: str, token: Token, expected: Optional[str] = None):
        self.message = message
        self.token = token
        self.expected = expected
        loc = f"line {token.line}, column {token.column}"

        if expected:
            full_message = f"Parse error at {loc}: {message}. Expected: {expected}, got: '{token.value}'"
        else:
            full_message = f"Parse error at {loc}: {message}"

        super().__init__(full_message)


class SQLParser:
    """SQL 语法分析器（递归下降实现）"""

    def __init__(self, tokens: List[Token]):
        self.tokens: List[Token] = tokens
        self.position: int = 0
        self.current_token: Optional[Token] = tokens[0] if tokens else None

    def advance(self) -> None:
        """将指针前移到下一个 token（若已到末尾则设置 EOF token）"""
        if self.position < len(self.tokens) - 1:
            self.position += 1
            self.current_token = self.tokens[self.position]
        else:
            # 确保 current_token 始终不是 None（使用 EOF 占位）
            self.current_token = Token(TokenType.EOF, "", 0, 0)

    def peek(self, offset: int = 1) -> Optional[Token]:
        """查看当前位置之后的某个 token，但不移动指针"""
        pos = self.position + offset
        if pos < len(self.tokens):
            return self.tokens[pos]
        return None

    def match(self, *token_types: TokenType) -> bool:
        """判断当前 token 的类型是否在给定类型列表中"""
        return (self.current_token is not None) and (self.current_token.type in token_types)

    def consume(self, token_type: TokenType, error_message: str = "") -> Token:
        """消费（获取并前进）指定类型的 token；否则抛出 ParseError"""
        if not self.match(token_type):
            expected = token_type.name
            msg = error_message or f"Expected {expected}"
            raise ParseError(msg, self.current_token, expected)

        token = self.current_token  # type: ignore
        self.advance()
        return token  # type: ignore

    def parse(self) -> SQLProgram:
        """解析整个 SQL 程序（由若干语句组成）并返回 SQLProgram 节点"""
        statements: List[Statement] = []

        while not self.match(TokenType.EOF):
            if self.current_token and self.current_token.type == TokenType.ERROR:
                raise ParseError(f"Lexical error: {self.current_token.value}", self.current_token)

            stmt = self.parse_statement()
            if stmt:
                statements.append(stmt)

            # 可有可无的分号分隔
            if self.match(TokenType.SEMICOLON):
                self.advance()

        return SQLProgram(statements, 1, 1)

    def parse_statement(self) -> Optional[Statement]:
        """根据当前 token 判断并解析单条语句"""
        if not self.current_token:
            return None

        if self.match(TokenType.KEYWORD):
            keyword = self.current_token.value.upper()

            if keyword == "CREATE":
                return self.parse_create_table()
            if keyword == "INSERT":
                return self.parse_insert()
            if keyword == "SELECT":
                return self.parse_select()
            if keyword == "DELETE":
                return self.parse_delete()
            if keyword == "UPDATE":
                return self.parse_update()

            raise ParseError(f"Unsupported statement: {keyword}", self.current_token)
        else:
            raise ParseError(
                "Expected statement",
                self.current_token,
                "CREATE, INSERT, SELECT, DELETE, or UPDATE",
            )

    def parse_create_table(self) -> CreateTableStatement:
        """解析 CREATE TABLE 语句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected CREATE")  # consume CREATE

        # 后续应为 TABLE 关键字
        if (not self.match(TokenType.KEYWORD)) or (self.current_token.value.upper() != "TABLE"):
            raise ParseError("Expected TABLE after CREATE", self.current_token, "TABLE")
        self.advance()  # consume TABLE

        # 表名
        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        # 左括号开列定义
        self.consume(TokenType.LEFT_PAREN, "Expected '(' after table name")

        cols: List[ColumnDef] = []
        while not self.match(TokenType.RIGHT_PAREN):
            col_def = self.parse_column_definition()
            cols.append(col_def)

            if self.match(TokenType.COMMA):
                self.advance()
            elif not self.match(TokenType.RIGHT_PAREN):
                raise ParseError("Expected ',' or ')' in column list", self.current_token, ", or )")

        if not cols:
            raise ParseError("Table must have at least one column", self.current_token)

        # 右括号结束列定义
        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after column list")

        return CreateTableStatement(table_name, cols, line, column)

    def parse_column_definition(self) -> ColumnDef:
        """解析单个列的定义（名称 + 数据类型）"""
        line, column = self.current_token.line, self.current_token.column

        column_name = self.consume(TokenType.IDENTIFIER, "Expected column name").value
        data_type = self.parse_data_type()

        return ColumnDef(column_name, data_type, line, column)

    def parse_data_type(self) -> DataType:
        """解析数据类型（支持带长度的 VARCHAR/CHAR）"""
        line, column = self.current_token.line, self.current_token.column

        if not self.match(TokenType.KEYWORD):
            raise ParseError("Expected data type", self.current_token, "INT, INTEGER, VARCHAR, or CHAR")

        type_name = self.current_token.value.upper()
        self.advance()

        size: Optional[int] = None
        if type_name in ["VARCHAR", "CHAR"] and self.match(TokenType.LEFT_PAREN):
            self.advance()  # consume '('
            size_token = self.consume(TokenType.INTEGER, "Expected size after '('")
            size = int(size_token.value)
            self.consume(TokenType.RIGHT_PAREN, "Expected ')' after size")

        return DataType(type_name, size, line, column)

    def parse_insert(self) -> InsertStatement:
        """解析 INSERT INTO ... VALUES (...) 形式的插入语句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected INSERT")  # consume INSERT

        if (not self.match(TokenType.KEYWORD)) or (self.current_token.value.upper() != "INTO"):
            raise ParseError("Expected INTO after INSERT", self.current_token, "INTO")
        self.advance()  # consume INTO

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        columns: Optional[List[str]] = None
        if self.match(TokenType.LEFT_PAREN):
            self.advance()  # '('
            cols_temp: List[str] = []
            while not self.match(TokenType.RIGHT_PAREN):
                col_name = self.consume(TokenType.IDENTIFIER, "Expected column name").value
                cols_temp.append(col_name)
                if self.match(TokenType.COMMA):
                    self.advance()
                elif not self.match(TokenType.RIGHT_PAREN):
                    raise ParseError("Expected ',' or ')' in column list", self.current_token, ", or )")
            self.consume(TokenType.RIGHT_PAREN, "Expected ')' after column list")
            columns = cols_temp

        # VALUES 关键字
        if (not self.match(TokenType.KEYWORD)) or (self.current_token.value.upper() != "VALUES"):
            raise ParseError("Expected VALUES", self.current_token, "VALUES")
        self.advance()  # consume VALUES

        values: List[List[Expression]] = []
        self.consume(TokenType.LEFT_PAREN, "Expected '(' after VALUES")
        current_row: List[Expression] = []
        while not self.match(TokenType.RIGHT_PAREN):
            val = self.parse_literal()
            current_row.append(val)
            if self.match(TokenType.COMMA):
                self.advance()
            elif not self.match(TokenType.RIGHT_PAREN):
                raise ParseError("Expected ',' or ')' in value list", self.current_token, ", or )")

        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after values")
        values.append(current_row)

        return InsertStatement(table_name, columns, values, line, column)

    def parse_select(self) -> SelectStatement:
        """解析 SELECT ... FROM ... [JOIN ...] [WHERE ...] [ORDER BY ...]"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected SELECT")  # consume SELECT

        select_list: List[Expression] = []
        # 目前仅支持以标识符形式列出选择字段（可扩展）
        while True:
            if self.match(TokenType.IDENTIFIER):
                col_name = self.current_token.value
                select_list.append(Identifier(col_name, self.current_token.line, self.current_token.column))
                self.advance()
            else:
                raise ParseError("Expected column name in SELECT list", self.current_token)

            if self.match(TokenType.COMMA):
                self.advance()
            else:
                break

        # FROM 子句
        if (not self.match(TokenType.KEYWORD)) or (self.current_token.value.upper() != "FROM"):
            raise ParseError("Expected FROM", self.current_token, "FROM")
        self.advance()  # consume FROM

        from_table = self.consume(TokenType.IDENTIFIER, "Expected table name after FROM").value

        # 可选 JOIN 子句集合
        join_clauses: List[JoinClause] = []
        while self.match(TokenType.KEYWORD) and self.current_token.value.upper() in ["JOIN", "INNER", "LEFT", "RIGHT", "FULL"]:
            join_clause = self.parse_join()
            join_clauses.append(join_clause)

        # 可选 WHERE 子句
        where_clause: Optional[Expression] = None
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "WHERE":
            self.advance()  # consume WHERE
            where_clause = self.parse_expression()

        # 可选 ORDER BY
        order_by_clause: Optional[OrderByClause] = None
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "ORDER":
            order_by_clause = self.parse_order_by()

        return SelectStatement(select_list, from_table, where_clause, order_by_clause, join_clauses, line, column)

    def parse_delete(self) -> DeleteStatement:
        """解析 DELETE FROM ... [WHERE ...]"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected DELETE")  # consume DELETE

        if (not self.match(TokenType.KEYWORD)) or (self.current_token.value.upper() != "FROM"):
            raise ParseError("Expected FROM after DELETE", self.current_token, "FROM")
        self.advance()  # consume FROM

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        where_clause: Optional[Expression] = None
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "WHERE":
            self.advance()
            where_clause = self.parse_expression()

        return DeleteStatement(table_name, where_clause, line, column)

    def parse_update(self) -> UpdateStatement:
        """解析 UPDATE ... SET col=val, ... [WHERE ...]"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected UPDATE")  # consume UPDATE

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        if (not self.match(TokenType.KEYWORD)) or (self.current_token.value.upper() != "SET"):
            raise ParseError("Expected SET after table name", self.current_token, "SET")
        self.advance()  # consume SET

        assignments: List[tuple] = []
        while True:
            column_name = self.consume(TokenType.IDENTIFIER, "Expected column name").value
            self.consume(TokenType.EQUALS, "Expected '=' after column name")
            value_expr = self.parse_primary()
            assignments.append((column_name, value_expr))

            if self.match(TokenType.COMMA):
                self.advance()
            else:
                break

        where_clause: Optional[Expression] = None
        if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "WHERE":
            self.advance()
            where_clause = self.parse_expression()

        return UpdateStatement(table_name, assignments, where_clause, line, column)

    def parse_expression(self) -> Expression:
        """入口：解析表达式（逻辑运算优先级处理）"""
        return self.parse_logical_or()

    def parse_logical_or(self) -> Expression:
        """解析 OR 链式表达式"""
        left = self.parse_logical_and()
        while self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "OR":
            op = self.current_token.value.upper()
            ln, col = self.current_token.line, self.current_token.column
            self.advance()
            right = self.parse_logical_and()
            left = BinaryExpression(left, op, right, ln, col)
        return left

    def parse_logical_and(self) -> Expression:
        """解析 AND 链式表达式"""
        left = self.parse_comparison()
        while self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "AND":
            op = self.current_token.value.upper()
            ln, col = self.current_token.line, self.current_token.column
            self.advance()
            right = self.parse_comparison()
            left = BinaryExpression(left, op, right, ln, col)
        return left

    def parse_comparison(self) -> Expression:
        """解析比较表达式（=, !=, <, >, <=, >=）"""
        left = self.parse_primary()

        if self.match(
            TokenType.EQUALS,
            TokenType.NOT_EQUALS,
            TokenType.LESS_THAN,
            TokenType.GREATER_THAN,
            TokenType.LESS_EQUALS,
            TokenType.GREATER_EQUALS,
        ):
            operator = self.current_token.value
            ln, col = self.current_token.line, self.current_token.column
            self.advance()
            right = self.parse_primary()
            return BinaryExpression(left, operator, right, ln, col)

        return left

    def parse_primary(self) -> Expression:
        """解析基础项：标识符或字面量"""
        if self.match(TokenType.IDENTIFIER):
            name = self.current_token.value
            ln, col = self.current_token.line, self.current_token.column
            self.advance()
            return Identifier(name, ln, col)

        if self.match(TokenType.INTEGER, TokenType.STRING):
            return self.parse_literal()

        raise ParseError("Expected identifier or literal", self.current_token)

    def parse_literal(self) -> Literal:
        """解析字面量节点（整数或字符串）"""
        if self.match(TokenType.INTEGER):
            val = int(self.current_token.value)
            ln, col = self.current_token.line, self.current_token.column
            self.advance()
            return Literal(val, "INT", ln, col)

        if self.match(TokenType.STRING):
            val = self.current_token.value
            ln, col = self.current_token.line, self.current_token.column
            self.advance()
            return Literal(val, "STRING", ln, col)

        raise ParseError("Expected literal value", self.current_token)

    def parse_order_by(self) -> OrderByClause:
        """解析 ORDER BY 子句及其排序项"""
        line, column = self.current_token.line, self.current_token.column

        self.consume_keyword("ORDER")
        self.consume_keyword("BY")

        sort_expressions: List[SortExpression] = []
        while True:
            expr = self.parse_primary()
            direction = "ASC"
            if self.match(TokenType.KEYWORD) and self.current_token.value.upper() in ["ASC", "DESC"]:
                direction = self.current_token.value.upper()
                self.advance()
            sort_expressions.append(SortExpression(expr, direction, expr.line, expr.column))
            if not self.match(TokenType.COMMA):
                break
            self.advance()

        return OrderByClause(sort_expressions, line, column)

    def parse_join(self) -> JoinClause:
        """解析 JOIN 子句（包括 INNER/LEFT/RIGHT/FULL 可选 OUTER）"""
        line, column = self.current_token.line, self.current_token.column

        join_type = "INNER"  # 默认类型

        if self.current_token.value.upper() in ["INNER", "LEFT", "RIGHT", "FULL"]:
            join_type = self.current_token.value.upper()
            self.advance()
            # 可能出现 OUTER
            if self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "OUTER":
                self.advance()

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "JOIN"):
            raise ParseError("Expected JOIN", self.current_token, "JOIN")
        self.advance()  # consume JOIN

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "ON"):
            raise ParseError("Expected ON after table name", self.current_token, "ON")
        self.advance()  # consume ON

        on_condition = self.parse_expression()

        return JoinClause(join_type, table_name, on_condition, line, column)

    def consume_keyword(self, keyword: str):
        """检查并消费指定关键字（区分大小写以大写比较）"""
        if not (self.match(TokenType.KEYWORD) and self.current_token.value.upper() == keyword):
            raise ParseError(f"Expected {keyword}", self.current_token, keyword)
        self.advance()


def main():
    """简单自测：词法 -> 语法 -> 打印 AST 列表"""
    sql = """
    CREATE TABLE student(id INT, name VARCHAR(50), age INT);
    INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
    SELECT id, name FROM student WHERE age > 18;
    DELETE FROM student WHERE id = 1;
    """

    print("输入SQL:")
    print(sql)
    print()

    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()

        parser = SQLParser(tokens)
        ast = parser.parse()

        print("语法分析成功!")
        print("AST结构:")
        for i, stmt in enumerate(ast.statements):
            print(f"{i+1}. {stmt}")

    except Exception as e:
        print(f"分析错误: {e}")


if __name__ == "__main__":
    main()
