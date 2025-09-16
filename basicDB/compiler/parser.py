"""SQL语法分析器 - 负责根据Token序列构建抽象语法树(AST)"""

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
    """语法分析错误"""

    def __init__(self, message: str, token: Token, expected: Optional[str] = None):
        self.message = message
        self.token = token
        self.expected = expected
        location = f"line {token.line}, column {token.column}"

        if expected:
            full_message = f"Parse error at {location}: {message}. Expected: {expected}, got: '{token.value}'"
        else:
            full_message = f"Parse error at {location}: {message}"

        super().__init__(full_message)


class SQLParser:
    """SQL语法分析器 - 采用递归下降分析方法"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.position = 0
        self.current_token = tokens[0] if tokens else None

    def advance(self):
        """移动到下一个Token"""
        if self.position < len(self.tokens) - 1:
            self.position += 1
            self.current_token = self.tokens[self.position]
        else:
            self.current_token = Token(TokenType.EOF, "", 0, 0)

    def peek(self, offset: int = 1) -> Optional[Token]:
        """预览后续Token"""
        pos = self.position + offset
        if pos < len(self.tokens):
            return self.tokens[pos]
        return None

    def match(self, *token_types: TokenType) -> bool:
        """检查当前Token是否匹配指定类型"""
        return self.current_token and self.current_token.type in token_types

    def consume(self, token_type: TokenType, error_message: str = "") -> Token:
        """消费指定类型的Token"""
        if not self.match(token_type):
            expected = token_type.name
            message = error_message or f"Expected {expected}"
            raise ParseError(message, self.current_token, expected)

        token = self.current_token
        self.advance()
        return token

    def parse(self) -> SQLProgram:
        """解析SQL程序"""
        statements = []

        while not self.match(TokenType.EOF):
            if self.current_token.type == TokenType.ERROR:
                raise ParseError(
                    f"Lexical error: {self.current_token.value}", self.current_token
                )

            stmt = self.parse_statement()
            if stmt:
                statements.append(stmt)

            # 可选的分号
            if self.match(TokenType.SEMICOLON):
                self.advance()

        return SQLProgram(statements, 1, 1)

    def parse_statement(self) -> Optional[Statement]:
        """解析SQL语句"""
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
            if keyword == "DROP":
                return self.parse_drop_table()
            elif keyword == "BEGIN":
                return self.parse_begin()
            elif keyword == "COMMIT":
                return self.parse_commit()
            elif keyword == "ROLLBACK":
                return self.parse_rollback()
            else:
                raise ParseError(
                    f"Unsupported statement: {keyword}", self.current_token
                )
        else:
            raise ParseError(
                "Expected statement",
                self.current_token,
                "CREATE, INSERT, SELECT, DELETE, or UPDATE",
            )

    def parse_create_table(self) -> CreateTableStatement:
        """解析CREATE TABLE语句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected CREATE")  # CREATE

        if (
            not self.match(TokenType.KEYWORD)
            or self.current_token.value.upper() != "TABLE"
        ):
            raise ParseError("Expected TABLE after CREATE", self.current_token, "TABLE")
        self.advance()  # TABLE

        # 表名
        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        # 左括号
        self.consume(TokenType.LEFT_PAREN, "Expected '(' after table name")

        # 列定义列表
        columns = []
        while not self.match(TokenType.RIGHT_PAREN):
            column = self.parse_column_definition()
            columns.append(column)

            if self.match(TokenType.COMMA):
                self.advance()
            elif not self.match(TokenType.RIGHT_PAREN):
                raise ParseError(
                    "Expected ',' or ')' in column list", self.current_token, ", or )"
                )

        if not columns:
            raise ParseError("Table must have at least one column", self.current_token)

        # 右括号
        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after column list")

        return CreateTableStatement(table_name, columns, line, column)

    def parse_column_definition(self) -> ColumnDef:
        """解析列定义"""
        line, column = self.current_token.line, self.current_token.column

        # 列名
        column_name = self.consume(TokenType.IDENTIFIER, "Expected column name").value

        # 数据类型
        data_type = self.parse_data_type()

        return ColumnDef(column_name, data_type, line, column)

    def parse_data_type(self) -> DataType:
        """解析数据类型"""
        line, column = self.current_token.line, self.current_token.column

        if not self.match(TokenType.KEYWORD):
            raise ParseError(
                "Expected data type",
                self.current_token,
                "INT, INTEGER, VARCHAR, or CHAR",
            )

        type_name = self.current_token.value.upper()
        self.advance()

        # 处理带长度的类型，如VARCHAR(50)
        size = None
        if type_name in ["VARCHAR", "CHAR"] and self.match(TokenType.LEFT_PAREN):
            self.advance()  # (
            size_token = self.consume(TokenType.INTEGER, "Expected size after '('")
            size = int(size_token.value)
            self.consume(TokenType.RIGHT_PAREN, "Expected ')' after size")

        return DataType(type_name, size, line, column)

    def parse_insert(self) -> InsertStatement:
        """解析INSERT语句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected INSERT")  # INSERT

        if (
            not self.match(TokenType.KEYWORD)
            or self.current_token.value.upper() != "INTO"
        ):
            raise ParseError("Expected INTO after INSERT", self.current_token, "INTO")
        self.advance()  # INTO

        # 表名
        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        # 可选的列列表
        columns = None
        if self.match(TokenType.LEFT_PAREN):
            self.advance()  # (
            columns = []

            while not self.match(TokenType.RIGHT_PAREN):
                col_name = self.consume(
                    TokenType.IDENTIFIER, "Expected column name"
                ).value
                columns.append(col_name)

                if self.match(TokenType.COMMA):
                    self.advance()
                elif not self.match(TokenType.RIGHT_PAREN):
                    raise ParseError(
                        "Expected ',' or ')' in column list",
                        self.current_token,
                        ", or )",
                    )

            self.consume(TokenType.RIGHT_PAREN, "Expected ')' after column list")

        # VALUES关键字
        if (
            not self.match(TokenType.KEYWORD)
            or self.current_token.value.upper() != "VALUES"
        ):
            raise ParseError("Expected VALUES", self.current_token, "VALUES")
        self.advance()  # VALUES

        # 值列表
        values = []
        self.consume(TokenType.LEFT_PAREN, "Expected '(' after VALUES")

        current_row = []
        while not self.match(TokenType.RIGHT_PAREN):
            value = self.parse_literal()
            current_row.append(value)

            if self.match(TokenType.COMMA):
                self.advance()
            elif not self.match(TokenType.RIGHT_PAREN):
                raise ParseError(
                    "Expected ',' or ')' in value list", self.current_token, ", or )"
                )

        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after values")
        values.append(current_row)

        return InsertStatement(table_name, columns, values, line, column)

    def parse_select(self) -> SelectStatement:
        """解析SELECT语句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected SELECT")  # SELECT

        # 选择列表
        select_list = []
        while True:
            if self.match(TokenType.IDENTIFIER):
                # 检查是否为聚合函数调用
                if (
                    self.peek()
                    and self.peek().type == TokenType.LEFT_PAREN
                    and self.current_token.value.upper()
                    in ["COUNT", "SUM", "AVG", "MAX", "MIN"]
                ):
                    # 解析聚合函数
                    aggregate_func = self.parse_aggregate_function()
                    select_list.append(aggregate_func)
                else:
                    # 普通列名
                    col_name = self.current_token.value
                    select_list.append(
                        Identifier(
                            col_name, self.current_token.line, self.current_token.column
                        )
                    )
                    self.advance()
            elif self.match(TokenType.KEYWORD) and self.current_token.value.upper() in [
                "COUNT",
                "SUM",
                "AVG",
                "MAX",
                "MIN",
            ]:
                # 聚合函数关键字
                aggregate_func = self.parse_aggregate_function()
                select_list.append(aggregate_func)
            elif self.match(TokenType.STAR):
                # 处理 *
                select_list.append(
                    Identifier("*", self.current_token.line, self.current_token.column)
                )
                self.advance()
            else:
                raise ParseError(
                    "Expected column name or aggregate function in SELECT list",
                    self.current_token,
                )

            if self.match(TokenType.COMMA):
                self.advance()
            else:
                break

        # FROM子句
        if (
            not self.match(TokenType.KEYWORD)
            or self.current_token.value.upper() != "FROM"
        ):
            raise ParseError("Expected FROM", self.current_token, "FROM")
        self.advance()  # FROM

        from_table = self.consume(
            TokenType.IDENTIFIER, "Expected table name after FROM"
        ).value

        # 可选的JOIN子句
        join_clauses = []
        while self.match(TokenType.KEYWORD) and self.current_token.value.upper() in [
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
        ]:
            join_clause = self.parse_join()
            join_clauses.append(join_clause)

        # 可选的WHERE子句
        where_clause = None
        if (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == "WHERE"
        ):
            self.advance()  # WHERE
            where_clause = self.parse_expression()

        # 可选的ORDER BY子句
        order_by_clause = None
        if (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == "ORDER"
        ):
            order_by_clause = self.parse_order_by()

        return SelectStatement(
            select_list,
            from_table,
            where_clause,
            order_by_clause,
            join_clauses,
            line,
            column,
        )

    def parse_delete(self) -> DeleteStatement:
        """解析DELETE语句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected DELETE")  # DELETE

        if (
            not self.match(TokenType.KEYWORD)
            or self.current_token.value.upper() != "FROM"
        ):
            raise ParseError("Expected FROM after DELETE", self.current_token, "FROM")
        self.advance()  # FROM

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        # 可选的WHERE子句
        where_clause = None
        if (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == "WHERE"
        ):
            self.advance()  # WHERE
            where_clause = self.parse_expression()

        return DeleteStatement(table_name, where_clause, line, column)

    def parse_update(self) -> UpdateStatement:
        """解析UPDATE语句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected UPDATE")  # UPDATE

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        # SET子句
        if (
            not self.match(TokenType.KEYWORD)
            or self.current_token.value.upper() != "SET"
        ):
            raise ParseError("Expected SET after table name", self.current_token, "SET")
        self.advance()  # SET

        # 解析赋值列表 column=value, column=value, ...
        assignments = []
        while True:
            column_name = self.consume(
                TokenType.IDENTIFIER, "Expected column name"
            ).value
            self.consume(TokenType.EQUALS, "Expected '=' after column name")
            value = self.parse_primary()
            assignments.append((column_name, value))

            if self.match(TokenType.COMMA):
                self.advance()  # ,
            else:
                break

        # 可选的WHERE子句
        where_clause = None
        if (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == "WHERE"
        ):
            self.advance()  # WHERE
            where_clause = self.parse_expression()

        return UpdateStatement(table_name, assignments, where_clause, line, column)

    def parse_drop_table(self) -> DropTableStatement:
        """解析 DROP TABLE 语句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume(TokenType.KEYWORD, "Expected DROP")  # consume DROP

        if (not self.match(TokenType.KEYWORD)) or (self.current_token.value.upper() != "TABLE"):
            raise ParseError("Expected TABLE after DROP", self.current_token, "TABLE")
        self.advance()  # consume TABLE

        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name after TABLE").value

        return DropTableStatement(table_name, line, column)



    def parse_expression(self) -> Expression:
        """解析表达式 - 支持逻辑和比较表达式"""
        return self.parse_logical_or()

    def parse_logical_or(self) -> Expression:
        """解析逻辑OR表达式"""
        left = self.parse_logical_and()

        while (
            self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "OR"
        ):
            operator = self.current_token.value.upper()
            line, column = self.current_token.line, self.current_token.column
            self.advance()
            right = self.parse_logical_and()
            left = BinaryExpression(left, operator, right, line, column)

        return left

    def parse_logical_and(self) -> Expression:
        """解析逻辑AND表达式"""
        left = self.parse_comparison()

        while (
            self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "AND"
        ):
            operator = self.current_token.value.upper()
            line, column = self.current_token.line, self.current_token.column
            self.advance()
            right = self.parse_comparison()
            left = BinaryExpression(left, operator, right, line, column)

        return left

    def parse_comparison(self) -> Expression:
        """解析比较表达式"""
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
            line, column = self.current_token.line, self.current_token.column
            self.advance()

            right = self.parse_primary()
            return BinaryExpression(left, operator, right, line, column)

        return left

    def parse_primary(self) -> Expression:
        """解析基本表达式"""
        if self.match(TokenType.IDENTIFIER):
            name = self.current_token.value
            line, column = self.current_token.line, self.current_token.column
            self.advance()
            return Identifier(name, line, column)

        elif self.match(TokenType.KEYWORD):
            # 检查是否是聚合函数
            if self.current_token.value.upper() in [
                "COUNT",
                "SUM",
                "AVG",
                "MAX",
                "MIN",
            ]:
                return self.parse_aggregate_function()
            else:
                raise ParseError(
                    f"Unexpected keyword '{self.current_token.value}'",
                    self.current_token,
                )

        elif self.match(TokenType.INTEGER, TokenType.STRING):
            return self.parse_literal()

        else:
            raise ParseError("Expected identifier or literal", self.current_token)

    def parse_aggregate_function(self) -> AggregateFunction:
        """解析聚合函数"""
        function_name = self.current_token.value.upper()
        line, column = self.current_token.line, self.current_token.column
        self.advance()

        self.consume(TokenType.LEFT_PAREN, "Expected '(' after function name")

        # 检查DISTINCT关键字
        distinct = False
        if (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == "DISTINCT"
        ):
            distinct = True
            self.advance()

        # 解析函数参数
        if function_name == "COUNT" and self.match(TokenType.STAR):
            # COUNT(*) 的特殊情况
            argument = Literal(
                "*", "WILDCARD", self.current_token.line, self.current_token.column
            )
            self.advance()
        else:
            # 普通列名或表达式
            argument = self.parse_expression()

        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after function argument")

        return AggregateFunction(function_name, argument, distinct, line, column)

    def parse_literal(self) -> Literal:
        """解析字面量"""
        if self.match(TokenType.INTEGER):
            value = int(self.current_token.value)
            line, column = self.current_token.line, self.current_token.column
            self.advance()
            return Literal(value, "INT", line, column)

        elif self.match(TokenType.STRING):
            value = self.current_token.value
            line, column = self.current_token.line, self.current_token.column
            self.advance()
            return Literal(value, "STRING", line, column)

        else:
            raise ParseError("Expected literal value", self.current_token)

    def parse_order_by(self) -> OrderByClause:
        """解析ORDER BY子句"""
        line, column = self.current_token.line, self.current_token.column

        self.consume_keyword("ORDER")
        self.consume_keyword("BY")

        sort_expressions = []
        while True:
            expr = self.parse_primary()  # 解析列名或表达式
            direction = "ASC"  # 默认升序

            if self.match(TokenType.KEYWORD) and self.current_token.value.upper() in [
                "ASC",
                "DESC",
            ]:
                direction = self.current_token.value.upper()
                self.advance()

            sort_expressions.append(
                SortExpression(expr, direction, expr.line, expr.column)
            )

            if not self.match(TokenType.COMMA):
                break
            self.advance()  # 跳过逗号

        return OrderByClause(sort_expressions, line, column)

    def parse_join(self) -> JoinClause:
        """解析JOIN子句"""
        line, column = self.current_token.line, self.current_token.column

        # 确定JOIN类型
        join_type = "INNER"  # 默认为INNER JOIN

        if self.current_token.value.upper() in ["INNER", "LEFT", "RIGHT", "FULL"]:
            join_type = self.current_token.value.upper()
            self.advance()

            # 处理 LEFT OUTER JOIN, RIGHT OUTER JOIN, FULL OUTER JOIN
            if (
                self.match(TokenType.KEYWORD)
                and self.current_token.value.upper() == "OUTER"
            ):
                self.advance()  # OUTER

        # 消费JOIN关键字
        if not (
            self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "JOIN"
        ):
            raise ParseError("Expected JOIN", self.current_token, "JOIN")
        self.advance()  # JOIN

        # 获取要JOIN的表名
        table_name = self.consume(TokenType.IDENTIFIER, "Expected table name").value

        # 消费ON关键字
        if not (
            self.match(TokenType.KEYWORD) and self.current_token.value.upper() == "ON"
        ):
            raise ParseError("Expected ON after table name", self.current_token, "ON")
        self.advance()  # ON

        # 解析JOIN条件
        on_condition = self.parse_expression()

        return JoinClause(join_type, table_name, on_condition, line, column)

    def consume_keyword(self, keyword: str):
        """消费指定关键字"""
        if not (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == keyword
        ):
            raise ParseError(f"Expected {keyword}", self.current_token, keyword)
        self.advance()

    def parse_begin(self) -> BeginStatement:
        """解析BEGIN语句"""
        line, column = self.current_token.line, self.current_token.column
        self.advance()  # BEGIN

        # 可选的TRANSACTION关键字
        if (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == "TRANSACTION"
        ):
            self.advance()

        return BeginStatement(line, column)

    def parse_commit(self) -> CommitStatement:
        """解析COMMIT语句"""
        line, column = self.current_token.line, self.current_token.column
        self.advance()  # COMMIT

        # 可选的TRANSACTION关键字
        if (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == "TRANSACTION"
        ):
            self.advance()

        return CommitStatement(line, column)

    def parse_rollback(self) -> RollbackStatement:
        """解析ROLLBACK语句"""
        line, column = self.current_token.line, self.current_token.column
        self.advance()  # ROLLBACK

        # 可选的TRANSACTION关键字
        if (
            self.match(TokenType.KEYWORD)
            and self.current_token.value.upper() == "TRANSACTION"
        ):
            self.advance()

        return RollbackStatement(line, column)


def main():
    """测试用例"""
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
        # 词法分析
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()

        # 语法分析
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
