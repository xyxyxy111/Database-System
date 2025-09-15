"""SQL 词法扫描器模块
职责：读取输入的 SQL 文本，逐字符识别并产出用于解析的 Token 列表
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional


class TokenType(Enum):
    """Token 类型枚举，表示词法单元类别"""

    # 关键字
    KEYWORD = auto()

    # 标识符与常量
    IDENTIFIER = auto()
    INTEGER = auto()
    STRING = auto()

    # 比较与相等运算符
    EQUALS = auto()
    NOT_EQUALS = auto()
    LESS_THAN = auto()
    GREATER_THAN = auto()
    LESS_EQUALS = auto()
    GREATER_EQUALS = auto()

    # 分隔符 / 标点
    COMMA = auto()
    SEMICOLON = auto()
    LEFT_PAREN = auto()
    RIGHT_PAREN = auto()

    # 其他特殊标记
    EOF = auto()
    ERROR = auto()


@dataclass
class Token:
    """词法单元结构体"""

    type: TokenType
    value: str
    line: int
    column: int

    def __repr__(self) -> str:
        return f"Token({self.type.name}, '{self.value}', {self.line}:{self.column})"


class LexerError(Exception):
    """词法阶段的异常类型"""

    def __init__(self, message: str, line: int, column: int):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(f"Lexical error at line {line}, column {column}: {message}")


class SQLLexer:
    """SQL 词法器：扫描输入并生成 Token 列表，供解析器使用"""

    # 关键字集合（大写）
    KEYWORDS = {
        "CREATE",
        "TABLE",
        "INSERT",
        "INTO",
        "SELECT",
        "FROM",
        "WHERE",
        "VALUES",
        "DELETE",
        "UPDATE",
        "SET",
        "INT",
        "INTEGER",
        "VARCHAR",
        "CHAR",
        "AND",
        "OR",
        "NOT",
        "ORDER",
        "BY",
        "ASC",
        "DESC",
        "LIMIT",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "OUTER",
        "ON",
    }

    # 运算符到 TokenType 的映射
    OPERATORS = {
        "=": TokenType.EQUALS,
        "!=": TokenType.NOT_EQUALS,
        "<>": TokenType.NOT_EQUALS,
        "<": TokenType.LESS_THAN,
        ">": TokenType.GREATER_THAN,
        "<=": TokenType.LESS_EQUALS,
        ">=": TokenType.GREATER_EQUALS,
    }

    # 分隔符映射（注意：'*' 临时按标识符处理）
    DELIMITERS = {
        ",": TokenType.COMMA,
        ";": TokenType.SEMICOLON,
        "(": TokenType.LEFT_PAREN,
        ")": TokenType.RIGHT_PAREN,
        "*": TokenType.IDENTIFIER,
    }

    def __init__(self, source: str):
        self.source: str = source
        self.position: int = 0
        self.line: int = 1
        self.column: int = 1
        self.tokens: List[Token] = []

    def current_char(self) -> Optional[str]:
        """返回当前位置字符；越界返回 None"""
        if self.position >= len(self.source):
            return None
        return self.source[self.position]

    def peek_char(self, offset: int = 1) -> Optional[str]:
        """向前查看指定偏移位置的字符（不移动指针）"""
        pos: int = self.position + offset
        if pos >= len(self.source):
            return None
        return self.source[pos]

    def advance(self) -> Optional[str]:
        """将指针前移一个字符，返回原字符；遇换行更新行列信息"""
        if self.position >= len(self.source):
            return None

        ch: str = self.source[self.position]
        self.position += 1

        if ch == "\n":
            self.line += 1
            self.column = 1
        else:
            self.column += 1

        return ch

    def skip_whitespace(self) -> None:
        """跳过所有空白字符（空格、制表、换行等）"""
        while self.current_char() and self.current_char().isspace():
            self.advance()

    def read_string(self) -> str:
        """读取并返回一个字符串字面量（支持简单转义）"""
        quote_char: str = self.current_char()  # ' 或 "
        start_line, start_column = self.line, self.column
        self.advance()  # 跳过开引号

        value: str = ""
        while self.current_char() and self.current_char() != quote_char:
            ch = self.advance()
            if ch == "\\":  # 处理转义
                next_ch = self.advance()
                if next_ch == "n":
                    value += "\n"
                elif next_ch == "t":
                    value += "\t"
                elif next_ch == "r":
                    value += "\r"
                elif next_ch == "\\":
                    value += "\\"
                elif next_ch == quote_char:
                    value += quote_char
                else:
                    value += next_ch or ""
            else:
                value += ch

        if not self.current_char():
            # 未闭合的字符串字面量
            raise LexerError("Unterminated string literal", start_line, start_column)

        self.advance()  # 跳过结束引号
        return value

    def read_number(self) -> str:
        """读取一个整数序列（只支持十进制整型）"""
        value: str = ""
        while self.current_char() and self.current_char().isdigit():
            value += self.advance()
        return value

    def read_identifier(self) -> str:
        """读取标识符或关键字（字母、数字或下划线）"""
        value: str = ""
        while self.current_char() and (
            self.current_char().isalnum() or self.current_char() == "_"
        ):
            value += self.advance()
        return value

    def read_operator(self) -> str:
        """读取运算符，优先识别双字符运算符（如 >=, <=, !=, <>）"""
        ch = self.current_char()

        # 双字符情况：检查后继字符
        if ch in ["!", "<", ">"]:
            nxt = self.peek_char()
            if nxt == "=":
                # 形如 !=, <=, >=
                self.advance()
                self.advance()
                return ch + "="
            if ch == "<" and nxt == ">":
                # 特殊 <> 表示不等于
                self.advance()
                self.advance()
                return "<>"

        # 单字符运算符
        return self.advance()

    def tokenize(self) -> List[Token]:
        """执行完整扫描并返回 Token 列表（含 EOF）"""
        self.tokens = []

        try:
            while self.position < len(self.source):
                self.skip_whitespace()

                if not self.current_char():
                    break

                ch = self.current_char()
                current_line, current_column = self.line, self.column

                # 字符串字面量
                if ch in ['"', "'"]:
                    try:
                        val = self.read_string()
                        tok = Token(TokenType.STRING, val, current_line, current_column)
                        self.tokens.append(tok)
                    except LexerError as lex_err:
                        # 记录错误 Token 并抛出异常
                        self.tokens.append(
                            Token(TokenType.ERROR, lex_err.message, current_line, current_column)
                        )
                        raise

                # 数字字面量
                elif ch.isdigit():
                    val = self.read_number()
                    self.tokens.append(Token(TokenType.INTEGER, val, current_line, current_column))

                # 标识符或关键字
                elif ch.isalpha() or ch == "_":
                    val = self.read_identifier()
                    ttype = TokenType.KEYWORD if val.upper() in self.KEYWORDS else TokenType.IDENTIFIER
                    self.tokens.append(Token(ttype, val.upper(), current_line, current_column))

                # 运算符
                elif ch in "=!<>":
                    op = self.read_operator()
                    if op in self.OPERATORS:
                        self.tokens.append(Token(self.OPERATORS[op], op, current_line, current_column))
                    else:
                        raise LexerError(f"Unknown operator '{op}'", current_line, current_column)

                # 分隔符与符号
                elif ch in self.DELIMITERS:
                    ttype = self.DELIMITERS[ch]
                    self.tokens.append(Token(ttype, ch, current_line, current_column))
                    self.advance()

                # 单行注释 -- 到行尾
                elif ch == "-" and self.peek_char() == "-":
                    while self.current_char() and self.current_char() != "\n":
                        self.advance()

                # 多行注释 /* ... */（需闭合）
                elif ch == "/" and self.peek_char() == "*":
                    self.advance()  # 跳过 /
                    self.advance()  # 跳过 *
                    while self.current_char():
                        if self.current_char() == "*" and self.peek_char() == "/":
                            self.advance()
                            self.advance()
                            break
                        self.advance()
                    else:
                        # 注释未闭合
                        raise LexerError("Unterminated comment", current_line, current_column)

                # 其他不可识别字符
                else:
                    raise LexerError(f"Unexpected character '{ch}'", current_line, current_column)

            # 在末尾附加 EOF 标记
            self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))

        except LexerError as e:
            # 将错误也记录为 Token，再次抛出以供调用方处理
            self.tokens.append(Token(TokenType.ERROR, e.message, e.line, e.column))
            raise

        return self.tokens

    def get_tokens(self) -> List[Token]:
        """获取已生成的 Token 列表；若尚未生成则自动触发扫描"""
        if not self.tokens:
            self.tokenize()
        return self.tokens

    def print_tokens(self) -> None:
        """友好格式化输出 Token 列表，便于调试"""
        tokens = self.get_tokens()
        print("Token 列表:")
        print("=" * 50)
        print(f"{'类型':<12} {'值':<15} {'位置':<10}")
        print("-" * 50)

        for token in tokens:
            if token.type == TokenType.EOF:
                break
            print(f"{token.type.name:<12} {token.value:<15} {token.line}:{token.column}")

        print("-" * 50)
        print(f"共生成 {len([t for t in tokens if t.type != TokenType.EOF])} 个 Token")


def main():
    """模块简单自检用例"""
    sql = """
    CREATE TABLE student(
        id INT,
        name VARCHAR(50),
        age INT
    );
    
    INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
    INSERT INTO student VALUES (2, "Bob", 22);
    
    SELECT id, name FROM student WHERE age > 18;
    
    DELETE FROM student WHERE id = 1;
    """

    print("输入SQL:")
    print(sql)
    print()

    try:
        lexer = SQLLexer(sql)
        lexer.print_tokens()
    except LexerError as e:
        print(f"词法分析错误: {e}")


if __name__ == "__main__":
    main()
