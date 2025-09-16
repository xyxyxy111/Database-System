"""
SQL 词法分析器
负责将输入的 SQL 文本拆分为一系列 Token，供后续解析器使用。
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional


class TokenType(Enum):
    """表示不同种类的标记（Token）"""

    # 关键字类
    KEYWORD = auto()

    # 标识符、字面量
    IDENTIFIER = auto()
    INTEGER = auto()
    STRING = auto()

    # 比较与算术运算符
    EQUALS = auto()
    NOT_EQUALS = auto()
    LESS_THAN = auto()
    GREATER_THAN = auto()
    LESS_EQUALS = auto()
    GREATER_EQUALS = auto()
    PLUS = auto()
    MINUS = auto()

    # 分隔符与符号
    COMMA = auto()
    SEMICOLON = auto()
    LEFT_PAREN = auto()
    RIGHT_PAREN = auto()
    STAR = auto()  # 星号

    # 文件末尾 / 错误
    EOF = auto()
    ERROR = auto()


@dataclass
class Token:
    """Token 数据容器"""

    type: TokenType
    value: str
    line: int
    column: int

    def __repr__(self) -> str:
        return f"Token({self.type.name}, '{self.value}', {self.line}:{self.column})"


class LexerError(Exception):
    """词法分析期间抛出的异常（含位置信息）"""

    def __init__(self, message: str, line: int, column: int) -> None:
        # 将错误信息用中文格式化，便于用户阅读
        self.message: str = message
        self.line: int = line
        self.column: int = column
        super().__init__(f"词法错误：第 {line} 行，第 {column} 列 - {message}")


class SQLLexer:
    """将 SQL 文本分解为 Token 列表的类"""

    # 支持的关键字集合（大写）
    KEYWORDS = {
        "CREATE", "TABLE", "INSERT", "INTO", "SELECT", "FROM", "WHERE", "VALUES",
        "DELETE", "DROP", "UPDATE", "SET", "INT", "INTEGER", "VARCHAR", "CHAR",
        "TEXT", "DECIMAL", "AND", "OR", "NOT", "ORDER", "BY", "ASC", "DESC",
        "LIMIT", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "ON",
        "COUNT", "SUM", "AVG", "MAX", "MIN", "DISTINCT",
        "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION",
    }

    # 支持的运算符到 TokenType 的映射
    OPERATORS = {
        "=": TokenType.EQUALS,
        "!=": TokenType.NOT_EQUALS,
        "<>": TokenType.NOT_EQUALS,
        "<": TokenType.LESS_THAN,
        ">": TokenType.GREATER_THAN,
        "<=": TokenType.LESS_EQUALS,
        ">=": TokenType.GREATER_EQUALS,
        "+": TokenType.PLUS,
        "-": TokenType.MINUS,
    }

    # 分隔符到 TokenType 的映射
    DELIMITERS = {
        ",": TokenType.COMMA,
        ";": TokenType.SEMICOLON,
        "(": TokenType.LEFT_PAREN,
        ")": TokenType.RIGHT_PAREN,
        "*": TokenType.STAR,
    }

    def __init__(self, source: str) -> None:
        self.source: str = source
        self.position: int = 0
        self.line: int = 1
        self.column: int = 1
        self.tokens: List[Token] = []

    def current_char(self) -> Optional[str]:
        """返回当前位置字符或 None（到达末尾）"""
        if self.position >= len(self.source):
            return None
        return self.source[self.position]

    def peek_char(self, offset: int = 1) -> Optional[str]:
        """向前查看 offset 个位置的字符（不移动游标）"""
        pos = self.position + offset
        if pos >= len(self.source):
            return None
        return self.source[pos]

    def advance(self) -> Optional[str]:
        """将位置向前移动一个字符并更新行/列信息，返回移动前的字符"""
        if self.position >= len(self.source):
            return None

        ch = self.source[self.position]
        self.position += 1

        if ch == "\n":
            self.line += 1
            self.column = 1
        else:
            self.column += 1

        return ch

    def skip_whitespace(self) -> None:
        """跳过连续的空白字符（包括换行）"""
        while (c := self.current_char()) is not None and c.isspace():
            self.advance()

    def read_string(self) -> str:
        """读取以单/双引号包裹的字符串字面量（支持常用转义）"""
        quote_char = self.current_char()
        start_line, start_column = self.line, self.column
        # consume starting quote
        self.advance()

        chars: List[str] = []
        while (c := self.current_char()) is not None and c != quote_char:
            if c == "\\":
                # 转义序列处理
                self.advance()  # skip '\'
                next_c = self.current_char()
                if next_c is None:
                    break
                esc = self.advance()
                if esc == "n":
                    chars.append("\n")
                elif esc == "t":
                    chars.append("\t")
                elif esc == "r":
                    chars.append("\r")
                elif esc == "\\":
                    chars.append("\\")
                elif esc == quote_char:
                    chars.append(quote_char)
                else:
                    chars.append(esc)
            else:
                chars.append(self.advance() or "")
        if self.current_char() is None:
            raise LexerError("字符串字面量未闭合", start_line, start_column)
        # consume ending quote
        self.advance()
        return "".join(chars)

    def read_number(self) -> str:
        """读取连续的数字作为整数字面量（暂不处理小数）"""
        digits: List[str] = []
        while (c := self.current_char()) is not None and c.isdigit():
            digits.append(self.advance() or "")
        return "".join(digits)

    def read_identifier(self) -> str:
        """读取标识符（字母/数字/下划线）"""
        parts: List[str] = []
        while (c := self.current_char()) is not None and (c.isalnum() or c == "_"):
            parts.append(self.advance() or "")
        return "".join(parts)

    def read_operator(self) -> str:
        """读取运算符，优先识别双字符运算符（如 !=, <=, >=, <>）"""
        ch = self.current_char()
        next_ch = self.peek_char()
        # 优先匹配两字符运算符
        if ch and next_ch and (ch + next_ch) in self.OPERATORS:
            first = self.advance()
            second = self.advance()
            return (first or "") + (second or "")
        # 单字符运算符
        return self.advance() or ""

    def tokenize(self) -> List[Token]:
        """执行完整的词法分析，返回 Token 列表（末尾包含 EOF）"""
        self.tokens = []
        try:
            while self.position < len(self.source):
                self.skip_whitespace()
                if (c := self.current_char()) is None:
                    break

                cur_line, cur_col = self.line, self.column

                # 单/双引号字符串
                if c in {"'", '"'}:
                    try:
                        val = self.read_string()
                        self.tokens.append(Token(TokenType.STRING, val, cur_line, cur_col))
                    except LexerError as lex_err:
                        self.tokens.append(Token(TokenType.ERROR, lex_err.message, cur_line, cur_col))
                        raise

                # 数字常量
                elif c.isdigit():
                    val = self.read_number()
                    self.tokens.append(Token(TokenType.INTEGER, val, cur_line, cur_col))

                # 标识符或关键字
                elif c.isalpha() or c == "_":
                    val = self.read_identifier()
                    up = val.upper()
                    ttype = TokenType.KEYWORD if up in self.KEYWORDS else TokenType.IDENTIFIER
                    # 保持关键字一律大写，标识符保留原样（若希望也大写可改）
                    token_value = up if ttype == TokenType.KEYWORD else val
                    self.tokens.append(Token(ttype, token_value, cur_line, cur_col))

                # 单行注释以 -- 开头
                elif c == "-" and self.peek_char() == "-":
                    # 跳过直到行尾
                    while (ch := self.current_char()) is not None and ch != "\n":
                        self.advance()

                # 多行注释 /* ... */
                elif c == "/" and self.peek_char() == "*":
                    # consume '/*'
                    self.advance()
                    self.advance()
                    found_end = False
                    while (ch := self.current_char()) is not None:
                        if ch == "*" and self.peek_char() == "/":
                            self.advance()
                            self.advance()
                            found_end = True
                            break
                        self.advance()
                    if not found_end:
                        raise LexerError("多行注释未闭合", cur_line, cur_col)

                # 运算符起始字符（包括 + - = ! < > 等）
                elif c in {k[0] for k in self.OPERATORS.keys()}:
                    op = self.read_operator()
                    if op in self.OPERATORS:
                        self.tokens.append(Token(self.OPERATORS[op], op, cur_line, cur_col))
                    else:
                        raise LexerError(f"未知运算符 '{op}'", cur_line, cur_col)

                # 分隔符和符号
                elif c in self.DELIMITERS:
                    tok_type = self.DELIMITERS[c]
                    self.tokens.append(Token(tok_type, c, cur_line, cur_col))
                    self.advance()

                else:
                    # 其余为不可识别字符
                    raise LexerError(f"遇到意外字符 '{c}'", cur_line, cur_col)

            # 文件末尾标记
            self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))

        except LexerError as e:
            # 将错误也作为一个 ERROR Token 记录，并继续抛出异常
            self.tokens.append(Token(TokenType.ERROR, e.message, e.line, e.column))
            raise

        return self.tokens

    def get_tokens(self) -> List[Token]:
        """返回已生成的 Token 列表（如未生成则先执行 tokenize）"""
        if not self.tokens:
            self.tokenize()
        return self.tokens

    def print_tokens(self) -> None:
        """调试用：在控制台格式化打印 Token 列表（不包含 EOF）"""
        tokens = self.get_tokens()
        print("Token 序列：")
        print("=" * 60)
        print(f"{'类型':<14} {'值':<20} {'位置':<10}")
        print("-" * 60)
        count = 0
        for t in tokens:
            if t.type == TokenType.EOF:
                break
            print(f"{t.type.name:<14} {t.value:<20} {t.line}:{t.column}")
            count += 1
        print("-" * 60)
        print(f"共生成 {count} 个 Token（不含 EOF）")


def main() -> None:
    """简单示例与自测"""
    sample_sql = """
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

    print("输入 SQL：")
    print(sample_sql)
    print()

    try:
        lexer = SQLLexer(sample_sql)
        lexer.print_tokens()
    except LexerError as e:
        # 直接打印中文化的错误信息
        print(f"词法分析错误: {e}")
        # 若需要调试详细信息，也可以访问 e.message, e.line, e.column
        # print(e.message, e.line, e.column)


if __name__ == "__main__":
    main()
