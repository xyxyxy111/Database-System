"""简化版 SQL 词法分析器 — 改善代码可读性与注释（接口不变）"""

from enum import Enum, auto
from typing import List, Optional
from dataclasses import dataclass


class TokenType(Enum):
    """Token 类型枚举（表示不同的词法单元）"""
    KEYWORD = auto()
    IDENTIFIER = auto()
    INTEGER = auto()
    STRING = auto()
    EQUALS = auto()
    NOT_EQUALS = auto()
    LESS_THAN = auto()
    GREATER_THAN = auto()
    LESS_EQUALS = auto()
    GREATER_EQUALS = auto()
    COMMA = auto()
    SEMICOLON = auto()
    LEFT_PAREN = auto()
    RIGHT_PAREN = auto()
    EOF = auto()
    ERROR = auto()


@dataclass
class Token:
    """词法单元结构"""
    type: TokenType
    value: str
    line: int
    column: int


class LexerError(Exception):
    """词法分析过程中抛出的异常"""

    def __init__(self, message: str, line: int, column: int):
        self.message = message
        self.line = line
        self.column = column
        location = f"line {line}, column {column}"
        super().__init__(f"Lexical error at {location}: {message}")


class SQLLexer:
    """SQL 词法器（职责：把源文本拆分为 Token 序列）"""

    # 常见 SQL 关键字（大写）
    KEYWORDS = {
        'CREATE', 'TABLE', 'INSERT', 'INTO', 'SELECT', 'FROM', 'WHERE',
        'VALUES', 'DELETE', 'INT', 'INTEGER', 'VARCHAR', 'CHAR', 'UPDATE',
        'SET', 'AND', 'OR', 'ORDER', 'BY', 'ASC', 'DESC', 'JOIN', 'ON'
    }

    def __init__(self, source: str):
        self.source: str = source
        self.position: int = 0
        self.line: int = 1
        self.column: int = 1
        self.tokens: List[Token] = []

    def current_char(self) -> Optional[str]:
        """返回当前位置字符；越界则返回 None"""
        if self.position >= len(self.source):
            return None
        return self.source[self.position]

    def advance(self) -> Optional[str]:
        """指针前移并返回刚读到的字符；同时维护行/列信息"""
        if self.position >= len(self.source):
            return None

        ch: str = self.source[self.position]
        self.position += 1

        if ch == '\n':
            self.line += 1
            self.column = 1
        else:
            self.column += 1

        return ch

    def skip_whitespace(self) -> None:
        """跳过空白字符（空格、制表、换行等）"""
        while self.current_char() and self.current_char().isspace():
            self.advance()

    def read_string(self) -> str:
        """读取字符串字面量，支持基本转义序列"""
        quote_char = self.current_char()
        start_line, start_column = self.line, self.column
        self.advance()  # 跳过起始引号

        value: str = ""
        while self.current_char() and self.current_char() != quote_char:
            ch = self.advance()
            if ch == '\\':
                # 处理常见转义
                nxt = self.advance()
                if nxt == 'n':
                    value += '\n'
                elif nxt == 't':
                    value += '\t'
                elif nxt == 'r':
                    value += '\r'
                elif nxt == '\\':
                    value += '\\'
                elif nxt == quote_char:
                    value += quote_char
                else:
                    value += nxt or ""
            else:
                value += ch

        if not self.current_char():
            # 字符串未闭合
            raise LexerError("Unterminated string literal", start_line, start_column)

        self.advance()  # 跳过结束引号
        return value

    def read_number(self) -> str:
        """读取整数（连续数字）"""
        value: str = ""
        while self.current_char() and self.current_char().isdigit():
            value += self.advance()
        return value

    def read_identifier(self) -> str:
        """读取标识符或关键字（字母、数字、下划线）"""
        value: str = ""
        while (self.current_char() and
               (self.current_char().isalnum() or self.current_char() == '_')):
            value += self.advance()
        return value

    def tokenize(self) -> List[Token]:
        """主流程：将源文本拆分为 Token 列表（包含 EOF）"""
        self.tokens = []

        while self.position < len(self.source):
            self._process_next_token()

        # 追加 EOF 标志
        self.tokens.append(Token(TokenType.EOF, '', self.line, self.column))
        return self.tokens

    def _process_next_token(self) -> None:
        """处理并生成下一个 token（将结果追加到 self.tokens）"""
        self.skip_whitespace()

        if not self.current_char():
            return

        char = self.current_char()
        current_line, current_column = self.line, self.column

        try:
            if char in ('"', "'"):
                self._process_string_token(current_line, current_column)
            elif char.isdigit():
                self._process_number_token(current_line, current_column)
            elif char.isalpha() or char == '_':
                self._process_identifier_token(current_line, current_column)
            elif char in '=!<>':
                self._process_operator_token(current_line, current_column)
            elif char in ',;()':
                self._process_delimiter_token(current_line, current_column)
            elif char == '-' and self._peek() == '-':
                self._skip_line_comment()
            elif char == '/' and self._peek() == '*':
                self._skip_block_comment(current_line, current_column)
            else:
                raise LexerError(f"Unexpected character '{char}'", current_line, current_column)
        except LexerError as e:
            # 记录错误 token 并抛出异常
            error_token = Token(TokenType.ERROR, e.message, current_line, current_column)
            self.tokens.append(error_token)
            raise

    def _process_string_token(self, line: int, column: int) -> None:
        """读取并添加字符串 token"""
        val = self.read_string()
        self.tokens.append(Token(TokenType.STRING, val, line, column))

    def _process_number_token(self, line: int, column: int) -> None:
        """读取并添加整数 token"""
        val = self.read_number()
        self.tokens.append(Token(TokenType.INTEGER, val, line, column))

    def _process_identifier_token(self, line: int, column: int) -> None:
        """读取标识符或关键字并添加相应 token"""
        val = self.read_identifier()
        ttype = TokenType.KEYWORD if val.upper() in self.KEYWORDS else TokenType.IDENTIFIER
        self.tokens.append(Token(ttype, val.upper(), line, column))

    def _process_operator_token(self, line: int, column: int) -> None:
        """读取运算符（支持 !=, <>, <=, >= 等）并生成 token"""
        op = self._read_operator()
        ttype = self._get_operator_type(op)
        if ttype:
            self.tokens.append(Token(ttype, op, line, column))
        else:
            raise LexerError(f"Unknown operator '{op}'", line, column)

    def _process_delimiter_token(self, line: int, column: int) -> None:
        """处理逗号、分号、括号等分隔符"""
        ch = self.advance()
        ttype = self._get_delimiter_type(ch)
        if ttype:
            self.tokens.append(Token(ttype, ch, line, column))

    def _skip_line_comment(self) -> None:
        """跳过单行注释内容（以 -- 开头）"""
        while self.current_char() and self.current_char() != '\n':
            self.advance()

    def _skip_block_comment(self, line: int, column: int) -> None:
        """跳过多行注释 /* ... */（若未闭合则抛错）"""
        # 已确认起始符，先跳过 '/*'
        self.advance()
        self.advance()
        while self.current_char():
            if self.current_char() == '*' and self._peek() == '/':
                self.advance()
                self.advance()
                return
            self.advance()
        # 若循环退出表示注释未闭合
        raise LexerError("Unterminated comment", line, column)

    def _peek(self, offset: int = 1) -> Optional[str]:
        """向前查看字符但不移动指针"""
        pos = self.position + offset
        if pos >= len(self.source):
            return None
        return self.source[pos]

    def _read_operator(self) -> str:
        """读取运算符文本，优先处理双字符运算符"""
        ch = self.current_char()
        if ch in ('!', '<', '>'):
            nxt = self._peek()
            if nxt == '=':
                self.advance()
                self.advance()
                return ch + '='
            if ch == '<' and nxt == '>':
                self.advance()
                self.advance()
                return '<>'
        # 单字符运算符
        return self.advance()

    def _get_operator_type(self, operator: str) -> Optional[TokenType]:
        """运算符到 TokenType 的映射"""
        mapping = {
            '=': TokenType.EQUALS,
            '!=': TokenType.NOT_EQUALS,
            '<>': TokenType.NOT_EQUALS,
            '<': TokenType.LESS_THAN,
            '>': TokenType.GREATER_THAN,
            '<=': TokenType.LESS_EQUALS,
            '>=': TokenType.GREATER_EQUALS,
        }
        return mapping.get(operator)

    def _get_delimiter_type(self, delimiter: str) -> Optional[TokenType]:
        """分隔符到 TokenType 的映射"""
        mapping = {
            ',': TokenType.COMMA,
            ';': TokenType.SEMICOLON,
            '(': TokenType.LEFT_PAREN,
            ')': TokenType.RIGHT_PAREN,
        }
        return mapping.get(delimiter)

    def print_tokens(self) -> None:
        """以表格形式打印 tokens（方便调试）"""
        if not self.tokens:
            self.tokenize()

        print("Token序列:")
        print("=" * 50)
        print(f"{'类型':<12} {'值':<15} {'位置':<10}")
        print("-" * 50)

        for token in self.tokens:
            if token.type == TokenType.EOF:
                break
            location = f"{token.line}:{token.column}"
            print(f"{token.type.name:<12} {token.value:<15} {location}")

        non_eof_tokens = [t for t in self.tokens if t.type != TokenType.EOF]
        print("-" * 50)
        print(f"共生成 {len(non_eof_tokens)} 个Token")


def main():
    """模块自检示例（可直接运行）"""
    sql = """
    CREATE TABLE student(
        id INT,
        name VARCHAR(50),
        age INT
    );

    INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
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
