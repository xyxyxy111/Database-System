import re
from typing import Iterator, List, Tuple

Token = Tuple[str, str, int, int]  # (type, lexeme, line, col)

TOKEN_SPEC = [
    ("NUMBER",   r"\d+(?:\.\d+)?"),
    ("STRING",   r"'(?:[^']*)'"),
    ("IDENT",    r"[A-Za-z_][A-Za-z0-9_]*"),
    ("STAR",     r"\*"),
    ("COMMA",    r","),
    ("LPAREN",   r"\("),
    ("RPAREN",   r"\)"),
    ("SEMI",     r";"),
    ("EQ",       r"="),
    ("GT",       r">"),
    ("LT",       r"<"),
    ("GE",       r">="),
    ("LE",       r"<="),
    ("NE",       r"<>|!="),
    ("WS",       r"\s+"),
]

# 顺序很重要：多字符操作符需在单字符之前
TOKEN_SPEC.sort(key=lambda x: -len(x[1]))
TOK_REGEX = re.compile("|".join(f"(?P<{name}>{pat})" for name, pat in TOKEN_SPEC), re.IGNORECASE)

KEYWORDS = {"select", "from", "where", "insert", "into", "values", "create", "table", "delete", "int", "varchar"}

class LexError(Exception):
    def __init__(self, message: str, line: int, col: int):
        super().__init__(f"LexError at {line}:{col} - {message}")
        self.line = line
        self.col = col


def tokenize(sql: str) -> List[Token]:
    tokens: List[Token] = []
    line = 1
    col = 1
    i = 0
    while i < len(sql):
        m = TOK_REGEX.match(sql, i)
        if not m:
            raise LexError(f"非法字符 '{sql[i]}'", line, col)
        kind = m.lastgroup or ""
        text = m.group(0)
        start_col = col
        # 计算换行
        newlines = text.count("\n")
        if newlines:
            line += newlines
            col = len(text.split("\n")[-1]) + 1
        else:
            col += len(text)
        i = m.end()
        if kind == "WS":
            continue
        if kind == "IDENT" and text.lower() in KEYWORDS:
            tokens.append((text.upper(), text, line, start_col))
        elif kind == "STRING":
            tokens.append(("STRING", text[1:-1], line, start_col))
        else:
            tokens.append((kind.upper(), text, line, start_col))
    return tokens
