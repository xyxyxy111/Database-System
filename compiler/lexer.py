"""
词法分析器模块 (Lexer)
=====================

本模块负责将SQL源代码字符串分解为词法单元(Token)序列。
词法分析是编译过程的第一步，它将输入的字符流转换为有意义的词法单元。

主要功能：
1. 识别SQL关键字（SELECT, FROM, WHERE等）
2. 识别标识符（表名、列名等）
3. 识别常量（数字、字符串）
4. 识别运算符（=, >, <等）
5. 识别分隔符（括号、逗号、分号等）
6. 提供错误定位功能

Token格式：[种别码, 词素值, 行号, 列号]
"""

import re
from typing import Iterator, List, Tuple

# Token类型定义：包含种别码、词素值、行号、列号四个元素
Token = Tuple[str, str, int, int]  # (type, lexeme, line, col)

# 词法单元规范定义
# 每个元组包含：(种别码名称, 正则表达式模式)
TOKEN_SPEC = [
    ("NUMBER",   r"\d+(?:\.\d+)?"),  # 数字：整数或小数
    ("STRING",   r"'(?:[^']*)'"),     # 字符串：单引号包围的字符序列
    ("IDENT",    r"[A-Za-z_][A-Za-z0-9_]*"),  # 标识符：字母或下划线开头，可包含字母、数字、下划线
    ("STAR",     r"\*"),              # 星号：用于SELECT *
    ("COMMA",    r","),               # 逗号：分隔符
    ("LPAREN",   r"\("),              # 左括号
    ("RPAREN",   r"\)"),              # 右括号
    ("SEMI",     r";"),               # 分号：语句结束符
    ("EQ",       r"="),               # 等号：赋值或比较
    ("GT",       r">"),               # 大于号
    ("LT",       r"<"),               # 小于号
    ("GE",       r">="),              # 大于等于
    ("LE",       r"<="),              # 小于等于
    ("NE",       r"<>|!="),           # 不等于：支持<>和!=两种写法
    ("WS",       r"\s+"),             # 空白字符：空格、制表符、换行符等
]

# 重要：多字符操作符必须在单字符操作符之前匹配
# 例如：">="必须在">"之前，否则">="会被错误地识别为">"和"="
TOKEN_SPEC.sort(key=lambda x: -len(x[1]))

# 编译正则表达式，用于匹配所有词法单元
# (?P<name>pattern) 创建命名组，便于后续提取匹配的组名
TOK_REGEX = re.compile("|".join(f"(?P<{name}>{pat})" for name, pat in TOKEN_SPEC), re.IGNORECASE)

# SQL关键字集合
# 这些词在SQL中有特殊含义，不是普通标识符
KEYWORDS = {"select", "from", "where", "insert", "into", "values", "create", "table", "delete", "int", "varchar"}


class LexError(Exception):
    """
    词法分析错误异常类
    
    当词法分析器遇到无法识别的字符或语法错误时，会抛出此异常。
    异常信息包含错误消息、行号和列号，便于用户定位错误。
    """
    def __init__(self, message: str, line: int, col: int):
        super().__init__(f"LexError at {line}:{col} - {message}")
        self.line = line  # 错误所在行号
        self.col = col    # 错误所在列号


def tokenize(sql: str) -> List[Token]:
    """
    词法分析主函数
    
    将SQL源代码字符串分解为Token序列。
    
    参数:
        sql (str): 输入的SQL源代码字符串
        
    返回:
        List[Token]: Token列表，每个Token包含[种别码, 词素值, 行号, 列号]
        
    异常:
        LexError: 当遇到无法识别的字符时抛出
        
    示例:
        >>> tokens = tokenize("SELECT * FROM users;")
        >>> print(tokens[0])  # ('SELECT', 'SELECT', 1, 1)
    """
    tokens: List[Token] = []  # 存储解析出的Token
    line = 1                  # 当前行号
    col = 1                   # 当前列号
    i = 0                     # 当前字符位置
    
    # 遍历SQL字符串的每个字符
    while i < len(sql):
        # 尝试匹配当前位置的词法单元
        m = TOK_REGEX.match(sql, i)
        
        # 如果没有匹配到任何词法单元，说明遇到了非法字符
        if not m:
            raise LexError(f"非法字符 '{sql[i]}'", line, col)
        
        # 获取匹配的词法单元信息
        kind = m.lastgroup or ""  # 匹配到的词法单元类型
        text = m.group(0)         # 匹配到的文本内容
        start_col = col           # 记录开始列号
        
        # 计算换行符数量，更新行号和列号
        newlines = text.count("\n")
        if newlines:
            # 如果包含换行符，更新行号
            line += newlines
            # 计算新行的列号（最后一个换行符后的字符数 + 1）
            col = len(text.split("\n")[-1]) + 1
        else:
            # 如果没有换行符，只更新列号
            col += len(text)
        
        # 移动到下一个字符位置
        i = m.end()
        
        # 跳过空白字符（不加入Token列表）
        if kind == "WS":
            continue
        
        # 处理标识符：检查是否为关键字
        if kind == "IDENT" and text.lower() in KEYWORDS:
            # 如果是关键字，种别码使用大写形式
            tokens.append((text.upper(), text, line, start_col))
        elif kind == "STRING":
            # 处理字符串：去掉首尾的单引号
            tokens.append(("STRING", text[1:-1], line, start_col))
        else:
            # 其他词法单元：种别码使用大写形式
            tokens.append((kind.upper(), text, line, start_col))
    
    return tokens
