"""
语法分析器模块 (Parser)
=====================

本模块负责将词法分析器生成的Token序列解析为抽象语法树(AST)。
语法分析是编译过程的第二步，它根据SQL语法规则构建语法树。

主要功能：
1. 解析CREATE TABLE语句
2. 解析INSERT语句
3. 解析SELECT语句（支持WHERE子句）
4. 解析DELETE语句（支持WHERE子句）
5. 提供详细的语法错误信息

解析方法：递归下降分析法
"""

from typing import Any, Dict, List, Optional, Tuple

from .lexer import tokenize, Token


class AST:
    """
    抽象语法树基类
    
    所有SQL语句的AST节点都继承自此类。
    AST是源代码的树形表示，便于后续的语义分析和代码生成。
    """
    pass


class Select(AST):
    """
    SELECT语句的AST节点
    
    表示一个SELECT查询语句，包含：
    - columns: 要查询的列名列表
    - table: 要查询的表名
    - where: WHERE条件（可选），格式为(列名, 操作符, 值)
    """
    def __init__(self, columns: List[str], table: str, where: Optional[Tuple[str, str, Any]] = None) -> None:
        self.columns = columns  # 列名列表，如["id", "name"]或["*"]
        self.table = table     # 表名
        self.where = where    # WHERE条件：(列名, 操作符, 值)，如("age", "GT", 18)


class Insert(AST):
    """
    INSERT语句的AST节点
    
    表示一个INSERT插入语句，包含：
    - table: 目标表名
    - columns: 要插入的列名列表
    - values: 对应的值列表
    """
    def __init__(self, table: str, columns: List[str], values: List[Any]) -> None:
        self.table = table    # 目标表名
        self.columns = columns  # 列名列表
        self.values = values    # 值列表，与columns一一对应


class CreateTable(AST):
    """
    CREATE TABLE语句的AST节点
    
    表示一个CREATE TABLE建表语句，包含：
    - table: 要创建的表名
    - columns: 列定义列表，每个元素为(列名, 类型)
    """
    def __init__(self, table: str, columns: List[Tuple[str, str]]) -> None:
        self.table = table    # 表名
        self.columns = columns  # 列定义列表：[(列名, 类型), ...]，类型为"INT"或"VARCHAR"


class Delete(AST):
    """
    DELETE语句的AST节点
    
    表示一个DELETE删除语句，包含：
    - table: 目标表名
    - where: WHERE条件（可选），格式为(列名, 操作符, 值)
    """
    def __init__(self, table: str, where: Optional[Tuple[str, str, Any]]) -> None:
        self.table = table  # 目标表名
        self.where = where  # WHERE条件：(列名, 操作符, 值)


class Parser:
    """
    语法分析器主类
    
    使用递归下降分析法解析SQL语句。
    递归下降分析法是一种自顶向下的语法分析方法，
    为每个语法规则编写一个对应的解析函数。
    """
    
    def __init__(self, sql: str) -> None:
        """
        初始化语法分析器
        
        参数:
            sql (str): 要解析的SQL源代码字符串
        """
        self.tokens = tokenize(sql)  # 调用词法分析器生成Token序列
        self.pos = 0                 # 当前Token位置指针

    def _peek(self) -> Optional[Token]:
        """
        查看当前Token但不消费（不移动位置指针）
        
        返回:
            Optional[Token]: 当前Token，如果已到末尾则返回None
        """
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def _eat(self, kind: str) -> Token:
        """
        消费（读取并移动）当前Token
        
        如果当前Token的类型不匹配期望的类型，抛出语法错误。
        
        参数:
            kind (str): 期望的Token类型
            
        返回:
            Token: 消费的Token
            
        异常:
            SyntaxError: 当Token类型不匹配时抛出
        """
        tok = self._peek()
        if tok is None or tok[0] != kind:
            raise SyntaxError(self._expect_msg(kind))
        self.pos += 1
        return tok

    def _expect_msg(self, kind: str) -> str:
        """
        生成期望Token的错误消息
        
        参数:
            kind (str): 期望的Token类型
            
        返回:
            str: 格式化的错误消息
        """
        tok = self._peek()
        if tok is None:
            return f"Expected {kind}, got EOF"
        t, lex, line, col = tok
        return f"Expected {kind}, got {t}('{lex}') at {line}:{col}"

    def parse_many(self) -> List[AST]:
        """
        解析多条SQL语句（用分号分隔）
        
        返回:
            List[AST]: AST节点列表
        """
        asts: List[AST] = []
        # 循环解析直到所有Token都被消费
        while self.pos < len(self.tokens):
            asts.append(self.parse())  # 解析一条语句
            # 如果下一条是分号，消费它
            if self._peek() and self._peek()[0] == "SEMI":
                self._eat("SEMI")
        return asts

    def parse(self) -> AST:
        """
        解析一条SQL语句
        
        根据当前Token的类型选择相应的解析函数。
        
        返回:
            AST: 解析出的AST节点
            
        异常:
            SyntaxError: 当遇到不支持的语句类型时抛出
        """
        tok = self._peek()
        if tok is None:
            raise SyntaxError("empty input")
        
        # 根据语句类型选择相应的解析函数
        if tok[0] == "SELECT":
            return self._parse_select()
        if tok[0] == "INSERT":
            return self._parse_insert()
        if tok[0] == "CREATE":
            return self._parse_create_table()
        if tok[0] == "DELETE":
            return self._parse_delete()
        
        raise SyntaxError(f"unsupported statement {tok}")

    def _parse_where_clause(self) -> Optional[Tuple[str, str, Any]]:
        """
        解析WHERE子句
        
        WHERE子句格式：WHERE 列名 操作符 值
        支持的操作符：=, !=, <>, >, <, >=, <=
        
        返回:
            Optional[Tuple[str, str, Any]]: WHERE条件元组(列名, 操作符, 值)，
                                           如果没有WHERE子句则返回None
        """
        tok = self._peek()
        if tok and tok[0] == "WHERE":
            self._eat("WHERE")  # 消费WHERE关键字
            
            # 解析列名
            col = self._eat("IDENT")[1]
            
            # 解析比较操作符
            op_tok = self._peek()
            if op_tok is None:
                raise SyntaxError(self._expect_msg("comparison operator"))
            op = op_tok[0]
            if op not in ("EQ", "NE", "GT", "LT", "GE", "LE"):
                raise SyntaxError(self._expect_msg("comparison operator (=,<> ,!=, >, <, >=, <=)"))
            self.pos += 1
            
            # 解析值（字符串或数字）
            val_tok = self._peek()
            if val_tok is None:
                raise SyntaxError(self._expect_msg("literal value"))
            
            if val_tok[0] == "STRING":
                val = val_tok[1]  # 字符串值
                self.pos += 1
            elif val_tok[0] == "NUMBER":
                text = val_tok[1]
                # 尝试转换为整数，失败则转换为浮点数
                val = int(text) if isinstance(text, str) and text.isdigit() else float(text)
                self.pos += 1
            else:
                raise SyntaxError(self._expect_msg("literal value"))
            
            return (col, op, val)
        return None

    def _parse_select(self) -> Select:
        """
        解析SELECT语句
        
        SELECT语句格式：
        SELECT 列名列表 FROM 表名 [WHERE 条件]
        
        返回:
            Select: SELECT语句的AST节点
        """
        self._eat("SELECT")  # 消费SELECT关键字
        
        # 解析列名列表
        cols: List[str] = []
        tok = self._peek()
        if tok and tok[0] == "STAR":
            # SELECT * 的情况
            self._eat("STAR")
            cols = ["*"]
        else:
            # SELECT 列名1, 列名2, ... 的情况
            while True:
                ident = self._eat("IDENT")[1]
                cols.append(ident)
                tok = self._peek()
                if tok and tok[0] == "COMMA":
                    self._eat("COMMA")  # 消费逗号
                    continue
                break
        
        self._eat("FROM")  # 消费FROM关键字
        table = self._eat("IDENT")[1]  # 解析表名
        
        # 解析可选的WHERE子句
        where = self._parse_where_clause()
        
        return Select(cols, table, where)

    def _parse_insert(self) -> Insert:
        """
        解析INSERT语句
        
        INSERT语句格式：
        INSERT INTO 表名(列名列表) VALUES(值列表)
        
        返回:
            Insert: INSERT语句的AST节点
        """
        self._eat("INSERT")  # 消费INSERT关键字
        self._eat("INTO")    # 消费INTO关键字
        table = self._eat("IDENT")[1]  # 解析表名
        
        # 解析列名列表
        self._eat("LPAREN")  # 消费左括号
        columns: List[str] = []
        while True:
            columns.append(self._eat("IDENT")[1])
            tok = self._peek()
            if tok and tok[0] == "COMMA":
                self._eat("COMMA")
                continue
            break
        self._eat("RPAREN")  # 消费右括号
        
        # 解析值列表
        self._eat("VALUES")  # 消费VALUES关键字
        self._eat("LPAREN")  # 消费左括号
        values: List[Any] = []
        while True:
            tok = self._peek()
            if tok is None:
                raise SyntaxError(self._expect_msg("value in VALUES"))
            
            if tok[0] == "STRING":
                values.append(tok[1])  # 字符串值
                self.pos += 1
            elif tok[0] == "NUMBER":
                text = tok[1]
                # 尝试转换为整数，失败则转换为浮点数
                values.append(int(text) if isinstance(text, str) and text.isdigit() else float(text))
                self.pos += 1
            else:
                raise SyntaxError(self._expect_msg("literal value in VALUES"))
            
            tok = self._peek()
            if tok and tok[0] == "COMMA":
                self._eat("COMMA")
                continue
            break
        self._eat("RPAREN")  # 消费右括号
        
        return Insert(table, columns, values)

    def _parse_create_table(self) -> CreateTable:
        """
        解析CREATE TABLE语句
        
        CREATE TABLE语句格式：
        CREATE TABLE 表名(列定义列表)
        列定义格式：列名 类型
        
        返回:
            CreateTable: CREATE TABLE语句的AST节点
        """
        self._eat("CREATE")  # 消费CREATE关键字
        self._eat("TABLE")  # 消费TABLE关键字
        table = self._eat("IDENT")[1]  # 解析表名
        
        # 解析列定义列表
        self._eat("LPAREN")  # 消费左括号
        cols: List[Tuple[str, str]] = []
        while True:
            name = self._eat("IDENT")[1]  # 解析列名
            
            # 解析列类型
            typ_tok = self._peek()
            if typ_tok is None:
                raise SyntaxError(self._expect_msg("type (INT or VARCHAR)"))
            
            if typ_tok[0] in ("INT", "VARCHAR"):
                # 类型是关键字
                typ_name = typ_tok[0]
                self.pos += 1
            elif typ_tok[0] == "IDENT":
                # 类型是标识符
                typ_name = typ_tok[1].upper()
                self.pos += 1
            else:
                raise SyntaxError(self._expect_msg("type (INT or VARCHAR)"))
            
            # 检查类型是否支持
            if typ_name not in ("INT", "VARCHAR"):
                raise SyntaxError(f"unsupported type {typ_name}")
            
            cols.append((name, typ_name))
            
            tok = self._peek()
            if tok and tok[0] == "COMMA":
                self._eat("COMMA")
                continue
            break
        self._eat("RPAREN")  # 消费右括号
        
        return CreateTable(table, cols)

    def _parse_delete(self) -> Delete:
        """
        解析DELETE语句
        
        DELETE语句格式：
        DELETE FROM 表名 [WHERE 条件]
        
        返回:
            Delete: DELETE语句的AST节点
        """
        self._eat("DELETE")  # 消费DELETE关键字
        self._eat("FROM")    # 消费FROM关键字
        table = self._eat("IDENT")[1]  # 解析表名
        
        # 解析可选的WHERE子句
        where = self._parse_where_clause()
        
        return Delete(table, where)
