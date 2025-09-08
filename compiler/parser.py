from typing import Any, Dict, List, Optional, Tuple

from .lexer import tokenize, Token

class AST:
    pass

class Select(AST):
    def __init__(self, columns: List[str], table: str, where: Optional[Tuple[str, str, Any]] = None) -> None:
        self.columns = columns
        self.table = table
        self.where = where  # (opd1, op, opd2) simplified as (col, op, value)

class Insert(AST):
    def __init__(self, table: str, columns: List[str], values: List[Any]) -> None:
        self.table = table
        self.columns = columns
        self.values = values

class CreateTable(AST):
    def __init__(self, table: str, columns: List[Tuple[str, str]]) -> None:
        self.table = table
        self.columns = columns  # [(name, type)] where type in {INT, VARCHAR}

class Delete(AST):
    def __init__(self, table: str, where: Optional[Tuple[str, str, Any]]) -> None:
        self.table = table
        self.where = where

class Parser:
    def __init__(self, sql: str) -> None:
        self.tokens = tokenize(sql)
        self.pos = 0

    def _peek(self) -> Optional[Token]:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def _eat(self, kind: str) -> Token:
        tok = self._peek()
        if tok is None or tok[0] != kind:
            raise SyntaxError(self._expect_msg(kind))
        self.pos += 1
        return tok

    def _expect_msg(self, kind: str) -> str:
        tok = self._peek()
        if tok is None:
            return f"Expected {kind}, got EOF"
        t, lex, line, col = tok
        return f"Expected {kind}, got {t}('{lex}') at {line}:{col}"

    def parse_many(self) -> List[AST]:
        asts: List[AST] = []
        while self.pos < len(self.tokens):
            asts.append(self.parse())
            if self._peek() and self._peek()[0] == "SEMI":
                self._eat("SEMI")
        return asts

    def parse(self) -> AST:
        tok = self._peek()
        if tok is None:
            raise SyntaxError("empty input")
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
        tok = self._peek()
        if tok and tok[0] == "WHERE":
            self._eat("WHERE")
            col = self._eat("IDENT")[1]
            op_tok = self._peek()
            if op_tok is None:
                raise SyntaxError(self._expect_msg("comparison operator"))
            op = op_tok[0]
            if op not in ("EQ", "NE", "GT", "LT", "GE", "LE"):
                raise SyntaxError(self._expect_msg("comparison operator (=,<> ,!=, >, <, >=, <=)"))
            self.pos += 1
            val_tok = self._peek()
            if val_tok is None:
                raise SyntaxError(self._expect_msg("literal value"))
            if val_tok[0] == "STRING":
                val = val_tok[1]
                self.pos += 1
            elif val_tok[0] == "NUMBER":
                text = val_tok[1]
                val = int(text) if isinstance(text, str) and text.isdigit() else float(text)
                self.pos += 1
            else:
                raise SyntaxError(self._expect_msg("literal value"))
            return (col, op, val)
        return None

    def _parse_select(self) -> Select:
        self._eat("SELECT")
        cols: List[str] = []
        tok = self._peek()
        if tok and tok[0] == "STAR":
            self._eat("STAR")
            cols = ["*"]
        else:
            while True:
                ident = self._eat("IDENT")[1]
                cols.append(ident)
                tok = self._peek()
                if tok and tok[0] == "COMMA":
                    self._eat("COMMA")
                    continue
                break
        self._eat("FROM")
        table = self._eat("IDENT")[1]
        where = self._parse_where_clause()
        return Select(cols, table, where)

    def _parse_insert(self) -> Insert:
        self._eat("INSERT")
        self._eat("INTO")
        table = self._eat("IDENT")[1]
        self._eat("LPAREN")
        columns: List[str] = []
        while True:
            columns.append(self._eat("IDENT")[1])
            tok = self._peek()
            if tok and tok[0] == "COMMA":
                self._eat("COMMA")
                continue
            break
        self._eat("RPAREN")
        self._eat("VALUES")
        self._eat("LPAREN")
        values: List[Any] = []
        while True:
            tok = self._peek()
            if tok is None:
                raise SyntaxError(self._expect_msg("value in VALUES"))
            if tok[0] == "STRING":
                values.append(tok[1])
                self.pos += 1
            elif tok[0] == "NUMBER":
                text = tok[1]
                values.append(int(text) if isinstance(text, str) and text.isdigit() else float(text))
                self.pos += 1
            else:
                raise SyntaxError(self._expect_msg("literal value in VALUES"))
            tok = self._peek()
            if tok and tok[0] == "COMMA":
                self._eat("COMMA")
                continue
            break
        self._eat("RPAREN")
        return Insert(table, columns, values)

    def _parse_create_table(self) -> CreateTable:
        self._eat("CREATE")
        self._eat("TABLE")
        table = self._eat("IDENT")[1]
        self._eat("LPAREN")
        cols: List[Tuple[str, str]] = []
        while True:
            name = self._eat("IDENT")[1]
            typ_tok = self._eat("IDENT")[1].upper()
            if typ_tok not in ("INT", "VARCHAR"):
                raise SyntaxError(f"unsupported type {typ_tok}")
            cols.append((name, typ_tok))
            tok = self._peek()
            if tok and tok[0] == "COMMA":
                self._eat("COMMA")
                continue
            break
        self._eat("RPAREN")
        return CreateTable(table, cols)

    def _parse_delete(self) -> Delete:
        self._eat("DELETE")
        self._eat("FROM")
        table = self._eat("IDENT")[1]
        where = self._parse_where_clause()
        return Delete(table, where)
