import os
import tempfile
import shutil
import unittest

from compiler.lexer import tokenize, LexError
from compiler.parser import Parser, Select, Insert, CreateTable, Delete
from compiler.sematic_analyzer import SemanticAnalyzer, SemanticError
from compiler.planner import Planner
from execution.sytem_catalog import SystemCatalog
from execution.executor import Executor


class TestCompilerLayer(unittest.TestCase):
    """
    编译器层测试覆盖：
    1) 词法：关键字/标识符/常量/运算符/分隔符识别与位置
    2) 语法：四类语句 AST 解析
    3) 语义：存在性/类型/列数列序检查
    4) 计划：根算子类型检查
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="mini_db_comp_")
        self.db_path = os.path.join(self.tmpdir, "test.db")
        self.syscat = SystemCatalog(self.db_path)
        self.executor = Executor(self.syscat)
        self.analyzer = SemanticAnalyzer(self.syscat)
        self.planner = Planner(self.executor)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_lexer_tokens_and_positions(self):
        sql = "SELECT id, name FROM t WHERE age >= 18;"
        toks = tokenize(sql)
        # 至少应包含 SELECT/IDENT/COMMA/IDENT/FROM/IDENT/WHERE/IDENT/GE/NUMBER/SEMI
        kinds = [t[0] for t in toks]
        self.assertIn("SELECT", kinds)
        self.assertIn("FROM", kinds)
        self.assertIn("WHERE", kinds)
        self.assertIn("GE", kinds)
        self.assertIn("SEMI", kinds)
        # 检查位置信息（行列号应为正数）
        for _, _, line, col in toks:
            self.assertGreaterEqual(line, 1)
            self.assertGreaterEqual(col, 1)
        # 非法字符触发错误
        with self.assertRaises(LexError):
            tokenize("SELECT * FROM t \x00;")

    def test_parser_ast(self):
        sql = """
        CREATE TABLE s(id INT, name VARCHAR);
        INSERT INTO s(id,name) VALUES (1,'A');
        SELECT id FROM s WHERE id = 1;
        DELETE FROM s WHERE id = 1;
        """
        p = Parser(sql)
        asts = p.parse_many()
        self.assertEqual(len(asts), 4)
        self.assertIsInstance(asts[0], CreateTable)
        self.assertIsInstance(asts[1], Insert)
        self.assertIsInstance(asts[2], Select)
        self.assertIsInstance(asts[3], Delete)

    def test_semantic_and_planner(self):
        # 创建表
        ast = Parser("CREATE TABLE s(id INT, name VARCHAR);").parse()
        a = self.analyzer.analyze(ast)
        op = self.planner.plan(a)
        # 根算子应为 CreateTable
        self.assertEqual(op.__class__.__name__, "CreateTable")
        # 执行建表
        self.executor.execute_plan(op)
        # 插入类型正确
        ast2 = Parser("INSERT INTO s(id,name) VALUES (1,'A');").parse()
        a2 = self.analyzer.analyze(ast2)
        self.executor.execute_plan(self.planner.plan(a2))
        # 插入类型错误（name 期望 VARCHAR）
        with self.assertRaises(SemanticError):
            bad = Parser("INSERT INTO s(id,name) VALUES (2,2);").parse()
            self.analyzer.analyze(bad)
        # 查询未知列
        with self.assertRaises(SemanticError):
            bad_q = Parser("SELECT foo FROM s;").parse()
            self.analyzer.analyze(bad_q)


if __name__ == "__main__":
    unittest.main()
