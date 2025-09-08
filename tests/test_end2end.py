import os
import tempfile
import shutil
import unittest

from compiler.parser import Parser
from compiler.sematic_analyzer import SemanticAnalyzer
from compiler.planner import Planner
from execution.sytem_catalog import SystemCatalog
from execution.executor import Executor


class TestEndToEnd(unittest.TestCase):
    """
    端到端测试：
    1) 多语句顺序执行（建表→插入→查询→删除→再查询）
    2) 重启后持久化验证（目录与数据仍可访问）
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="mini_db_e2e_")
        self.db_path = os.path.join(self.tmpdir, "test.db")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _run_sqls(self, db_path: str, sql: str):
        syscat = SystemCatalog(db_path)
        executor = Executor(syscat)
        analyzer = SemanticAnalyzer(syscat)
        parser = Parser(sql)
        asts = parser.parse_many()
        results = []
        for ast in asts:
            a = analyzer.analyze(ast)
            op = Planner(executor).plan(a)
            results.extend(executor.execute_plan(op))
        return results

    def test_full_flow_and_persistence(self):
        sql = """
        CREATE TABLE student(id INT, name VARCHAR, age INT);
        INSERT INTO student(id,name,age) VALUES (1,'Alice',20);
        INSERT INTO student(id,name,age) VALUES (2,'Bob',17);
        SELECT id,name FROM student WHERE age >= 18;
        DELETE FROM student WHERE id = 1;
        SELECT * FROM student;
        """
        res = self._run_sqls(self.db_path, sql)
        # 期望第四条 SELECT 返回 Alice
        self.assertIn({'id': 1, 'name': 'Alice'}, res)
        # 删除后最终 SELECT 只剩 Bob 一条
        tail = res[-1]  # 最后一次查询的最后一行
        self.assertEqual(tail.get('name'), 'Bob')
        # 重启再查验证目录与数据持久
        res2 = self._run_sqls(self.db_path, "SELECT * FROM student;")
        names = {r.get('name') for r in res2}
        self.assertIn('Bob', names)
        self.assertNotIn('Alice', names)


if __name__ == "__main__":
    unittest.main()
