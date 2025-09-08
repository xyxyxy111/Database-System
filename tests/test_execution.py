import os
import tempfile
import shutil
import unittest

from execution.sytem_catalog import SystemCatalog
from execution.executor import Executor
from execution.operators import SeqScan, Project, Filter, Insert, CreateTable, Delete, make_predicate


class TestExecutionLayer(unittest.TestCase):
    """
    执行层测试覆盖：
    1) CreateTable/Insert 算子能正确创建与插入
    2) SeqScan/Project/Filter 组合能返回正确结果
    3) Delete 算子能删除满足条件的行
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="mini_db_exec_")
        self.db_path = os.path.join(self.tmpdir, "test.db")
        self.syscat = SystemCatalog(self.db_path)
        self.executor = Executor(self.syscat)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_insert_scan_pipeline(self):
        # 1) 创建表 schema
        op_create = CreateTable(self.syscat, "t", [("id", "INT"), ("name", "VARCHAR")])
        res = self.executor.execute_plan(op_create)
        self.assertEqual(res[0]["created"], "t")
        # 2) 插入若干行
        rows = [{"id": i, "name": f"n{i}"} for i in range(5)]
        op_insert = Insert(self.syscat.get_table("t"), rows)
        res2 = self.executor.execute_plan(op_insert)
        self.assertEqual(res2[0]["inserted"], 5)
        # 3) 扫描与投影
        scan = SeqScan(self.syscat.get_table("t"))
        proj = Project(scan, ["name"])  # 仅投影 name 列
        got = self.executor.execute_plan(proj)
        self.assertEqual(len(got), 5)
        self.assertTrue(all("name" in r and len(r) == 1 for r in got))
        # 4) 过滤：id >= 3
        scan2 = SeqScan(self.syscat.get_table("t"))
        pred = make_predicate("id", "GE", 3)
        filt = Filter(scan2, pred)
        got2 = self.executor.execute_plan(filt)
        self.assertEqual({r["id"] for r in got2}, {3, 4})

    def test_delete_operator(self):
        # 建表与插入
        self.executor.execute_plan(CreateTable(self.syscat, "t2", [("id", "INT")]))
        self.executor.execute_plan(Insert(self.syscat.get_table("t2"), [{"id": i} for i in range(6)]))
        # 删除偶数 id
        dele = Delete(self.syscat.get_table("t2"), make_predicate("id", "EQ", 2))
        res = self.executor.execute_plan(dele)
        self.assertEqual(res[0]["deleted"], 1)
        # 再删 id>3
        dele2 = Delete(self.syscat.get_table("t2"), make_predicate("id", "GT", 3))
        res2 = self.executor.execute_plan(dele2)
        self.assertEqual(res2[0]["deleted"], 2)  # 删除 4,5


if __name__ == "__main__":
    unittest.main()
