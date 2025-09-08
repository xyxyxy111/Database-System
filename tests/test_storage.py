import os
import tempfile
import shutil
import unittest

# 被测模块：存储与表
from storage.page import Page, PAGE_SIZE
from storage.disk_manager import DiskManager
from storage.buffer_manager import BufferManager
from storage.table import Table


class TestStorageLayer(unittest.TestCase):
    """
    存储层测试覆盖：
    1) Page 序列化/反序列化 与 容量检查
    2) DiskManager 页分配/读写
    3) BufferManager LRU 缓存与统计
    4) Table 的 insert/scan/delete 基本功能
    """

    def setUp(self) -> None:
        # 每个用例创建独立的临时目录与数据库文件，避免相互干扰
        self.tmpdir = tempfile.mkdtemp(prefix="mini_db_test_")
        self.db_path = os.path.join(self.tmpdir, "test.db")
        self.disk = DiskManager(self.db_path)
        self.buffer = BufferManager(self.disk, capacity=2)  # 小容量便于触发逐出

    def tearDown(self) -> None:
        # 清理临时目录
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_page_serialize_roundtrip(self):
        # 构造一页，插入若干行，序列化再还原，内容应一致
        p = Page(page_id=123)
        self.assertTrue(p.insert_row({'a': 1}))
        self.assertTrue(p.insert_row({'a': 2, 'b': 'x'}))
        raw = p.to_bytes()
        self.assertEqual(len(raw), PAGE_SIZE)  # 固定页大小
        p2 = Page.from_bytes(raw)
        p2.page_id = p.page_id
        self.assertEqual(p2.get_rows(), p.get_rows())

    def test_disk_manager_allocate_and_rw(self):
        # 初始无页
        self.assertEqual(self.disk.num_pages(), 0)
        # 分配两页
        pid0 = self.disk.allocate_page()
        pid1 = self.disk.allocate_page()
        self.assertEqual(pid0, 0)
        self.assertEqual(pid1, 1)
        self.assertEqual(self.disk.num_pages(), 2)
        # 写入并读回
        page = Page(pid1, rows=[{"x": 42}])
        self.disk.write_page(pid1, page.to_bytes())
        raw = self.disk.read_page(pid1)
        back = Page.from_bytes(raw)
        back.page_id = pid1
        self.assertEqual(back.get_rows(), [{"x": 42}])

    def test_buffer_manager_lru_and_stats(self):
        # 准备三页数据，缓冲容量=2，将触发逐出
        pids = [self.disk.allocate_page() for _ in range(3)]
        # 写入不同内容
        for i, pid in enumerate(pids):
            page = Page(pid, rows=[{"pid": pid, "i": i}])
            self.disk.write_page(pid, page.to_bytes())
        # 访问前两页（miss 两次，放入缓存）
        self.buffer.get_page(pids[0])
        self.buffer.get_page(pids[1])
        # 再访问第三页（miss，逐出最久未使用页 pids[0]）
        self.buffer.get_page(pids[2])
        # 再次访问 pids[1]（hit）与 pids[0]（miss, 因已被逐出）
        self.buffer.get_page(pids[1])
        self.buffer.get_page(pids[0])
        hits, misses, evictions = self.buffer.stats()
        # 期望：miss 至少 4 次（前3次首次读+最后一次重读被逐出的页），hit 至少 1 次
        self.assertGreaterEqual(misses, 4)
        self.assertGreaterEqual(hits, 1)
        self.assertGreaterEqual(evictions, 1)

    def test_table_insert_scan_delete(self):
        # 构建表并插入多行，跨页以验证分页插入/扫描
        table = Table(self.buffer, name="t")
        total_rows = 200
        for i in range(total_rows):
            table.insert({"id": i, "val": f"v{i}"})
        # 全表扫描校验
        scanned = list(table.scan())
        self.assertEqual(len(scanned), total_rows)
        # 删除一半（偶数 id）
        deleted = table.delete(lambda r: r.get("id", -1) % 2 == 0)
        self.assertEqual(deleted, total_rows // 2)
        # 再扫描验证仅剩奇数 id
        left = list(table.scan())
        self.assertTrue(all(r["id"] % 2 == 1 for r in left))
        self.assertEqual(len(left), total_rows - deleted)


if __name__ == "__main__":
    unittest.main()
