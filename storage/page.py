import pickle
from typing import Any, Dict, List, Optional, Tuple

PAGE_SIZE = 4096

class Page:
    """
    简易页格式：
    - 使用 Python pickle 将 {"page_id": int, "rows": List[Dict[str, Any]]} 序列化到定长 PAGE_SIZE 字节。
    - 仅支持追加式插入与顺序扫描。
    """

    def __init__(self, page_id: int, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self.page_id = page_id
        self.rows: List[Dict[str, Any]] = rows or []

    def capacity_left(self) -> int:
        data = {"page_id": self.page_id, "rows": self.rows}
        raw = pickle.dumps(data)
        return PAGE_SIZE - len(raw)

    def can_insert(self, row: Dict[str, Any]) -> bool:
        data = {"page_id": self.page_id, "rows": self.rows + [row]}
        raw = pickle.dumps(data)
        return len(raw) <= PAGE_SIZE

    def insert_row(self, row: Dict[str, Any]) -> bool:
        if self.can_insert(row):
            self.rows.append(row)
            return True
        return False

    def get_rows(self) -> List[Dict[str, Any]]:
        return list(self.rows)

    def to_bytes(self) -> bytes:
        data = {"page_id": self.page_id, "rows": self.rows}
        raw = pickle.dumps(data)
        if len(raw) > PAGE_SIZE:
            raise ValueError("Page overflow: serialized data exceeds PAGE_SIZE")
        return raw.ljust(PAGE_SIZE, b"\x00")

    @staticmethod
    def from_bytes(raw: bytes) -> "Page":
        # 去除右侧 padding 0
        payload = raw.rstrip(b"\x00")
        if not payload:
            # 空页（新分配的页）
            return Page(page_id=-1, rows=[])
        data = pickle.loads(payload)
        page = Page(page_id=int(data.get("page_id", -1)), rows=list(data.get("rows", [])))
        return page
