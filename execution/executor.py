from typing import Any, Dict, Iterable, List, Optional

from .operators import Operator
from .sytem_catalog import SystemCatalog

Row = Dict[str, Any]

class Executor:
    def __init__(self, catalog: SystemCatalog) -> None:
        self.catalog = catalog

    def execute_plan(self, root: Operator) -> List[Row]:
        results: List[Row] = []
        root.open()
        try:
            while True:
                row = root.next()
                if row is None:
                    break
                results.append(row)
        finally:
            root.close()
        return results

    # 便捷方法可按需加入
