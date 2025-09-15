from typing import Any, Dict, Iterable, List, Optional

from execution.operators import Operator, Update, Drop
from execution.sytem_catalog import SystemCatalog

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

    def execute(self, op: Operator) -> Any:
        """执行操作符并返回结果"""
        results = self.execute_plan(op)
        if isinstance(op, Update):
            if results and "updated" in results[0]:
                count = results[0]["updated"]
                print(f"更新了 {count} 条记录")
                return count
        elif isinstance(op, Drop):
            if results and "dropped" in results[0]:
                table_name = results[0]["dropped"]
                status = results[0].get("status", "unknown")
                if status == "success" and table_name:
                    print(f"成功删除表 {table_name}")
                    return True
                else:
                    error = results[0].get("message", "未知错误")
                    print(f"删除表失败: {error}")
                    return False
        return results