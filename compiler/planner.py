from typing import Any, Dict, List, Optional

from execution.executor import Executor
from execution.operators import SeqScan, Filter, Project, Insert as OpInsert, Operator, CreateTable as OpCreate, Delete as OpDelete, make_predicate

class Planner:
    def __init__(self, executor: Executor) -> None:
        self.executor = executor

    def plan(self, analyzed) -> Operator:
        kind = analyzed.kind
        payload = analyzed.payload
        if kind == "select":
            table = payload["table"]
            columns = payload["columns"]
            where = payload.get("where")
            op: Operator = SeqScan(self.executor.catalog.get_table(table))
            if where is not None:
                col, op_str, val = where
                op = Filter(op, make_predicate(col, op_str, val))
            if columns != ["*"]:
                op = Project(op, columns)
            return op
        if kind == "insert":
            table = payload["table"]
            row = payload["row"]
            return OpInsert(self.executor.catalog.get_table(table), [row])
        if kind == "create_table":
            return OpCreate(self.executor.catalog, payload["table"], payload["columns"])
        if kind == "delete":
            table = self.executor.catalog.get_table(payload["table"])
            pred = None
            if payload.get("where") is not None:
                col, op_str, val = payload["where"]
                pred = make_predicate(col, op_str, val)
            return OpDelete(table, pred)
        raise ValueError("unsupported analyzed plan kind")
