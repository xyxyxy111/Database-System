"""
执行算子模块 (Execution Operators)
================================

本模块实现了数据库执行引擎的各种算子。
算子是实现具体数据库操作的执行单元，采用拉模型（Pull Model）执行。

主要算子：
1. SeqScan: 顺序扫描算子
2. Filter: 过滤算子
3. Project: 投影算子
4. Insert: 插入算子
5. CreateTable: 建表算子
6. Delete: 删除算子

执行模型：
- 拉模型：上层算子通过调用next()方法从下层算子拉取数据
- 三阶段：open() -> next() -> close()
- 流水线：算子可以组合形成执行计划树
"""

from typing import Any, Dict, Iterable, Iterator, List, Optional, Callable

from storage.table import Table
from execution.sytem_catalog import SystemCatalog

# 行数据类型：字典，键为列名，值为列值
Row = Dict[str, Any]


class Operator:
    """
    算子基类
    
    所有执行算子都继承自此类。
    采用拉模型执行，包含三个阶段的接口：
    - open(): 初始化算子
    - next(): 获取下一行数据
    - close(): 清理资源
    """
    
    def open(self) -> None:
        """
        打开算子
        
        初始化算子状态，准备开始执行。
        子类可以重写此方法进行特定的初始化。
        """
        pass

    def next(self) -> Optional[Row]:
        """
        获取下一行数据
        
        返回:
            Optional[Row]: 下一行数据，如果没有更多数据则返回None
            
        异常:
            NotImplementedError: 子类必须实现此方法
        """
        raise NotImplementedError

    def close(self) -> None:
        """
        关闭算子
        
        清理算子资源，结束执行。
        子类可以重写此方法进行特定的清理。
        """
        pass


class SeqScan(Operator):
    """
    顺序扫描算子
    
    对表进行顺序扫描，逐行返回表中的所有数据。
    这是最基本的算子，其他算子通常以此为基础。
    """
    
    def __init__(self, table: Table) -> None:
        """
        初始化顺序扫描算子
        
        参数:
            table (Table): 要扫描的表
        """
        self.table = table
        self._iter: Optional[Iterator[Row]] = None

    def open(self) -> None:
        """
        打开算子，初始化迭代器
        """
        self._iter = iter(self.table.scan())

    def next(self) -> Optional[Row]:
        """
        获取下一行数据
        
        返回:
            Optional[Row]: 下一行数据，如果没有更多数据则返回None
        """
        assert self._iter is not None, "operator not opened"
        try:
            return next(self._iter)
        except StopIteration:
            return None


class Filter(Operator):
    """
    过滤算子
    
    根据谓词函数过滤输入的行数据。
    只返回满足条件的行。
    """
    
    def __init__(self, child: Operator, predicate: Callable[[Row], bool]) -> None:
        """
        初始化过滤算子
        
        参数:
            child (Operator): 子算子（数据源）
            predicate (Callable[[Row], bool]): 谓词函数，返回True表示保留该行
        """
        self.child = child
        self.predicate = predicate

    def open(self) -> None:
        """
        打开算子，递归打开子算子
        """
        self.child.open()

    def next(self) -> Optional[Row]:
        """
        获取下一行满足条件的数据
        
        返回:
            Optional[Row]: 下一行满足条件的数据，如果没有更多数据则返回None
        """
        while True:
            row = self.child.next()
            if row is None:
                return None
            if self.predicate(row):
                return row

    def close(self) -> None:
        """
        关闭算子，递归关闭子算子
        """
        self.child.close()


class Project(Operator):
    """
    投影算子
    
    从输入行中选择指定的列，丢弃其他列。
    实现SELECT语句中的列选择功能。
    """
    
    def __init__(self, child: Operator, columns: List[str]) -> None:
        """
        初始化投影算子
        
        参数:
            child (Operator): 子算子（数据源）
            columns (List[str]): 要保留的列名列表
        """
        self.child = child
        self.columns = columns

    def open(self) -> None:
        """
        打开算子，递归打开子算子
        """
        self.child.open()

    def next(self) -> Optional[Row]:
        """
        获取下一行投影后的数据
        
        返回:
            Optional[Row]: 下一行投影后的数据，如果没有更多数据则返回None
        """
        row = self.child.next()
        if row is None:
            return None
        # 只保留指定的列
        return {col: row.get(col) for col in self.columns}

    def close(self) -> None:
        """
        关闭算子，递归关闭子算子
        """
        self.child.close()


class Insert(Operator):
    """
    插入算子
    
    将数据插入到指定的表中。
    实现INSERT语句的功能。
    """
    
    def __init__(self, table: Table, rows: Iterable[Row]) -> None:
        """
        初始化插入算子
        
        参数:
            table (Table): 目标表
            rows (Iterable[Row]): 要插入的行数据
        """
        self.table = table
        self.rows = iter(rows)
        self._done = False

    def open(self) -> None:
        """
        打开算子，重置完成状态
        """
        self._done = False

    def next(self) -> Optional[Row]:
        """
        执行插入操作
        
        返回:
            Optional[Row]: 插入结果，包含插入的行数
        """
        if self._done:
            return None
        
        count = 0
        for r in self.rows:
            self.table.insert(r)
            count += 1
        
        self._done = True
        return {"inserted": count}

    def close(self) -> None:
        """
        关闭算子
        """
        pass


class CreateTable(Operator):
    """
    建表算子
    
    创建新的表。
    实现CREATE TABLE语句的功能。
    """
    
    def __init__(self, syscat: SystemCatalog, name: str, columns: List[tuple]) -> None:
        """
        初始化建表算子
        
        参数:
            syscat (SystemCatalog): 系统目录
            name (str): 表名
            columns (List[tuple]): 列定义列表
        """
        self.syscat = syscat
        self.name = name
        self.columns = columns
        self._done = False

    def open(self) -> None:
        """
        打开算子，重置完成状态
        """
        self._done = False

    def next(self) -> Optional[Row]:
        """
        执行建表操作
        
        返回:
            Optional[Row]: 建表结果，包含创建的表名
        """
        if self._done:
            return None
        
        self.syscat.create_table(self.name, self.columns)
        self._done = True
        return {"created": self.name}

    def close(self) -> None:
        """
        关闭算子
        """
        pass


class Delete(Operator):
    """
    删除算子
    
    从表中删除满足条件的行。
    实现DELETE语句的功能。
    """
    
    def __init__(self, table: Table, predicate: Optional[Callable[[Row], bool]]) -> None:
        """
        初始化删除算子
        
        参数:
            table (Table): 目标表
            predicate (Optional[Callable[[Row], bool]]): 删除条件，None表示删除所有行
        """
        self.table = table
        self.predicate = predicate
        self._done = False

    def open(self) -> None:
        """
        打开算子，重置完成状态
        """
        self._done = False

    def next(self) -> Optional[Row]:
        """
        执行删除操作
        
        返回:
            Optional[Row]: 删除结果，包含删除的行数
        """
        if self._done:
            return None
        
        deleted = self.table.delete(self.predicate)
        self._done = True
        return {"deleted": deleted}

    def close(self) -> None:
        """
        关闭算子
        """
        pass


# 辅助函数

def make_predicate(col: str, op: str, val: Any) -> Callable[[Row], bool]:
    """
    创建谓词函数
    
    根据列名、操作符和值创建谓词函数，用于WHERE条件。
    
    参数:
        col (str): 列名
        op (str): 操作符（EQ, NE, GT, LT, GE, LE）
        val (Any): 比较值
        
    返回:
        Callable[[Row], bool]: 谓词函数
        
    支持的操作符:
        - EQ: 等于
        - NE: 不等于
        - GT: 大于
        - LT: 小于
        - GE: 大于等于
        - LE: 小于等于
    """
    if op == "EQ":
        return lambda r: r.get(col) == val
    if op == "NE":
        return lambda r: r.get(col) != val
    if op == "GT":
        return lambda r: (r.get(col) is not None) and (r.get(col) > val)
    if op == "LT":
        return lambda r: (r.get(col) is not None) and (r.get(col) < val)
    if op == "GE":
        return lambda r: (r.get(col) is not None) and (r.get(col) >= val)
    if op == "LE":
        return lambda r: (r.get(col) is not None) and (r.get(col) <= val)
    
    # 默认返回True（保留所有行）
    return lambda r: True
