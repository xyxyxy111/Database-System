"""
查询规划器模块 (Query Planner)
============================

本模块负责将语义分析的结果转换为逻辑执行计划。
查询规划是编译过程的第四步，它将高级的SQL操作转换为具体的执行算子。

主要功能：
1. 将语义分析结果转换为执行算子
2. 构建执行计划树
3. 优化执行计划（当前版本为简单实现）
4. 处理不同类型的SQL操作

支持的算子：
- CreateTable: 创建表
- Insert: 插入数据
- SeqScan: 顺序扫描
- Filter: 条件过滤
- Project: 列投影
- Delete: 删除数据
"""

from typing import Any, Dict, List, Optional

from execution.executor import Executor
from execution.operators import (
    SeqScan, Filter, Project, Insert as OpInsert, Operator, 
    CreateTable as OpCreate, Delete as OpDelete, make_predicate
)


class Planner:
    """
    查询规划器主类
    
    负责将语义分析的结果转换为可执行的算子树。
    查询规划器是连接高级SQL语言和底层执行引擎的桥梁。
    """
    
    def __init__(self, executor: Executor) -> None:
        """
        初始化查询规划器
        
        参数:
            executor (Executor): 执行器，用于获取表和目录信息
        """
        self.executor = executor

    def plan(self, analyzed) -> Operator:
        """
        查询规划主函数
        
        根据语义分析的结果生成相应的执行计划。
        
        参数:
            analyzed: 语义分析结果，包含操作类型和相关信息
            
        返回:
            Operator: 执行计划的根算子
            
        异常:
            ValueError: 当遇到不支持的操作类型时抛出
        """
        kind = analyzed.kind      # 操作类型
        payload = analyzed.payload  # 操作信息
        
        # 根据操作类型生成相应的执行计划
        if kind == "select":
            return self._plan_select(payload)
        elif kind == "insert":
            return self._plan_insert(payload)
        elif kind == "create_table":
            return self._plan_create_table(payload)
        elif kind == "delete":
            return self._plan_delete(payload)
        else:
            raise ValueError("unsupported analyzed plan kind")

    def _plan_select(self, payload: Dict[str, Any]) -> Operator:
        """
        规划SELECT查询
        
        构建SELECT查询的执行计划：
        1. 从顺序扫描开始
        2. 如果有WHERE条件，添加过滤算子
        3. 如果不是SELECT *，添加投影算子
        
        参数:
            payload (Dict[str, Any]): SELECT操作的信息
            
        返回:
            Operator: SELECT查询的执行计划根算子
        """
        table = payload["table"]    # 表名
        columns = payload["columns"]  # 要查询的列
        where = payload.get("where")  # WHERE条件（可选）
        
        # 从顺序扫描开始
        op: Operator = SeqScan(self.executor.catalog.get_table(table))
        
        # 如果有WHERE条件，添加过滤算子
        if where is not None:
            col, op_str, val = where
            # 创建谓词函数
            predicate = make_predicate(col, op_str, val)
            # 将过滤算子包装在扫描算子外面
            op = Filter(op, predicate)
        
        # 如果不是SELECT *，添加投影算子
        if columns != ["*"]:
            op = Project(op, columns)
        
        return op

    def _plan_insert(self, payload: Dict[str, Any]) -> Operator:
        """
        规划INSERT操作
        
        构建INSERT操作的执行计划。
        
        参数:
            payload (Dict[str, Any]): INSERT操作的信息
            
        返回:
            Operator: INSERT操作的执行计划
        """
        table = payload["table"]  # 表名
        row = payload["row"]     # 要插入的行数据
        
        # 创建插入算子
        return OpInsert(self.executor.catalog.get_table(table), [row])

    def _plan_create_table(self, payload: Dict[str, Any]) -> Operator:
        """
        规划CREATE TABLE操作
        
        构建CREATE TABLE操作的执行计划。
        
        参数:
            payload (Dict[str, Any]): CREATE TABLE操作的信息
            
        返回:
            Operator: CREATE TABLE操作的执行计划
        """
        table = payload["table"]      # 表名
        columns = payload["columns"]  # 列定义
        
        # 创建建表算子
        return OpCreate(self.executor.catalog, table, columns)

    def _plan_delete(self, payload: Dict[str, Any]) -> Operator:
        """
        规划DELETE操作
        
        构建DELETE操作的执行计划。
        
        参数:
            payload (Dict[str, Any]): DELETE操作的信息
            
        返回:
            Operator: DELETE操作的执行计划
        """
        table = self.executor.catalog.get_table(payload["table"])  # 获取表对象
        where = payload.get("where")  # WHERE条件（可选）
        
        # 创建谓词函数（如果有WHERE条件）
        pred = None
        if where is not None:
            col, op_str, val = where
            pred = make_predicate(col, op_str, val)
        
        # 创建删除算子
        return OpDelete(table, pred)
