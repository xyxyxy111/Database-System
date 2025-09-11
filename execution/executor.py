"""
执行器模块 (Executor)
===================

本模块实现了数据库的执行器。
执行器负责执行查询计划，协调各个算子的执行。

主要功能：
1. 执行查询计划
2. 管理算子的生命周期
3. 收集执行结果
4. 异常处理和资源清理

执行模型：
- 拉模型：从根算子开始，逐层拉取数据
- 三阶段：open() -> next() -> close()
- 异常安全：使用try-finally确保资源清理
"""

from typing import Any, Dict, Iterable, List, Optional

from execution.operators import Operator
from execution.sytem_catalog import SystemCatalog

# 行数据类型：字典，键为列名，值为列值
Row = Dict[str, Any]


class Executor:
    """
    执行器类
    
    负责执行查询计划，协调各个算子的执行。
    采用拉模型执行，从根算子开始逐层拉取数据。
    """
    
    def __init__(self, catalog: SystemCatalog) -> None:
        """
        初始化执行器
        
        参数:
            catalog (SystemCatalog): 系统目录，用于获取表信息
        """
        self.catalog = catalog

    def execute_plan(self, root: Operator) -> List[Row]:
        """
        执行查询计划
        
        执行以root为根算子的查询计划，收集所有结果。
        
        参数:
            root (Operator): 查询计划的根算子
            
        返回:
            List[Row]: 执行结果列表
            
        执行流程:
            1. 打开根算子（初始化）
            2. 循环调用next()获取数据
            3. 收集所有结果
            4. 关闭根算子（清理资源）
            
        异常处理:
            - 使用try-finally确保资源清理
            - 即使发生异常也会正确关闭算子
        """
        results: List[Row] = []
        
        # 打开根算子，开始执行
        root.open()
        
        try:
            # 循环拉取数据
            while True:
                row = root.next()
                if row is None:
                    break  # 没有更多数据
                results.append(row)
        finally:
            # 确保关闭算子，清理资源
            root.close()
        
        return results

    # 便捷方法可按需加入
    # 例如：execute_sql(), explain_plan() 等
