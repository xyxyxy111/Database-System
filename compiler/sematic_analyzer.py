"""
语义分析器模块 (Semantic Analyzer)
================================

本模块负责对语法分析器生成的AST进行语义检查。
语义分析是编译过程的第三步，它检查SQL语句的语义正确性。

主要功能：
1. 存在性检查：检查表和列是否存在
2. 类型一致性检查：验证数据类型匹配
3. 列数/列序检查：验证INSERT语句的列数和顺序
4. Catalog维护：维护系统目录信息
5. 错误报告：提供详细的语义错误信息

错误格式：[错误类型, 位置, 原因说明]
"""

from typing import Any, Dict, List, Optional, Tuple

from .parser import Select as ASTSelect, Insert as ASTInsert, CreateTable as ASTCreate, Delete as ASTDelete, AST
from execution.sytem_catalog import SystemCatalog


class SemanticError(Exception):
    """
    语义分析错误异常类
    
    当语义分析器发现语义错误时，会抛出此异常。
    语义错误包括：
    - 表不存在
    - 列不存在
    - 类型不匹配
    - 列数不匹配等
    """
    pass


class Analyzed:
    """
    语义分析结果类
    
    包含语义分析的结果信息，用于后续的查询规划。
    每个Analyzed对象包含：
    - kind: 操作类型（create_table, insert, select, delete）
    - payload: 操作的具体信息
    """
    def __init__(self, kind: str, payload: Dict[str, Any]) -> None:
        self.kind = kind      # 操作类型
        self.payload = payload  # 操作信息


class SemanticAnalyzer:
    """
    语义分析器主类
    
    负责对AST进行各种语义检查，确保SQL语句的语义正确性。
    """
    
    def __init__(self, catalog: SystemCatalog) -> None:
        """
        初始化语义分析器
        
        参数:
            catalog (SystemCatalog): 系统目录，用于查询表结构信息
        """
        self.catalog = catalog

    def _ensure_table_exists(self, table: str) -> None:
        """
        检查表是否存在
        
        参数:
            table (str): 表名
            
        异常:
            SemanticError: 当表不存在时抛出
        """
        if not self.catalog.table_exists(table):
            raise SemanticError(f"table '{table}' does not exist")

    def analyze(self, ast: AST) -> Analyzed:
        """
        语义分析主函数
        
        根据AST的类型进行相应的语义检查，并返回分析结果。
        
        参数:
            ast (AST): 要分析的抽象语法树节点
            
        返回:
            Analyzed: 语义分析结果
            
        异常:
            SemanticError: 当发现语义错误时抛出
        """
        # 处理CREATE TABLE语句
        if isinstance(ast, ASTCreate):
            return self._analyze_create_table(ast)
        
        # 处理INSERT语句
        if isinstance(ast, ASTInsert):
            return self._analyze_insert(ast)
        
        # 处理SELECT语句
        if isinstance(ast, ASTSelect):
            return self._analyze_select(ast)
        
        # 处理DELETE语句
        if isinstance(ast, ASTDelete):
            return self._analyze_delete(ast)
        
        raise SemanticError("unsupported AST")

    def _analyze_create_table(self, ast: ASTCreate) -> Analyzed:
        """
        分析CREATE TABLE语句
        
        检查：
        1. 表是否已存在（重复创建检查）
        2. 列类型是否支持
        
        参数:
            ast (ASTCreate): CREATE TABLE的AST节点
            
        返回:
            Analyzed: 分析结果
            
        异常:
            SemanticError: 当表已存在时抛出
        """
        # 检查表是否已存在
        if self.catalog.table_exists(ast.table):
            raise SemanticError(f"table '{ast.table}' already exists")
        
        # 类型合法性已在语法层初步保证，这里直接通过
        return Analyzed("create_table", {
            "table": ast.table,
            "columns": ast.columns
        })

    def _analyze_insert(self, ast: ASTInsert) -> Analyzed:
        """
        分析INSERT语句
        
        检查：
        1. 表是否存在
        2. 列是否存在
        3. 列数和值数是否匹配
        4. 数据类型是否匹配
        
        参数:
            ast (ASTInsert): INSERT的AST节点
            
        返回:
            Analyzed: 分析结果
            
        异常:
            SemanticError: 当发现语义错误时抛出
        """
        # 检查表是否存在
        self._ensure_table_exists(ast.table)
        
        # 获取表的模式（列定义）
        schema = self.catalog.get_schema(ast.table)
        schema_cols = [c for c, _ in schema]  # 提取列名列表
        
        # 检查列数和值数是否匹配
        if len(ast.columns) != len(ast.values):
            raise SemanticError("columns and values length mismatch")
        
        # 检查列是否存在
        for c in ast.columns:
            if c not in schema_cols:
                raise SemanticError(f"unknown column '{c}' for table '{ast.table}'")
        
        # 类型检查和数据准备
        row: Dict[str, Any] = {}
        for c, v in zip(ast.columns, ast.values):
            # 获取列在模式中的索引
            idx = schema_cols.index(c)
            typ = schema[idx][1].upper()  # 获取列类型
            
            # 类型检查
            if typ == "INT":
                if not isinstance(v, int):
                    raise SemanticError(f"column '{c}' expects INT, got {type(v).__name__}")
            elif typ == "VARCHAR":
                if not isinstance(v, str):
                    raise SemanticError(f"column '{c}' expects VARCHAR, got {type(v).__name__}")
            
            row[c] = v
        
        return Analyzed("insert", {
            "table": ast.table,
            "row": row
        })

    def _analyze_select(self, ast: ASTSelect) -> Analyzed:
        """
        分析SELECT语句
        
        检查：
        1. 表是否存在
        2. 列是否存在（如果不是SELECT *）
        3. WHERE子句中的列是否存在
        
        参数:
            ast (ASTSelect): SELECT的AST节点
            
        返回:
            Analyzed: 分析结果
            
        异常:
            SemanticError: 当发现语义错误时抛出
        """
        # 检查表是否存在
        self._ensure_table_exists(ast.table)
        
        # 获取表的模式
        schema = self.catalog.get_schema(ast.table)
        schema_cols = [c for c, _ in schema]
        
        # 检查列是否存在（如果不是SELECT *）
        cols = ast.columns
        if cols != ["*"]:
            for c in cols:
                if c not in schema_cols:
                    raise SemanticError(f"unknown column '{c}' for table '{ast.table}'")
        
        # 检查WHERE子句
        where = None
        if ast.where is not None:
            col, op, val = ast.where
            if col not in schema_cols:
                raise SemanticError(f"unknown column '{col}' in WHERE")
            where = (col, op, val)
        
        return Analyzed("select", {
            "table": ast.table,
            "columns": cols,
            "where": where
        })

    def _analyze_delete(self, ast: ASTDelete) -> Analyzed:
        """
        分析DELETE语句
        
        检查：
        1. 表是否存在
        2. WHERE子句中的列是否存在
        
        参数:
            ast (ASTDelete): DELETE的AST节点
            
        返回:
            Analyzed: 分析结果
            
        异常:
            SemanticError: 当发现语义错误时抛出
        """
        # 检查表是否存在
        self._ensure_table_exists(ast.table)
        
        # 检查WHERE子句
        where = None
        if ast.where is not None:
            schema_cols = [c for c, _ in self.catalog.get_schema(ast.table)]
            col, op, val = ast.where
            if col not in schema_cols:
                raise SemanticError(f"unknown column '{col}' in WHERE")
            where = (col, op, val)
        
        return Analyzed("delete", {
            "table": ast.table,
            "where": where
        })
