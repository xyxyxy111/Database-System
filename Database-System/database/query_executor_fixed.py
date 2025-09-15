"""查询执行器 - 执行SQL查询计划"""

from typing import List, Dict, Any
from datetime import datetime

from compiler import SQLLexer, SQLParser, SemanticAnalyzer, PlanGenerator
from storage import StorageEngine
from .database_catalog import DatabaseCatalog, TableDefinition, ColumnDefinition


class QueryResult:
    """查询结果类"""
    
    def __init__(self, success: bool, message: str = "", 
                 data: List[Dict[str, Any]] = None, 
                 affected_rows: int = 0):
        self.success = success
        self.message = message
        self.data = data or []
        self.affected_rows = affected_rows
        self.execution_time = 0.0  # 执行时间（秒）
    
    def __str__(self):
        if self.success:
            return f"Success: {self.message}, {self.affected_rows} rows affected"
        else:
            return f"Error: {self.message}"


class QueryExecutor:
    """查询执行器"""
    
    def __init__(self, storage_engine: StorageEngine,
                 catalog: DatabaseCatalog):
        self.storage_engine = storage_engine
        self.catalog = catalog
    
    def execute_sql(self, sql: str) -> QueryResult:
        """执行SQL语句"""
        start_time = datetime.now()
        
        try:
            # 词法分析
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            if not tokens:
                return QueryResult(False, "SQL语句为空或无效")
            
            # 语法分析
            parser = SQLParser(tokens)
            ast = parser.parse()
            if not ast:
                return QueryResult(False, "SQL语法解析失败")
            
            # 语义分析
            semantic_analyzer = SemanticAnalyzer()
            # 同步数据库catalog到语义分析器
            self._sync_catalog_to_semantic_analyzer(semantic_analyzer)
            
            is_valid, errors = semantic_analyzer.analyze(ast)
            if not is_valid:
                error_msg = "; ".join([f"{err.error_type}: {err.message}" 
                                     for err in errors])
                return QueryResult(False, f"语义错误: {error_msg}")
            
            # 生成执行计划
            plan_generator = PlanGenerator()
            execution_plans = plan_generator.generate(ast)
            if not execution_plans:
                return QueryResult(False, "生成执行计划失败")
            
            # 执行查询 - 当前假设只有一个SQL语句
            execution_plan = execution_plans[0]
            result = self._execute_plan(execution_plan)
            
            # 计算执行时间
            end_time = datetime.now()
            result.execution_time = (end_time - start_time).total_seconds()
            
            return result
            
        except Exception as e:
            return QueryResult(False, f"执行错误: {str(e)}")
    
    def _sync_catalog_to_semantic_analyzer(self, semantic_analyzer):
        """将数据库catalog同步到语义分析器"""
        from compiler.catalog import ColumnInfo
        
        # 获取所有表
        for table_name, table_def in self.catalog.tables.items():
            # 创建列信息列表
            columns = []
            for col_def in table_def.columns:
                col_info = ColumnInfo(
                    name=col_def.name,
                    data_type=col_def.data_type,
                    size=getattr(col_def, 'size', None)
                )
                columns.append(col_info)
            
            # 添加到语义分析器的catalog
            semantic_analyzer.catalog.create_table(table_name, columns)
    
    def _execute_plan(self, plan) -> QueryResult:
        """执行查询计划"""
        # 如果是ExecutionPlan对象，转换为字典
        if hasattr(plan, 'to_dict'):
            plan_dict = plan.to_dict()
        else:
            plan_dict = plan
            
        operation = plan_dict.get('operator', '').upper()
        
        # 标准化操作名称
        if operation in ['CREATETABLE', 'CREATE_TABLE']:
            return self._execute_create_table(plan_dict)
        elif operation == 'INSERT':
            return self._execute_insert(plan_dict)
        elif operation == 'SELECT':
            return self._execute_select(plan_dict)
        elif operation == 'DELETE':
            return self._execute_delete(plan_dict)
        elif operation in ['DROPTABLE', 'DROP_TABLE']:
            return self._execute_drop_table(plan_dict)
        else:
            return QueryResult(False, f"不支持的操作: {operation}")
    
    def _execute_create_table(self, plan: Dict[str, Any]) -> QueryResult:
        """执行CREATE TABLE"""
        try:
            # 从properties中获取参数
            properties = plan.get('properties', {})
            table_name = properties.get('table_name')
            columns_info = properties.get('columns')
            
            if not table_name or not columns_info:
                return QueryResult(False, "CREATE TABLE 计划缺少必要参数")
            
            # 创建列定义
            columns = []
            for col_info in columns_info:
                column = ColumnDefinition(
                    name=col_info['name'],
                    data_type=col_info.get('type', col_info.get('data_type')),
                    nullable=col_info.get('nullable', True),
                    default_value=col_info.get('default'),
                    primary_key=col_info.get('primary_key', False),
                    unique=col_info.get('unique', False)
                )
                columns.append(column)
            
            # 创建表定义
            table_def = TableDefinition(
                name=table_name,
                columns=columns
            )
            
            # 添加到catalog
            if not self.catalog.create_table(table_def):
                return QueryResult(False, f"创建表失败: 表 '{table_name}' 已存在")
            
            # 创建存储引擎表结构
            column_names = [col.name for col in columns]
            column_types = [col.data_type for col in columns]
            
            if not self.storage_engine.create_table(table_name, column_names, column_types):
                # 如果存储引擎创建失败，从catalog中移除
                self.catalog.drop_table(table_name)
                return QueryResult(False, f"存储引擎创建表失败")
            
            return QueryResult(True, f"表 '{table_name}' 创建成功")
            
        except Exception as e:
            return QueryResult(False, f"创建表失败: {str(e)}")
    
    def _execute_insert(self, plan: Dict[str, Any]) -> QueryResult:
        """执行INSERT"""
        try:
            # 从properties中获取参数
            properties = plan.get('properties', {})
            table_name = properties.get('table_name')
            columns = properties.get('columns')
            values = properties.get('values')
            
            if not table_name or not values:
                return QueryResult(False, "INSERT 计划缺少必要参数")
            
            # 获取表定义
            table_def = self.catalog.get_table(table_name)
            if not table_def:
                return QueryResult(False, f"表 '{table_name}' 不存在")
            
            successful_inserts = 0
            
            for value_row in values:
                # 创建记录数据
                if columns:
                    # 指定了列名
                    record_data = {col: val for col, val in zip(columns, value_row)}
                else:
                    # 未指定列名，按表定义顺序
                    record_data = {col.name: val for col, val in zip(table_def.columns, value_row)}
                
                # 验证记录
                valid, message = self.catalog.validate_record(table_name, record_data)
                if not valid:
                    return QueryResult(False, f"记录验证失败: {message}")
                
                # 转换为存储格式
                record_values = [record_data.get(col.name) for col in table_def.columns]
                
                # 插入存储引擎
                if self.storage_engine.insert_record(table_name, record_values):
                    successful_inserts += 1
                else:
                    return QueryResult(False, "存储引擎插入失败")
            
            return QueryResult(True, f"成功插入 {successful_inserts} 条记录", 
                             affected_rows=successful_inserts)
            
        except Exception as e:
            return QueryResult(False, f"插入失败: {str(e)}")
    
    def _execute_select(self, plan: Dict[str, Any]) -> QueryResult:
        """执行SELECT"""
        try:
            # 从properties中获取参数
            properties = plan.get('properties', {})
            table_name = properties.get('table_name')
            select_columns = properties.get('columns', [])
            where_condition = properties.get('where_condition')
            
            if not table_name:
                return QueryResult(False, "SELECT 计划缺少表名")
            
            # 获取表定义
            table_def = self.catalog.get_table(table_name)
            if not table_def:
                return QueryResult(False, f"表 '{table_name}' 不存在")
            
            # 从存储引擎获取所有记录
            records = self.storage_engine.scan_table(table_name)
            if records is None:
                return QueryResult(False, f"无法扫描表 '{table_name}'")
            
            # 转换记录为字典格式
            result_records = []
            for record in records:
                record_dict = {}
                for i, col in enumerate(table_def.columns):
                    if i < len(record):
                        record_dict[col.name] = record[i]
                result_records.append(record_dict)
            
            # 应用WHERE条件
            if where_condition:
                result_records = self._apply_where_condition(result_records, where_condition)
            
            # 应用列选择
            if select_columns and select_columns != ['*']:
                projected_records = []
                for record in result_records:
                    projected_record = {}
                    for col in select_columns:
                        if col in record:
                            projected_record[col] = record[col]
                    projected_records.append(projected_record)
                result_records = projected_records
            
            return QueryResult(True, f"查询成功，返回 {len(result_records)} 条记录", 
                             data=result_records)
            
        except Exception as e:
            return QueryResult(False, f"查询失败: {str(e)}")
    
    def _execute_delete(self, plan: Dict[str, Any]) -> QueryResult:
        """执行DELETE"""
        # TODO: 实现DELETE操作
        return QueryResult(False, "DELETE操作暂未实现")
    
    def _execute_drop_table(self, plan: Dict[str, Any]) -> QueryResult:
        """执行DROP TABLE"""
        # TODO: 实现DROP TABLE操作
        return QueryResult(False, "DROP TABLE操作暂未实现")
    
    def _apply_where_condition(self, records: List[Dict[str, Any]],
                              where_condition: Any) -> List[Dict[str, Any]]:
        """应用WHERE条件过滤"""
        # 这是一个简化的WHERE条件处理
        # TODO: 实现完整的WHERE条件评估
        return records  # 暂时返回所有记录
