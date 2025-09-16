"""查询执行器 - 执行SQL查询计划"""

from datetime import datetime
from typing import Any, Dict, List

from compiler import PlanGenerator, SemanticAnalyzer, SQLLexer, SQLParser
from compiler.ast_nodes import (
    AggregateFunction,
    BeginStatement,
    CommitStatement,
    RollbackStatement,
)
from storage import StorageEngine

from .aggregate_processor import AggregateProcessor
from .db_catalog import ColumnDefinition, DatabaseCatalog, TableDefinition
from .transaction_manager import TransactionManager


class QueryResult:
    """查询结果类"""

    def __init__(
        self,
        success: bool,
        message: str = "",
        data: List[Dict[str, Any]] = None,
        affected_rows: int = 0,
    ):
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

    def __init__(self, storage_engine: StorageEngine, catalog: DatabaseCatalog):
        self.storage_engine = storage_engine
        self.catalog = catalog
        self.aggregate_processor = AggregateProcessor()
        self.transaction_manager = TransactionManager(storage_engine, catalog)

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
                error_msg = "; ".join(
                    [f"{err.error_type}: {err.message}" for err in errors]
                )
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
                    size=getattr(col_def, "size", None),
                )
                columns.append(col_info)

            # 添加到语义分析器的catalog
            semantic_analyzer.catalog.create_table(table_name, columns)

    def _execute_plan(self, plan) -> QueryResult:
        """执行查询计划"""
        # 如果是ExecutionPlan对象，转换为字典
        if hasattr(plan, "to_dict"):
            plan_dict = plan.to_dict()
        else:
            plan_dict = plan

        operation = plan_dict.get("operator", "").upper()

        # 标准化操作名称
        if operation in ["CREATETABLE", "CREATE_TABLE"]:
            return self._execute_create_table(plan_dict)
        elif operation == "INSERT":
            return self._execute_insert(plan_dict)
        elif operation in ["SELECT", "PROJECT", "SEQSCAN"]:
            return self._execute_select(plan_dict)
        elif operation == "DELETE":
            return self._execute_delete(plan_dict)
        elif operation == "UPDATE":
            return self._execute_update(plan_dict)
        elif operation in ["DROPTABLE", "DROP_TABLE"]:
            return self._execute_drop_table(plan_dict)
        elif operation == "BEGIN":
            return self._execute_begin(plan_dict)
        elif operation == "COMMIT":
            return self._execute_commit(plan_dict)
        elif operation == "ROLLBACK":
            return self._execute_rollback(plan_dict)
        else:
            return QueryResult(False, f"不支持的操作: {operation}")

    def _execute_create_table(self, plan: Dict[str, Any]) -> QueryResult:
        """执行CREATE TABLE"""
        try:
            # 从properties中获取参数
            properties = plan.get("properties", {})
            table_name = properties.get("table_name")
            columns_info = properties.get("columns")

            if not table_name or not columns_info:
                return QueryResult(False, "CREATE TABLE 计划缺少必要参数")

            # 创建列定义
            columns = []
            for col_info in columns_info:
                column = ColumnDefinition(
                    name=col_info["name"],
                    data_type=col_info.get("type", col_info.get("data_type")),
                    nullable=col_info.get("nullable", True),
                    default_value=col_info.get("default"),
                    primary_key=col_info.get("primary_key", False),
                    unique=col_info.get("unique", False),
                )
                columns.append(column)

            # 创建表定义
            from datetime import datetime

            table_def = TableDefinition(
                name=table_name, columns=columns, created_at=datetime.now().isoformat()
            )

            # 添加到catalog
            if not self.catalog.create_table(table_def):
                return QueryResult(False, f"创建表失败: 表 '{table_name}' 已存在")

            # 创建存储引擎表结构
            column_names = [col.name for col in columns]
            column_types = [col.data_type for col in columns]

            if not self.storage_engine.create_table(
                table_name, column_names, column_types
            ):
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
            properties = plan.get("properties", {})
            table_name = properties.get("table_name")
            columns = properties.get("columns")
            values = properties.get("values")

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
                    record_data = {
                        col.name: val for col, val in zip(table_def.columns, value_row)
                    }

                # 验证记录
                valid, message = self.catalog.validate_record(table_name, record_data)
                if not valid:
                    return QueryResult(False, f"记录验证失败: {message}")

                # 转换为存储格式
                record_values = [record_data.get(col.name) for col in table_def.columns]

                # 插入存储引擎
                if self.storage_engine.insert_record(table_name, record_values):
                    successful_inserts += 1

                    # 记录事务日志
                    if self.transaction_manager.is_in_transaction():
                        self.transaction_manager.log_insert(table_name, record_values)
                else:
                    return QueryResult(False, "存储引擎插入失败")

            return QueryResult(
                True,
                f"成功插入 {successful_inserts} 条记录",
                affected_rows=successful_inserts,
            )

        except Exception as e:
            return QueryResult(False, f"插入失败: {str(e)}")

    def _execute_select(self, plan: Dict[str, Any]) -> QueryResult:
        """执行SELECT"""
        try:
            # SELECT操作可能是复杂的计划树，需要递归处理
            operation = plan.get("operator", "").upper()
            properties = plan.get("properties", {})

            if operation == "PROJECT":
                # 处理投影操作
                select_columns = properties.get("columns", [])
                select_list = properties.get("select_list", [])
                children = plan.get("children", [])

                if not children:
                    return QueryResult(False, "投影操作缺少子操作")

                # 递归执行子操作
                child_result = self._execute_select(children[0])
                if not child_result.success:
                    return child_result

                # 检查是否包含聚合函数
                if select_list and self.aggregate_processor.is_aggregate_query(
                    select_list
                ):
                    # 处理聚合查询
                    aggregates = self.aggregate_processor.extract_aggregates(
                        select_list
                    )
                    aggregate_results = self.aggregate_processor.process_aggregates(
                        aggregates, child_result.data
                    )

                    # 返回聚合结果
                    child_result.data = [aggregate_results]
                else:
                    # 应用列投影（非聚合查询）
                    if select_columns and select_columns != ["*"]:
                        projected_records = []
                        for record in child_result.data:
                            projected_record = {}
                            for col in select_columns:
                                if col in record:
                                    projected_record[col] = record[col]
                            projected_records.append(projected_record)
                        child_result.data = projected_records

                return child_result

            elif operation == "FILTER":
                # 处理过滤操作
                condition = properties.get("condition")
                children = plan.get("children", [])

                if not children:
                    return QueryResult(False, "过滤操作缺少子操作")

                # 递归执行子操作
                child_result = self._execute_select(children[0])
                if not child_result.success:
                    return child_result

                # 应用过滤条件
                if condition:
                    child_result.data = self._apply_where_condition(
                        child_result.data, condition
                    )

                return child_result

            elif operation == "SORT":
                # 处理排序操作
                sort_expressions = properties.get("sort_expressions", [])
                children = plan.get("children", [])

                if not children:
                    return QueryResult(False, "排序操作缺少子操作")

                # 递归执行子操作
                child_result = self._execute_select(children[0])
                if not child_result.success:
                    return child_result

                # 应用排序
                if sort_expressions:
                    child_result.data = self._apply_sort(
                        child_result.data, sort_expressions
                    )

                return child_result

            elif operation == "JOIN":
                # 处理连接操作
                join_type = properties.get("join_type", "INNER")
                join_condition = properties.get("condition")
                children = plan.get("children", [])

                if len(children) != 2:
                    return QueryResult(False, "连接操作需要两个子操作")

                # 递归执行左表和右表
                left_result = self._execute_select(children[0])
                if not left_result.success:
                    return left_result

                right_result = self._execute_select(children[1])
                if not right_result.success:
                    return right_result

                # 执行连接
                joined_data = self._apply_join(
                    left_result.data, right_result.data, join_type, join_condition
                )

                return QueryResult(
                    True,
                    f"连接完成，获取 {len(joined_data)} 条记录",
                    data=joined_data,
                )

            elif operation == "SEQSCAN":
                # 处理顺序扫描操作
                table_name = properties.get("table_name")

                if not table_name:
                    return QueryResult(False, "顺序扫描操作缺少表名")

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
                    # Record对象有values属性
                    record_values = (
                        record.values if hasattr(record, "values") else record
                    )
                    for i, col in enumerate(table_def.columns):
                        if i < len(record_values):
                            record_dict[col.name] = record_values[i]
                    result_records.append(record_dict)

                return QueryResult(
                    True,
                    f"扫描完成，获取 {len(result_records)} 条记录",
                    data=result_records,
                )

            else:
                # 兼容性处理：处理旧格式的SELECT计划
                return self._execute_simple_select(plan)

        except Exception as e:
            return QueryResult(False, f"查询失败: {str(e)}")

    def _execute_simple_select(self, plan: Dict[str, Any]) -> QueryResult:
        """执行简单格式的SELECT（向后兼容）"""
        try:
            # 从properties中获取参数
            properties = plan.get("properties", {})
            table_name = properties.get("table_name")
            select_columns = properties.get("columns", [])
            where_condition = properties.get("where_condition")
            select_list = properties.get("select_list", [])

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
                # Record对象有values属性
                record_values = record.values if hasattr(record, "values") else record
                for i, col in enumerate(table_def.columns):
                    if i < len(record_values):
                        record_dict[col.name] = record_values[i]
                result_records.append(record_dict)

            # 应用WHERE条件
            if where_condition:
                result_records = self._apply_where_condition(
                    result_records, where_condition
                )

            # 检查是否为聚合查询
            if select_list and self.aggregate_processor.is_aggregate_query(select_list):
                # 处理聚合查询
                aggregates = self.aggregate_processor.extract_aggregates(select_list)
                aggregate_results = self.aggregate_processor.process_aggregates(
                    aggregates, result_records
                )

                # 构建结果
                result_records = [aggregate_results]
            else:
                # 应用列选择（非聚合查询）
                if select_columns and select_columns != ["*"]:
                    projected_records = []
                    for record in result_records:
                        projected_record = {}
                        for col in select_columns:
                            if col in record:
                                projected_record[col] = record[col]
                        projected_records.append(projected_record)
                    result_records = projected_records

            return QueryResult(
                True,
                f"查询成功，返回 {len(result_records)} 条记录",
                data=result_records,
            )

        except Exception as e:
            return QueryResult(False, f"查询失败: {str(e)}")

    def _execute_delete(self, plan: Dict[str, Any]) -> QueryResult:
        """执行DELETE"""
        try:
            properties = plan.get("properties", {})
            table_name = properties.get("table_name")
            condition = properties.get("condition")

            if not table_name:
                return QueryResult(False, "DELETE 计划缺少表名")

            # 检查表是否存在
            if not self.storage_engine.table_exists(table_name):
                return QueryResult(False, f"表 '{table_name}' 不存在")

            # 获取所有记录
            all_records = self.storage_engine.scan_table(table_name)

            # 将记录转换为字典格式
            table_info = self.catalog.get_table(table_name)
            if not table_info:
                return QueryResult(False, f"表 '{table_name}' 的元数据不存在")

            formatted_records = []
            for record in all_records:
                record_dict = {}
                record_values = record.values if hasattr(record, "values") else record
                for i, col in enumerate(table_info.columns):
                    if i < len(record_values):
                        record_dict[col.name] = record_values[i]
                formatted_records.append(record_dict)
            all_records = formatted_records

            # 应用WHERE条件过滤
            if condition:
                records_to_delete = self._apply_where_condition(all_records, condition)
            else:
                records_to_delete = all_records

            # 删除记录 (简化实现：重新插入不匹配的记录)
            if condition:
                records_to_keep = [r for r in all_records if r not in records_to_delete]
                # 清空表并重新插入保留的记录
                self.storage_engine.clear_table(table_name)
                for record in records_to_keep:
                    self.storage_engine.insert_record(table_name, list(record.values()))
            else:
                # 删除所有记录
                self.storage_engine.clear_table(table_name)

            deleted_count = len(records_to_delete)
            return QueryResult(
                True, f"成功删除 {deleted_count} 条记录", affected_rows=deleted_count
            )

        except Exception as e:
            return QueryResult(False, f"删除失败: {str(e)}")

    def _execute_update(self, plan: Dict[str, Any]) -> QueryResult:
        """执行UPDATE"""
        try:
            properties = plan.get("properties", {})
            table_name = properties.get("table_name")
            assignments = properties.get("assignments", [])
            condition = properties.get("condition")

            if not table_name:
                return QueryResult(False, "UPDATE 计划缺少表名")

            if not assignments:
                return QueryResult(False, "UPDATE 计划缺少赋值语句")

            # 检查表是否存在
            if not self.storage_engine.table_exists(table_name):
                return QueryResult(False, f"表 '{table_name}' 不存在")

            # 获取表信息
            table_info = self.catalog.get_table(table_name)
            if not table_info:
                return QueryResult(False, f"无法获取表 '{table_name}' 的元数据")

            # 获取所有记录
            all_records = self.storage_engine.scan_table(table_name)

            # 将记录转换为字典格式
            formatted_records = []
            for record in all_records:
                record_dict = {}
                record_values = record.values if hasattr(record, "values") else record
                for i, col in enumerate(table_info.columns):
                    if i < len(record_values):
                        record_dict[col.name] = record_values[i]
                formatted_records.append(record_dict)
            all_records = formatted_records

            # 应用WHERE条件过滤需要更新的记录
            if condition:
                records_to_update = self._apply_where_condition(all_records, condition)
            else:
                records_to_update = all_records

            # 更新记录
            updated_records = []
            for record in all_records:
                if record in records_to_update:
                    # 创建更新后的记录
                    updated_record = record.copy()
                    for col_name, value_expr in assignments:
                        # 评估表达式值
                        new_value = self._evaluate_expression(value_expr)
                        updated_record[col_name] = new_value
                    updated_records.append(updated_record)
                else:
                    updated_records.append(record)

            # 清空表并重新插入更新后的记录
            self.storage_engine.clear_table(table_name)
            for record in updated_records:
                column_order = [col.name for col in table_info.columns]
                values = [record.get(col, None) for col in column_order]
                self.storage_engine.insert_record(table_name, values)

            updated_count = len(records_to_update)
            return QueryResult(
                True, f"成功更新 {updated_count} 条记录", affected_rows=updated_count
            )

        except Exception as e:
            return QueryResult(False, f"更新失败: {str(e)}")

    def _execute_drop_table(self, plan: Dict[str, Any]) -> QueryResult:
        """执行DROP TABLE"""
        try:
            # 从properties中获取参数
            properties = plan.get("properties", {})
            table_name = properties.get("table_name")

            if not table_name:
                return QueryResult(False, "DROP TABLE 计划缺少表名")

            # 检查表是否存在
            table_def = self.catalog.get_table(table_name)
            if not table_def:
                return QueryResult(False, f"表 '{table_name}' 不存在")

            # 检查是否有外键约束引用此表（如果有外键实现的话）
            # 这里简化处理，实际应该检查所有表是否有引用此表的外键

            # 从catalog中删除表定义
            if not self.catalog.drop_table(table_name):
                return QueryResult(False, f"从catalog中删除表 '{table_name}' 失败")

            # 从存储引擎中删除表数据
            if not self.storage_engine.drop_table(table_name):
                # 如果存储引擎删除失败，尝试恢复catalog中的表定义
                self.catalog.create_table(table_def)
                return QueryResult(False, f"存储引擎删除表 '{table_name}' 失败")

            return QueryResult(True, f"表 '{table_name}' 删除成功")

        except Exception as e:
            return QueryResult(False, f"删除表失败: {str(e)}")


    def _evaluate_expression(self, expr_dict: Dict[str, Any]) -> Any:
        """评估表达式值"""
        if expr_dict.get("type") == "literal":
            return expr_dict.get("value")
        elif expr_dict.get("type") == "identifier":
            # 对于标识符，这里简化处理，实际应该从当前记录中获取值
            return expr_dict.get("name")
        else:
            # 其他复杂表达式的处理
            return str(expr_dict)

    def _execute_begin(self, plan: Dict[str, Any]) -> QueryResult:
        """执行BEGIN TRANSACTION"""
        try:
            transaction_id = self.transaction_manager.begin_transaction()
            return QueryResult(
                True, f"事务已开始，事务ID: {transaction_id}", affected_rows=0
            )
        except Exception as e:
            return QueryResult(False, f"开始事务失败: {str(e)}")

    def _execute_commit(self, plan: Dict[str, Any]) -> QueryResult:
        """执行COMMIT"""
        try:
            if not self.transaction_manager.is_in_transaction():
                return QueryResult(False, "没有活跃的事务可提交")

            success = self.transaction_manager.commit_transaction()
            if success:
                return QueryResult(True, "事务提交成功", affected_rows=0)
            else:
                return QueryResult(False, "事务提交失败")
        except Exception as e:
            return QueryResult(False, f"提交事务失败: {str(e)}")

    def _execute_rollback(self, plan: Dict[str, Any]) -> QueryResult:
        """执行ROLLBACK"""
        try:
            if not self.transaction_manager.is_in_transaction():
                return QueryResult(False, "没有活跃的事务可回滚")

            success = self.transaction_manager.rollback_transaction()
            if success:
                return QueryResult(True, "事务回滚成功", affected_rows=0)
            else:
                return QueryResult(False, "事务回滚失败")
        except Exception as e:
            return QueryResult(False, f"回滚事务失败: {str(e)}")

    def _apply_where_condition(
        self, records: List[Dict[str, Any]], where_condition: Any
    ) -> List[Dict[str, Any]]:
        """应用WHERE条件过滤"""
        if not where_condition or not records:
            return records

        filtered_records = []
        for record in records:
            if self._evaluate_condition(record, where_condition):
                filtered_records.append(record)

        return filtered_records

    def _evaluate_condition(
        self, record: Dict[str, Any], condition: Dict[str, Any]
    ) -> bool:
        """评估单条记录是否满足条件"""
        condition_type = condition.get("type")
        if condition_type in ["binary_expression", "binary"]:
            left_val = self._get_condition_value(record, condition.get("left"))
            right_val = self._get_condition_value(record, condition.get("right"))
            operator = condition.get("operator")

            # 执行比较操作
            if operator == "=":
                return left_val == right_val
            elif operator in ["!=", "<>"]:
                return left_val != right_val
            elif operator == "<":
                return left_val < right_val
            elif operator == ">":
                return left_val > right_val
            elif operator == "<=":
                return left_val <= right_val
            elif operator == ">=":
                return left_val >= right_val
            else:
                return False

        return True  # 默认通过

    def _get_condition_value(self, record: Dict[str, Any], expr: Dict[str, Any]) -> Any:
        """从表达式中获取值"""
        if expr.get("type") == "literal":
            return expr.get("value")
        elif expr.get("type") == "identifier":
            column_name = expr.get("name")
            return record.get(column_name)
        else:
            return None

    def _apply_sort(
        self, records: List[Dict[str, Any]], sort_expressions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """应用排序到记录列表"""
        if not records or not sort_expressions:
            return records

        def sort_key(record: Dict[str, Any]):
            """生成排序键"""
            key_values = []
            for sort_expr in sort_expressions:
                column_name = sort_expr.get("column")
                direction = sort_expr.get("direction", "ASC").upper()

                if column_name and column_name in record:
                    value = record[column_name]
                    # 对于DESC排序，我们需要反转排序
                    if direction == "DESC":
                        # 对于数字，使用负值
                        if isinstance(value, (int, float)):
                            key_values.append(-value)
                        # 对于字符串，需要特殊处理
                        elif isinstance(value, str):
                            # 使用元组技巧，先按反转标志，再按字符串
                            key_values.append((1, value))
                        else:
                            key_values.append((1, str(value)))
                    else:  # ASC
                        if isinstance(value, str):
                            key_values.append((0, value))
                        else:
                            key_values.append(value)
                else:
                    # 缺失列按None处理
                    key_values.append(None)

            return key_values

        try:
            # 执行排序
            sorted_records = sorted(records, key=sort_key)
            return sorted_records
        except Exception as e:
            # 如果排序失败，返回原记录
            print(f"排序失败: {e}")
            return records

    def _apply_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        join_type: str,
        join_condition: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """应用连接到两个数据集"""
        if not left_data or not right_data:
            if join_type == "LEFT":
                return left_data
            elif join_type == "RIGHT":
                return right_data
            else:
                return []

        joined_records = []

        for left_record in left_data:
            matched = False

            for right_record in right_data:
                # 合并记录用于条件评估
                combined_record = {**left_record, **right_record}

                # 检查连接条件
                if self._evaluate_condition(combined_record, join_condition):
                    # 创建连接后的记录
                    joined_record = {**left_record, **right_record}
                    joined_records.append(joined_record)
                    matched = True

            # 处理LEFT JOIN的情况
            if join_type == "LEFT" and not matched:
                # 对于左连接，没有匹配的右表记录时，用None填充
                null_right_record = (
                    {key: None for key in right_data[0].keys()} if right_data else {}
                )
                joined_record = {**left_record, **null_right_record}
                joined_records.append(joined_record)

        # 处理RIGHT JOIN的情况
        if join_type == "RIGHT":
            for right_record in right_data:
                matched = False
                for left_record in left_data:
                    combined_record = {**left_record, **right_record}
                    if self._evaluate_condition(combined_record, join_condition):
                        matched = True
                        break

                if not matched:
                    # 对于右连接，没有匹配的左表记录时，用None填充
                    null_left_record = (
                        {key: None for key in left_data[0].keys()} if left_data else {}
                    )
                    joined_record = {**null_left_record, **right_record}
                    joined_records.append(joined_record)

        return joined_records
