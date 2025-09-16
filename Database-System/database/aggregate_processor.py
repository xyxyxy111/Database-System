from typing import Any, Dict, List, Optional
from compiler.ast_nodes import AggregateFunction, Identifier, Literal


class AggregateProcessor:
    """聚合函数处理器"""

    def __init__(self):
        self.supported_functions = {"COUNT", "SUM", "AVG", "MAX", "MIN"}

    def is_aggregate_query(self, select_list: List[Any]) -> bool:
        """判断是否包含聚合函数"""
        for item in select_list:
            if isinstance(item, AggregateFunction):
                return True
        return False

    def extract_aggregates(self, select_list: List[Any]) -> List[AggregateFunction]:
        """提取聚合函数"""
        aggregates = []
        for item in select_list:
            if isinstance(item, AggregateFunction):
                aggregates.append(item)
        return aggregates

    def process_aggregates(
        self, aggregates: List[AggregateFunction], records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """处理聚合函数计算"""
        result = {}

        for aggregate in aggregates:
            func_name = aggregate.function_name
            column_name = self._get_column_name(aggregate.argument)

            if func_name == "COUNT":
                result[f"COUNT({column_name})"] = self._compute_count(
                    records, column_name, aggregate.distinct
                )
            elif func_name == "SUM":
                result[f"SUM({column_name})"] = self._compute_sum(records, column_name)
            elif func_name == "AVG":
                result[f"AVG({column_name})"] = self._compute_avg(records, column_name)
            elif func_name == "MAX":
                result[f"MAX({column_name})"] = self._compute_max(records, column_name)
            elif func_name == "MIN":
                result[f"MIN({column_name})"] = self._compute_min(records, column_name)

        return result

    def _get_column_name(self, argument: Any) -> str:
        """获取列名"""
        if isinstance(argument, Identifier):
            return argument.name
        elif isinstance(argument, Literal):
            return argument.value
        else:
            return str(argument)

    def _compute_count(
        self, records: List[Dict[str, Any]], column_name: str, distinct: bool = False
    ) -> int:
        """计算COUNT"""
        if column_name == "*":
            return len(records)

        values = []
        for record in records:
            if column_name in record and record[column_name] is not None:
                values.append(record[column_name])

        if distinct:
            return len(set(values))
        else:
            return len(values)

    def _compute_sum(
        self, records: List[Dict[str, Any]], column_name: str
    ) -> Optional[float]:
        """计算SUM"""
        total = 0
        count = 0

        for record in records:
            if column_name in record and record[column_name] is not None:
                try:
                    value = float(record[column_name])
                    total += value
                    count += 1
                except (ValueError, TypeError):
                    continue

        return total if count > 0 else None

    def _compute_avg(
        self, records: List[Dict[str, Any]], column_name: str
    ) -> Optional[float]:
        """计算AVG"""
        total = 0
        count = 0

        for record in records:
            if column_name in record and record[column_name] is not None:
                try:
                    value = float(record[column_name])
                    total += value
                    count += 1
                except (ValueError, TypeError):
                    continue

        return total / count if count > 0 else None

    def _compute_max(
        self, records: List[Dict[str, Any]], column_name: str
    ) -> Optional[Any]:
        """计算MAX"""
        max_value = None

        for record in records:
            if column_name in record and record[column_name] is not None:
                value = record[column_name]
                if max_value is None or value > max_value:
                    max_value = value

        return max_value

    def _compute_min(
        self, records: List[Dict[str, Any]], column_name: str
    ) -> Optional[Any]:
        """计算MIN"""
        min_value = None

        for record in records:
            if column_name in record and record[column_name] is not None:
                value = record[column_name]
                if min_value is None or value < min_value:
                    min_value = value

        return min_value


class AggregateQueryBuilder:
    """聚合查询构建器"""

    @staticmethod
    def build_aggregate_result(aggregates: Dict[str, Any]) -> List[Dict[str, Any]]:
        """构建聚合查询结果"""
        if not aggregates:
            return []

        # 聚合查询返回单行结果
        return [aggregates]

    @staticmethod
    def format_aggregate_names(aggregates: List[AggregateFunction]) -> List[str]:
        """格式化聚合函数名称"""
        names = []
        for aggregate in aggregates:
            func_name = aggregate.function_name
            if isinstance(aggregate.argument, Identifier):
                column_name = aggregate.argument.name
            elif isinstance(aggregate.argument, Literal):
                column_name = aggregate.argument.value
            else:
                column_name = str(aggregate.argument)

            if aggregate.distinct:
                names.append(f"{func_name}(DISTINCT {column_name})")
            else:
                names.append(f"{func_name}({column_name})")

        return names
