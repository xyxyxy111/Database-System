"""
执行计划生成器
将AST转换为逻辑执行计划
"""

import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class QueryStats:
    """查询统计信息"""

    sql: str
    execution_time: float
    rows_affected: int
    rows_scanned: int
    timestamp: datetime = field(default_factory=datetime.now)
    plan_type: str = "unknown"
    optimization_applied: bool = False


@dataclass
class PerformanceMetrics:
    """性能指标"""

    total_queries: int = 0
    total_execution_time: float = 0.0
    average_execution_time: float = 0.0
    fastest_query_time: float = float("inf")
    slowest_query_time: float = 0.0
    query_history: List[QueryStats] = field(default_factory=list)


class PerformanceAnalyzer:
    """性能分析器"""

    def __init__(self):
        self.metrics = PerformanceMetrics()
        self.slow_query_threshold = 1.0  # 1秒
        self.enable_detailed_logging = True

    def start_query_timing(self) -> float:
        """开始查询计时"""
        return time.time()

    def end_query_timing(
        self,
        start_time: float,
        sql: str,
        rows_affected: int = 0,
        rows_scanned: int = 0,
        plan_type: str = "unknown",
        optimization_applied: bool = False,
    ) -> QueryStats:
        """结束查询计时并记录统计"""
        execution_time = time.time() - start_time

        stats = QueryStats(
            sql=sql,
            execution_time=execution_time,
            rows_affected=rows_affected,
            rows_scanned=rows_scanned,
            plan_type=plan_type,
            optimization_applied=optimization_applied,
        )

        self._update_metrics(stats)
        return stats

    def _update_metrics(self, stats: QueryStats):
        """更新性能指标"""
        self.metrics.total_queries += 1
        self.metrics.total_execution_time += stats.execution_time
        self.metrics.average_execution_time = (
            self.metrics.total_execution_time / self.metrics.total_queries
        )

        if stats.execution_time < self.metrics.fastest_query_time:
            self.metrics.fastest_query_time = stats.execution_time

        if stats.execution_time > self.metrics.slowest_query_time:
            self.metrics.slowest_query_time = stats.execution_time

        self.metrics.query_history.append(stats)

        # 如果启用详细日志且查询较慢，记录警告
        if (
            self.enable_detailed_logging
            and stats.execution_time > self.slow_query_threshold
        ):
            self._log_slow_query(stats)

    def _log_slow_query(self, stats: QueryStats):
        """记录慢查询"""
        print(f"[SLOW QUERY] 执行时间: {stats.execution_time:.3f}s")
        print(f"SQL: {stats.sql[:100]}{'...' if len(stats.sql) > 100 else ''}")
        print(f"影响行数: {stats.rows_affected}, 扫描行数: {stats.rows_scanned}")

    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        slow_queries = [
            stats
            for stats in self.metrics.query_history
            if stats.execution_time > self.slow_query_threshold
        ]

        optimized_queries = [
            stats for stats in self.metrics.query_history if stats.optimization_applied
        ]

        return {
            "总体统计": {
                "查询总数": self.metrics.total_queries,
                "总执行时间": f"{self.metrics.total_execution_time:.3f}s",
                "平均执行时间": f"{self.metrics.average_execution_time:.3f}s",
                "最快查询": f"{self.metrics.fastest_query_time:.3f}s",
                "最慢查询": f"{self.metrics.slowest_query_time:.3f}s",
            },
            "慢查询分析": {
                "慢查询数量": len(slow_queries),
                "慢查询阈值": f"{self.slow_query_threshold}s",
                "慢查询占比": f"{len(slow_queries)/max(1, self.metrics.total_queries)*100:.1f}%",
            },
            "优化效果": {
                "应用优化的查询数": len(optimized_queries),
                "优化率": f"{len(optimized_queries)/max(1, self.metrics.total_queries)*100:.1f}%",
            },
            "查询类型分布": self._analyze_query_types(),
            "性能趋势": self._analyze_performance_trend(),
        }

    def _analyze_query_types(self) -> Dict[str, int]:
        """分析查询类型分布"""
        type_counts = {}
        for stats in self.metrics.query_history:
            sql_type = self._get_sql_type(stats.sql)
            type_counts[sql_type] = type_counts.get(sql_type, 0) + 1
        return type_counts

    def _get_sql_type(self, sql: str) -> str:
        """获取SQL类型"""
        sql_upper = sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            return "SELECT"
        elif sql_upper.startswith("INSERT"):
            return "INSERT"
        elif sql_upper.startswith("UPDATE"):
            return "UPDATE"
        elif sql_upper.startswith("DELETE"):
            return "DELETE"
        elif sql_upper.startswith("CREATE"):
            return "CREATE"
        else:
            return "OTHER"

    def _analyze_performance_trend(self) -> Dict[str, Any]:
        """分析性能趋势"""
        if len(self.metrics.query_history) < 2:
            return {"趋势": "数据不足"}

        recent_queries = self.metrics.query_history[-10:]  # 最近10个查询
        recent_avg = sum(q.execution_time for q in recent_queries) / len(recent_queries)

        all_avg = self.metrics.average_execution_time

        if recent_avg > all_avg * 1.2:
            trend = "性能下降"
        elif recent_avg < all_avg * 0.8:
            trend = "性能提升"
        else:
            trend = "性能稳定"

        return {
            "趋势": trend,
            "最近平均时间": f"{recent_avg:.3f}s",
            "总体平均时间": f"{all_avg:.3f}s",
        }

    def visualize_execution_plan(self, plan_dict: Dict[str, Any]) -> str:
        """可视化执行计划"""
        return self._format_plan_tree(plan_dict, 0)

    def _format_plan_tree(self, plan_dict: Dict[str, Any], depth: int) -> str:
        """格式化计划树"""
        indent = "  " * depth
        operator = plan_dict.get("operator", "Unknown")
        properties = plan_dict.get("properties", {})

        result = f"{indent}┌─ {operator}"
        if properties:
            prop_str = ", ".join(f"{k}={v}" for k, v in properties.items())
            result += f" ({prop_str})"
        result += "\n"

        children = plan_dict.get("children", [])
        for i, child in enumerate(children):
            if i == len(children) - 1:
                result += f"{indent}└─ "
            else:
                result += f"{indent}├─ "
            result += self._format_plan_tree(child, depth + 1)

        return result

    def suggest_optimizations(self, stats: QueryStats) -> List[str]:
        """建议优化方案"""
        suggestions = []

        # 基于执行时间的建议
        if stats.execution_time > self.slow_query_threshold:
            suggestions.append("查询执行时间较长，考虑添加索引或优化WHERE条件")

        # 基于扫描行数的建议
        if stats.rows_scanned > stats.rows_affected * 10:
            suggestions.append("扫描行数远大于结果行数，考虑优化查询条件")

        # 基于SQL类型的建议
        sql_type = self._get_sql_type(stats.sql)
        if sql_type == "SELECT" and "SELECT *" in stats.sql.upper():
            suggestions.append("避免使用SELECT *，明确指定需要的列")

        if not stats.optimization_applied:
            suggestions.append("启用查询优化器可能会提升性能")

        return suggestions


class QueryProfiler:
    """查询性能分析器装饰器"""

    def __init__(self, analyzer: PerformanceAnalyzer):
        self.analyzer = analyzer

    def profile_query(self, sql: str):
        """查询性能分析装饰器"""

        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = self.analyzer.start_query_timing()

                try:
                    result = func(*args, **kwargs)

                    # 尝试从结果中提取统计信息
                    rows_affected = getattr(result, "affected_rows", 0)
                    rows_scanned = getattr(result, "rows_scanned", 0)

                    stats = self.analyzer.end_query_timing(
                        start_time, sql, rows_affected, rows_scanned
                    )

                    # 添加性能统计到结果中
                    if hasattr(result, "__dict__"):
                        result.performance_stats = stats

                    return result

                except Exception as e:
                    # 即使出错也记录时间
                    self.analyzer.end_query_timing(start_time, sql)
                    raise e

            return wrapper

        return decorator


def main():
    """测试性能分析工具"""
    analyzer = PerformanceAnalyzer()
    profiler = QueryProfiler(analyzer)

    # 模拟一些查询
    test_queries = [
        "SELECT * FROM users WHERE age > 18",
        "INSERT INTO users VALUES (1, 'Alice', 25)",
        "UPDATE users SET age = 26 WHERE id = 1",
        "SELECT name FROM users ORDER BY age DESC",
    ]

    for sql in test_queries:
        start_time = analyzer.start_query_timing()

        # 模拟查询执行
        time.sleep(0.1)  # 模拟执行时间

        stats = analyzer.end_query_timing(
            start_time, sql, rows_affected=10, rows_scanned=100, plan_type="SeqScan"
        )

        print(f"查询: {sql[:50]}...")
        print(f"执行时间: {stats.execution_time:.3f}s")

        suggestions = analyzer.suggest_optimizations(stats)
        if suggestions:
            print("优化建议:")
            for suggestion in suggestions:
                print(f"  - {suggestion}")
        print()

    # 输出性能报告
    report = analyzer.get_performance_report()
    print("=== 性能分析报告 ===")
    for section, data in report.items():
        print(f"\n{section}:")
        if isinstance(data, dict):
            for key, value in data.items():
                print(f"  {key}: {value}")
        else:
            print(f"  {data}")


if __name__ == "__main__":
    main()
