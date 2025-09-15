"""性能分析组件
用于统计 SQL 执行效率、生成性能趋势报告以及可视化执行计划
"""

import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class QueryStats:
    """单条查询的性能信息"""

    sql: str
    execution_time: float
    rows_affected: int
    rows_scanned: int
    timestamp: datetime = field(default_factory=datetime.now)
    plan_type: str = "unknown"
    optimization_applied: bool = False


@dataclass
class PerformanceMetrics:
    """全局性能指标"""

    total_queries: int = 0
    total_execution_time: float = 0.0
    average_execution_time: float = 0.0
    fastest_query_time: float = float("inf")
    slowest_query_time: float = 0.0
    query_history: List[QueryStats] = field(default_factory=list)


class PerformanceAnalyzer:
    """性能分析器"""

    def __init__(self):
        self.metrics: PerformanceMetrics = PerformanceMetrics()
        self.slow_query_threshold: float = 1.0  # 默认 1 秒
        self.enable_detailed_logging: bool = True

    def start_query_timing(self) -> float:
        """记录开始时间戳"""
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
        """结束计时并生成查询统计"""
        duration: float = time.time() - start_time
        stats = QueryStats(
            sql=sql,
            execution_time=duration,
            rows_affected=rows_affected,
            rows_scanned=rows_scanned,
            plan_type=plan_type,
            optimization_applied=optimization_applied,
        )
        self._update_metrics(stats)
        return stats

    def _update_metrics(self, stats: QueryStats):
        """更新累计指标"""
        m = self.metrics
        m.total_queries += 1
        m.total_execution_time += stats.execution_time
        m.average_execution_time = m.total_execution_time / m.total_queries

        if stats.execution_time < m.fastest_query_time:
            m.fastest_query_time = stats.execution_time

        if stats.execution_time > m.slowest_query_time:
            m.slowest_query_time = stats.execution_time

        m.query_history.append(stats)

        if self.enable_detailed_logging and stats.execution_time > self.slow_query_threshold:
            self._log_slow_query(stats)

    def _log_slow_query(self, stats: QueryStats):
        """打印慢查询日志"""
        print(f"[SLOW QUERY] 执行时间: {stats.execution_time:.3f}s")
        print(f"SQL: {stats.sql[:100]}{'...' if len(stats.sql) > 100 else ''}")
        print(f"影响行数: {stats.rows_affected}, 扫描行数: {stats.rows_scanned}")

    def get_performance_report(self) -> Dict[str, Any]:
        """生成整体性能报告"""
        slow_queries: List[QueryStats] = [
            q for q in self.metrics.query_history if q.execution_time > self.slow_query_threshold
        ]
        optimized: List[QueryStats] = [
            q for q in self.metrics.query_history if q.optimization_applied
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
                "阈值": f"{self.slow_query_threshold}s",
                "占比": f"{len(slow_queries)/max(1, self.metrics.total_queries)*100:.1f}%",
            },
            "优化情况": {
                "已优化查询数": len(optimized),
                "优化率": f"{len(optimized)/max(1, self.metrics.total_queries)*100:.1f}%",
            },
            "类型分布": self._analyze_query_types(),
            "趋势分析": self._analyze_performance_trend(),
        }

    def _analyze_query_types(self) -> Dict[str, int]:
        """统计不同 SQL 类型的分布"""
        type_counts: Dict[str, int] = {}
        for stats in self.metrics.query_history:
            sql_type = self._get_sql_type(stats.sql)
            type_counts[sql_type] = type_counts.get(sql_type, 0) + 1
        return type_counts

    def _get_sql_type(self, sql: str) -> str:
        """识别 SQL 类型"""
        text = sql.upper().strip()
        if text.startswith("SELECT"):
            return "SELECT"
        if text.startswith("INSERT"):
            return "INSERT"
        if text.startswith("UPDATE"):
            return "UPDATE"
        if text.startswith("DELETE"):
            return "DELETE"
        if text.startswith("CREATE"):
            return "CREATE"
        return "OTHER"

    def _analyze_performance_trend(self) -> Dict[str, Any]:
        """分析性能走势"""
        if len(self.metrics.query_history) < 2:
            return {"趋势": "数据不足"}

        recent = self.metrics.query_history[-10:]
        recent_avg: float = sum(q.execution_time for q in recent) / len(recent)
        all_avg: float = self.metrics.average_execution_time

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
        """树状图形式显示执行计划"""
        return self._format_plan_tree(plan_dict, 0)

    def _format_plan_tree(self, plan_dict: Dict[str, Any], depth: int) -> str:
        """递归格式化执行计划树"""
        indent = "  " * depth
        operator = plan_dict.get("operator", "Unknown")
        props = plan_dict.get("properties", {})

        line = f"{indent}┌─ {operator}"
        if props:
            props_str = ", ".join(f"{k}={v}" for k, v in props.items())
            line += f" ({props_str})"
        line += "\n"

        children = plan_dict.get("children", [])
        for i, child in enumerate(children):
            prefix = "└─ " if i == len(children) - 1 else "├─ "
            line += f"{indent}{prefix}{self._format_plan_tree(child, depth + 1)}"
        return line

    def suggest_optimizations(self, stats: QueryStats) -> List[str]:
        """基于统计信息给出优化建议"""
        tips: List[str] = []

        if stats.execution_time > self.slow_query_threshold:
            tips.append("执行时间过长，考虑建立索引或优化 WHERE 条件")

        if stats.rows_scanned > stats.rows_affected * 10:
            tips.append("扫描行数明显偏大，建议检查过滤条件")

        sql_type = self._get_sql_type(stats.sql)
        if sql_type == "SELECT" and "SELECT *" in stats.sql.upper():
            tips.append("避免使用 SELECT *，推荐明确列名")

        if not stats.optimization_applied:
            tips.append("启用查询优化器可能提升性能")

        return tips


class QueryProfiler:
    """查询分析装饰器"""

    def __init__(self, analyzer: PerformanceAnalyzer):
        self.analyzer = analyzer

    def profile_query(self, sql: str):
        """装饰器：测量函数执行的 SQL 查询性能"""

        def decorator(func):
            def wrapper(*args, **kwargs):
                start = self.analyzer.start_query_timing()
                try:
                    result = func(*args, **kwargs)
                    rows_affected = getattr(result, "affected_rows", 0)
                    rows_scanned = getattr(result, "rows_scanned", 0)
                    stats = self.analyzer.end_query_timing(start, sql, rows_affected, rows_scanned)
                    if hasattr(result, "__dict__"):
                        result.performance_stats = stats
                    return result
                except Exception as e:
                    self.analyzer.end_query_timing(start, sql)
                    raise e

            return wrapper

        return decorator

#
# def main():
#     """简单测试入口"""
#     analyzer = PerformanceAnalyzer()
#
#     test_queries = [
#         "SELECT * FROM users WHERE age > 18",
#         "INSERT INTO users VALUES (1, 'Alice', 25)",
#         "UPDATE users SET age = 26 WHERE id = 1",
#         "SELECT name FROM users ORDER BY age DESC",
#     ]
#
#     for sql in test_queries:
#         start = analyzer.start_query_timing()
#         time.sleep(0.1)  # 模拟执行
#         stats = analyzer.end_query_timing(start, sql, rows_affected=10, rows_scanned=100, plan_type="SeqScan")
#
#         print(f"查询: {sql[:50]}...")
#         print(f"执行时间: {stats.execution_time:.3f}s")
#         tips = analyzer.suggest_optimizations(stats)
#         if tips:
#             print("优化建议:")
#             for t in tips:
#                 print(f"  - {t}")
#         print()
#
#     report = analyzer.get_performance_report()
#     print("=== 性能分析报告 ===")
#     for section, data in report.items():
#         print(f"\n{section}:")
#         if isinstance(data, dict):
#             for k, v in data.items():
#                 print(f"  {k}: {v}")
#         else:
#             print(f"  {data}")
#
#
# if __name__ == "__main__":
#     main()
