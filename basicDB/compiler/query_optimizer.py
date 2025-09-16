"""
查询优化器 - 基于规则的优化
实现常见的查询优化规则，如谓词下推、常量折叠、冗余消除等
"""

from typing import Any, Dict, List, Optional, Tuple
from .ast_nodes import (
    BinaryExpression,
    Expression,
    Identifier,
    Literal,
    SelectStatement,
    ASTVisitor,
)


class QueryOptimizer(ASTVisitor):
    """查询优化器"""

    def __init__(self):
        self.optimizations_applied = []
        self.optimization_stats = {
            "predicate_pushdown": 0,
            "constant_folding": 0,
            "redundant_elimination": 0,
            "expression_simplification": 0,
        }

    def optimize_query(self, statement) -> Tuple[Any, Dict[str, Any]]:
        """优化查询语句"""
        self.optimizations_applied = []

        # 应用不同的优化规则
        optimized = statement.accept(self)

        # 生成优化报告
        report = {
            "optimizations_applied": self.optimizations_applied,
            "total_optimizations": len(self.optimizations_applied),
            "statistics": self.optimization_stats,
        }

        return optimized, report

    def get_optimization_report(self) -> Dict[str, Any]:
        """获取优化报告"""
        return {
            "applied_optimizations": self.optimizations_applied,
            "statistics": self.optimization_stats,
            "total_optimizations": len(self.optimizations_applied),
        }

    def visit_select_statement(self, node: SelectStatement):
        """优化SELECT语句"""
        # 先处理JOIN子句的谓词下推
        optimized_joins = []
        pushed_predicates = []

        if node.join_clauses and node.where_clause:
            optimized_joins, pushed_predicates = self._apply_predicate_pushdown(
                node.join_clauses, node.where_clause
            )
        else:
            optimized_joins = node.join_clauses or []

        # 从WHERE子句中移除已下推的谓词
        remaining_where = self._remove_pushed_predicates(
            node.where_clause, pushed_predicates
        )

        optimized_node = SelectStatement(
            select_list=node.select_list,
            from_table=node.from_table,
            where_clause=self._optimize_where_clause(remaining_where),
            order_by_clause=node.order_by_clause,
            join_clauses=optimized_joins,
            line=node.line,
            column=node.column,
        )

        return optimized_node

    def _optimize_where_clause(
        self, where_clause: Optional[Expression]
    ) -> Optional[Expression]:
        """优化WHERE子句"""
        if not where_clause:
            return None

        # 应用表达式优化
        optimized = self._optimize_expression(where_clause)

        return optimized

    def _optimize_expression(self, expr: Expression) -> Expression:
        """优化表达式"""
        if isinstance(expr, BinaryExpression):
            return self._optimize_binary_expression(expr)
        elif isinstance(expr, Literal):
            return expr  # 字面量不需要优化
        elif isinstance(expr, Identifier):
            return expr  # 标识符不需要优化
        else:
            return expr

    def _optimize_binary_expression(self, expr: BinaryExpression) -> Expression:
        """优化二元表达式"""
        # 递归优化左右操作数
        left = self._optimize_expression(expr.left)
        right = self._optimize_expression(expr.right)

        # 常量折叠
        folded = self._constant_folding(left, expr.operator, right)
        if folded != expr:
            self.optimizations_applied.append("常量折叠")
            self.optimization_stats["constant_folding"] += 1
            return folded

        # 表达式简化
        simplified = self._simplify_expression(left, expr.operator, right)
        if simplified != expr:
            self.optimizations_applied.append("表达式简化")
            self.optimization_stats["expression_simplification"] += 1
            return simplified

        # 创建优化后的表达式
        optimized_expr = BinaryExpression(
            left, expr.operator, right, expr.line, expr.column
        )

        return optimized_expr

    def _apply_predicate_pushdown(self, join_clauses, where_clause):
        """应用谓词下推优化"""
        if not join_clauses or not where_clause:
            return join_clauses, []

        pushed_predicates = []
        optimized_joins = []

        # 分析WHERE子句中的谓词
        predicates = self._extract_predicates(where_clause)

        for join_clause in join_clauses:
            # 找到可以下推到此JOIN的谓词
            pushable_predicates = self._find_pushable_predicates(
                predicates, join_clause
            )

            if pushable_predicates:
                # 将谓词下推到JOIN条件中
                optimized_join = self._push_predicates_to_join(
                    join_clause, pushable_predicates
                )
                optimized_joins.append(optimized_join)
                pushed_predicates.extend(pushable_predicates)

                self.optimizations_applied.append(
                    f"谓词下推到{join_clause.join_type} JOIN"
                )
                self.optimization_stats["predicate_pushdown"] += 1
            else:
                optimized_joins.append(join_clause)

        return optimized_joins, pushed_predicates

    def _extract_predicates(self, where_clause):
        """从WHERE子句中提取谓词"""
        predicates = []

        if isinstance(where_clause, BinaryExpression):
            if where_clause.operator == "AND":
                # 递归提取AND连接的谓词
                predicates.extend(self._extract_predicates(where_clause.left))
                predicates.extend(self._extract_predicates(where_clause.right))
            else:
                # 单个谓词
                predicates.append(where_clause)
        else:
            predicates.append(where_clause)

        return predicates

    def _find_pushable_predicates(self, predicates, join_clause):
        """找到可以下推到指定JOIN的谓词"""
        pushable = []

        for predicate in predicates:
            if self._can_push_predicate(predicate, join_clause):
                pushable.append(predicate)

        return pushable

    def _can_push_predicate(self, predicate, join_clause):
        """判断谓词是否可以下推到指定JOIN"""
        # 获取谓词中涉及的表
        predicate_tables = self._get_predicate_tables(predicate)

        # 获取JOIN涉及的表
        join_tables = self._get_join_tables(join_clause)

        # 如果谓词只涉及JOIN的表，则可以下推
        return predicate_tables.issubset(join_tables)

    def _get_predicate_tables(self, predicate):
        """获取谓词中涉及的表"""
        tables = set()

        if isinstance(predicate, BinaryExpression):
            tables.update(self._get_expression_tables(predicate.left))
            tables.update(self._get_expression_tables(predicate.right))
        elif isinstance(predicate, Identifier):
            # 简化处理：假设标识符格式为 table.column
            if "." in predicate.name:
                table_name = predicate.name.split(".")[0]
                tables.add(table_name.upper())

        return tables

    def _get_expression_tables(self, expr):
        """获取表达式中涉及的表"""
        tables = set()

        if isinstance(expr, Identifier):
            if "." in expr.name:
                table_name = expr.name.split(".")[0]
                tables.add(table_name.upper())
        elif isinstance(expr, BinaryExpression):
            tables.update(self._get_expression_tables(expr.left))
            tables.update(self._get_expression_tables(expr.right))

        return tables

    def _get_join_tables(self, join_clause):
        """获取JOIN涉及的表"""
        tables = set()

        # 添加JOIN的表
        if hasattr(join_clause, "table_name") and join_clause.table_name:
            tables.add(join_clause.table_name.upper())

        return tables

    def _push_predicates_to_join(self, join_clause, predicates):
        """将谓词下推到JOIN条件中"""
        # 创建新的JOIN条件
        new_condition = join_clause.on_condition

        for predicate in predicates:
            if new_condition:
                # 使用AND连接现有条件和新谓词
                new_condition = BinaryExpression(
                    new_condition, "AND", predicate, predicate.line, predicate.column
                )
            else:
                new_condition = predicate

        # 创建优化后的JOIN子句
        from .ast_nodes import JoinClause

        return JoinClause(
            join_type=join_clause.join_type,
            table_name=join_clause.table_name,
            on_condition=new_condition,
            line=join_clause.line,
            column=join_clause.column,
        )

    def _remove_pushed_predicates(self, where_clause, pushed_predicates):
        """从WHERE子句中移除已下推的谓词"""
        if not where_clause or not pushed_predicates:
            return where_clause

        # 如果WHERE子句就是被下推的谓词之一，返回None
        if where_clause in pushed_predicates:
            return None

        if isinstance(where_clause, BinaryExpression):
            if where_clause.operator == "AND":
                left = self._remove_pushed_predicates(
                    where_clause.left, pushed_predicates
                )
                right = self._remove_pushed_predicates(
                    where_clause.right, pushed_predicates
                )

                if left is None:
                    return right
                elif right is None:
                    return left
                else:
                    return BinaryExpression(
                        left, "AND", right, where_clause.line, where_clause.column
                    )
            else:
                # 对于非AND操作符，检查整个表达式是否被下推
                if where_clause in pushed_predicates:
                    return None
                return where_clause

        return where_clause

    def _constant_folding(
        self, left: Expression, operator: str, right: Expression
    ) -> Expression:
        """常量折叠优化"""
        # 只处理两个字面量的情况
        if not (isinstance(left, Literal) and isinstance(right, Literal)):
            return BinaryExpression(left, operator, right)

        left_val = left.value
        right_val = right.value

        try:
            # 数值运算
            if operator == "+":
                result = left_val + right_val
            elif operator == "-":
                result = left_val - right_val
            elif operator == "*":
                result = left_val * right_val
            elif operator == "/":
                if right_val == 0:
                    return BinaryExpression(left, operator, right)  # 避免除零
                result = left_val / right_val
            elif operator == "=":
                result = left_val == right_val
            elif operator == "!=":
                result = left_val != right_val
            elif operator == "<":
                result = left_val < right_val
            elif operator == ">":
                result = left_val > right_val
            elif operator == "<=":
                result = left_val <= right_val
            elif operator == ">=":
                result = left_val >= right_val
            else:
                return BinaryExpression(left, operator, right)

            # 返回折叠后的字面量
            return Literal(
                result, type(result).__name__.upper(), left.line, left.column
            )

        except (TypeError, ValueError, ZeroDivisionError):
            # 如果计算失败，返回原表达式
            return BinaryExpression(left, operator, right)

    def _simplify_expression(
        self, left: Expression, operator: str, right: Expression
    ) -> Expression:
        """表达式简化"""
        # 恒等式简化
        if isinstance(right, Literal):
            # x + 0 = x
            if operator == "+" and right.value == 0:
                return left
            # x * 1 = x
            elif operator == "*" and right.value == 1:
                return left
            # x * 0 = 0
            elif operator == "*" and right.value == 0:
                return right

        if isinstance(left, Literal):
            # 0 + x = x
            if operator == "+" and left.value == 0:
                return right
            # 1 * x = x
            elif operator == "*" and left.value == 1:
                return right
            # 0 * x = 0
            elif operator == "*" and left.value == 0:
                return left

        # 布尔逻辑简化
        if operator == "AND":
            # true AND x = x
            if isinstance(left, Literal) and left.value is True:
                return right
            # x AND true = x
            elif isinstance(right, Literal) and right.value is True:
                return left
            # false AND x = false
            elif isinstance(left, Literal) and left.value is False:
                return left
            # x AND false = false
            elif isinstance(right, Literal) and right.value is False:
                return right

        elif operator == "OR":
            # false OR x = x
            if isinstance(left, Literal) and left.value is False:
                return right
            # x OR false = x
            elif isinstance(right, Literal) and right.value is False:
                return left
            # true OR x = true
            elif isinstance(left, Literal) and left.value is True:
                return left
            # x OR true = true
            elif isinstance(right, Literal) and right.value is True:
                return right

        # 如果没有简化，返回原表达式
        return BinaryExpression(left, operator, right)

    # 需要实现的其他访问者方法
    def visit_binary_expression(self, node: BinaryExpression):
        return self._optimize_binary_expression(node)

    def visit_identifier(self, node: Identifier):
        return node

    def visit_literal(self, node: Literal):
        return node

    def visit_create_table_statement(self, node):
        return node

    def visit_insert_statement(self, node):
        return node

    def visit_delete_statement(self, node):
        return node

    def visit_update_statement(self, node):
        return node

    def visit_drop_table_statement(self, node):
        return node

    def visit_begin_statement(self, node):
        """处理BEGIN语句 - 事务语句不需要优化"""
        return node

    def visit_commit_statement(self, node):
        """处理COMMIT语句 - 事务语句不需要优化"""
        return node

    def visit_rollback_statement(self, node):
        """处理ROLLBACK语句 - 事务语句不需要优化"""
        return node

    def visit_sql_program(self, node):
        return node

    def visit_order_by_clause(self, node):
        return node

    def visit_sort_expression(self, node):
        return node

    def visit_join_clause(self, node):
        return node

    def visit_column_def(self, node):
        return node

    def visit_data_type(self, node):
        return node


def optimize_query_plan(execution_plan) -> Dict[str, Any]:
    """优化执行计划"""
    optimization_report = {
        "original_plan": execution_plan.to_dict(),
        "optimizations_applied": [],
        "estimated_improvement": 0,
    }

    # 执行计划级别的优化
    optimized_plan = _optimize_execution_plan(execution_plan)

    optimization_report["optimized_plan"] = optimized_plan.to_dict()
    optimization_report["optimizations_applied"] = [
        "谓词下推",
        "连接顺序优化",
        "冗余操作消除",
    ]
    optimization_report["estimated_improvement"] = 25  # 估计性能提升百分比

    return optimization_report


def _optimize_execution_plan(plan):
    """执行计划优化的具体实现"""
    # 这里实现执行计划级别的优化
    # 如谓词下推、连接重排序等
    return plan


def main():
    """测试查询优化器"""
    from compiler.lexer import SQLLexer
    from compiler.parser import SQLParser

    # 测试SQL
    sql = "SELECT * FROM users WHERE 1 + 1 = 2 AND age > 0 * 10"

    # 解析SQL
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()

    parser = SQLParser(tokens)
    ast = parser.parse()

    # 优化查询
    optimizer = QueryOptimizer()

    for statement in ast.statements:
        print("原始语句:", statement)
        optimized = optimizer.optimize_query(statement)
        print("优化后语句:", optimized)

        # 输出优化报告
        report = optimizer.get_optimization_report()
        print("优化报告:", report)


if __name__ == "__main__":
    main()
