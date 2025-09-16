"""
执行计划生成器模块
职责：将解析得到的 AST 转换为逻辑执行计划（树形结构），并提供序列化/可视化输出
（接口与行为保持不变）
"""

import json
from typing import Any, Dict, List, Optional

from .ast_nodes import (
    AggregateFunction,
    ASTVisitor,
    BeginStatement,
    BinaryExpression,
    ColumnDef,
    CommitStatement,
    CreateTableStatement,
    DataType,
    DeleteStatement,
    DropTableStatement,
    Identifier,
    InsertStatement,
    JoinClause,
    Literal,
    OrderByClause,
    RollbackStatement,
    SelectStatement,
    SortExpression,
    SQLProgram,
    UpdateStatement,
)
from .catalog import Catalog


class ExecutionPlan:
    """执行计划节点基类"""

    def __init__(self, operator_type: str, **kwargs):
        self.operator_type = operator_type
        self.properties = kwargs
        self.children: List["ExecutionPlan"] = []

    def add_child(self, child: "ExecutionPlan"):
        """添加子节点"""
        self.children.append(child)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {"operator": self.operator_type, "properties": self.properties}

        if self.children:
            result["children"] = [child.to_dict() for child in self.children]

        return result

    def to_json(self, indent: int = 2) -> str:
        """转换为JSON格式"""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def to_tree_string(self, indent: int = 0) -> str:
        """转换为树形字符串"""
        prefix = "  " * indent
        result = f"{prefix}{self.operator_type}"

        if self.properties:
            props = ", ".join(f"{k}={v}" for k, v in self.properties.items())
            result += f"({props})"

        result += "\n"

        for child in self.children:
            result += child.to_tree_string(indent + 1)

        return result

    def __repr__(self):
        return f"ExecutionPlan({self.operator_type}, {self.properties})"


class PlanGenerationError(Exception):
    """执行计划生成错误"""

    def __init__(self, message: str, line: int = 0, column: int = 0):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(
            f"Plan generation error at line {line}, column {column}: {message}"
        )


class PlanGenerator(ASTVisitor):
    """执行计划生成器"""

    def __init__(self, catalog: Optional[Catalog] = None):
        self.catalog = catalog or Catalog()
        self.current_plan: Optional[ExecutionPlan] = None

    def generate(self, ast: SQLProgram) -> List[ExecutionPlan]:
        """生成执行计划"""
        plans = []

        for statement in ast.statements:
            try:
                plan = statement.accept(self)
                if plan:
                    plans.append(plan)
            except Exception as e:
                raise PlanGenerationError(
                    f"Failed to generate plan: {str(e)}",
                    getattr(statement, "line", 0),
                    getattr(statement, "column", 0),
                )

        return plans

    def visit_sql_program(self, node: SQLProgram):
        """访问SQL程序节点"""
        return self.generate(node)

    def visit_create_table_statement(self, node: CreateTableStatement) -> ExecutionPlan:
        """访问CREATE TABLE语句节点"""
        # 提取列信息
        columns = []
        for col_def in node.columns:
            col_info = {
                "name": col_def.name,
                "type": col_def.data_type.type_name,
                "size": col_def.data_type.size,
            }
            columns.append(col_info)

        plan = ExecutionPlan("CreateTable", table_name=node.table_name, columns=columns)

        return plan

    def visit_insert_statement(self, node: InsertStatement) -> ExecutionPlan:
        """访问INSERT语句节点"""
        # 处理值列表
        values = []
        for value_row in node.values:
            row_values = []
            for value in value_row:
                if isinstance(value, Literal):
                    row_values.append(value.value)
                else:
                    raise PlanGenerationError(
                        f"Unsupported value type in INSERT: {type(value)}",
                        value.line,
                        value.column,
                    )
            values.append(row_values)

        plan = ExecutionPlan(
            "Insert", table_name=node.table_name, columns=node.columns, values=values
        )

        return plan

    def visit_select_statement(self, node: SelectStatement) -> ExecutionPlan:
        """访问SELECT语句节点"""
        # 创建扫描操作
        scan_plan = ExecutionPlan("SeqScan", table_name=node.from_table)

        current_plan = scan_plan

        # 添加过滤操作（WHERE子句）
        if node.where_clause:
            filter_condition = self.convert_expression_to_dict(node.where_clause)
            filter_plan = ExecutionPlan("Filter", condition=filter_condition)
            filter_plan.add_child(current_plan)
            current_plan = filter_plan

        # 添加连接操作（JOIN子句）
        for join_clause in node.join_clauses:
            # 创建右表的扫描操作
            right_scan = ExecutionPlan("SeqScan", table_name=join_clause.table_name)

            # 创建JOIN操作
            join_condition = self.convert_expression_to_dict(join_clause.on_condition)
            join_plan = ExecutionPlan(
                "Join", join_type=join_clause.join_type, condition=join_condition
            )

            join_plan.add_child(current_plan)  # 左表
            join_plan.add_child(right_scan)  # 右表
            current_plan = join_plan

        # 添加排序操作（ORDER BY子句）
        if node.order_by_clause:
            sort_expressions = []
            for sort_expr in node.order_by_clause.expressions:
                if isinstance(sort_expr.expression, Identifier):
                    sort_expressions.append(
                        {
                            "column": sort_expr.expression.name,
                            "direction": sort_expr.direction or "ASC",
                        }
                    )
                else:
                    raise PlanGenerationError(
                        f"Unsupported sort expression: {type(sort_expr.expression)}",
                        getattr(sort_expr, "line", 0),
                        getattr(sort_expr, "column", 0),
                    )

            sort_plan = ExecutionPlan("Sort", sort_expressions=sort_expressions)
            sort_plan.add_child(current_plan)
            current_plan = sort_plan

        # 添加投影操作（SELECT列表）
        select_columns = []
        select_list = []

        for select_item in node.select_list:
            if isinstance(select_item, Identifier):
                select_columns.append(select_item.name)
                select_list.append(select_item)
            elif isinstance(select_item, AggregateFunction):
                # 聚合函数处理
                select_columns.append(f"{select_item.function_name}(...)")
                select_list.append(select_item)
            else:
                raise PlanGenerationError(
                    f"Unsupported select item: {type(select_item)}",
                    select_item.line,
                    select_item.column,
                )

        project_plan = ExecutionPlan(
            "Project", columns=select_columns, select_list=select_list
        )
        project_plan.add_child(current_plan)

        return project_plan

    def visit_delete_statement(self, node: DeleteStatement) -> ExecutionPlan:
        """访问DELETE语句节点"""
        plan = ExecutionPlan("Delete", table_name=node.table_name)

        # 如果有WHERE条件，添加过滤条件
        if node.where_clause:
            condition = self.convert_expression_to_dict(node.where_clause)
            plan.properties["condition"] = condition

        return plan

    def visit_update_statement(self, node: UpdateStatement) -> ExecutionPlan:
        """访问UPDATE语句节点"""
        plan = ExecutionPlan(
            "Update",
            table_name=node.table_name,
            assignments=[
                (col, self.convert_expression_to_dict(expr))
                for col, expr in node.assignments
            ],
        )

        # 如果有WHERE条件，添加过滤条件
        if node.where_clause:
            condition = self.convert_expression_to_dict(node.where_clause)
            plan.properties["condition"] = condition

        return plan


    def visit_drop_table_statement(self, node: DropTableStatement) -> ExecutionPlan:
        """为 DROP TABLE 构建 DropTable 节点"""
        # 实际删除表
        if self.catalog.table_exists(node.table_name):
            self.catalog.drop_table(node.table_name)

        plan = ExecutionPlan("DropTable", table_name=node.table_name)
        return plan

    def visit_begin_statement(self, node: BeginStatement) -> ExecutionPlan:
        """访问BEGIN语句节点"""
        return ExecutionPlan("Begin")

    def visit_commit_statement(self, node: CommitStatement) -> ExecutionPlan:
        """访问COMMIT语句节点"""
        return ExecutionPlan("Commit")

    def visit_rollback_statement(self, node: RollbackStatement) -> ExecutionPlan:
        """访问ROLLBACK语句节点"""
        return ExecutionPlan("Rollback")

    def visit_binary_expression(self, node: BinaryExpression):
        """访问二元表达式节点"""
        return self.convert_expression_to_dict(node)

    def visit_identifier(self, node: Identifier):
        """访问标识符节点"""
        return {"type": "identifier", "name": node.name}

    def visit_literal(self, node: Literal):
        """访问字面量节点"""
        return {"type": "literal", "value": node.value, "data_type": node.data_type}

    def visit_column_def(self, node: ColumnDef):
        """访问列定义节点"""
        return {
            "name": node.name,
            "type": node.data_type.type_name,
            "size": node.data_type.size,
        }

    def visit_data_type(self, node: DataType):
        """访问数据类型节点"""
        return {"type_name": node.type_name, "size": node.size}

    def visit_order_by_clause(self, node: OrderByClause):
        """访问ORDER BY子句节点"""
        return [sort_expr.accept(self) for sort_expr in node.expressions]

    def visit_sort_expression(self, node: SortExpression):
        """访问排序表达式节点"""
        return {
            "expression": self.convert_expression_to_dict(node.expression),
            "direction": node.direction or "ASC",
        }

    def visit_join_clause(self, node: JoinClause):
        """访问JOIN子句节点"""
        return {
            "join_type": node.join_type,
            "table_name": node.table_name,
            "condition": self.convert_expression_to_dict(node.on_condition),
        }

    def convert_expression_to_dict(self, expr) -> Dict[str, Any]:
        """将表达式转换为字典格式"""
        if isinstance(expr, BinaryExpression):
            return {
                "type": "binary",
                "operator": expr.operator,
                "left": self.convert_expression_to_dict(expr.left),
                "right": self.convert_expression_to_dict(expr.right),
            }
        elif isinstance(expr, Identifier):
            return {"type": "identifier", "name": expr.name}
        elif isinstance(expr, Literal):
            return {"type": "literal", "value": expr.value, "data_type": expr.data_type}
        else:
            raise PlanGenerationError(f"Unsupported expression type: {type(expr)}")


def print_plans(plans: List[ExecutionPlan], format_type: str = "tree"):
    """打印执行计划"""
    print("执行计划:")
    print("=" * 50)

    for i, plan in enumerate(plans, 1):
        print(f"Plan {i}:")

        if format_type == "tree":
            print(plan.to_tree_string())
        elif format_type == "json":
            print(plan.to_json())
        else:
            print(plan.to_dict())

        print("-" * 30)


def main():
    """测试用例"""
    from .lexer import SQLLexer
    from .parser import SQLParser
    from .semantic_analyzer import SemanticAnalyzer

    sql = """
    CREATE TABLE student(id INT, name VARCHAR(50), age INT);
    INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
    SELECT id, name FROM student WHERE age > 18;
    DELETE FROM student WHERE id = 1;
    """

    print("输入SQL:")
    print(sql)
    print()

    try:
        # 词法分析
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()

        # 语法分析
        parser = SQLParser(tokens)
        ast = parser.parse()

        # 语义分析
        analyzer = SemanticAnalyzer()
        success, errors = analyzer.analyze(ast)

        if not success:
            print("语义分析失败:")
            for error in errors:
                print(f"  {error}")
            return

        # 执行计划生成
        generator = PlanGenerator(analyzer.catalog)
        plans = generator.generate(ast)

        print("执行计划生成成功!")
        print_plans(plans, "tree")

        print("\nJSON格式:")
        print_plans(plans, "json")

    except Exception as e:
        print(f"处理失败: {e}")


if __name__ == "__main__":
    main()
