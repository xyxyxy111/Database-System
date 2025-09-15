"""
执行计划生成器模块
职责：将解析得到的 AST 转换为逻辑执行计划（树形结构），并提供序列化/可视化输出
（接口与行为保持不变）
"""

import json
from typing import Any, Dict, List, Optional

from .ast_nodes import (
    ASTVisitor,
    BinaryExpression,
    ColumnDef,
    CreateTableStatement,
    DataType,
    DeleteStatement,
    Identifier,
    InsertStatement,
    JoinClause,
    Literal,
    OrderByClause,
    SelectStatement,
    SortExpression,
    SQLProgram,
    UpdateStatement,
)
from .catalog import Catalog


class ExecutionPlan:
    """执行计划树节点"""

    def __init__(self, operator_type: str, **kwargs):
        self.operator_type: str = operator_type
        self.properties: Dict[str, Any] = kwargs
        self.children: List["ExecutionPlan"] = []

    def add_child(self, child: "ExecutionPlan") -> None:
        """将子节点加入当前节点下"""
        self.children.append(child)

    def to_dict(self) -> Dict[str, Any]:
        """把计划节点转换为字典表示，便于 JSON 序列化或进一步处理"""
        node: Dict[str, Any] = {"operator": self.operator_type, "properties": self.properties}
        if self.children:
            node["children"] = [c.to_dict() for c in self.children]
        return node

    def to_json(self, indent: int = 2) -> str:
        """返回 JSON 字符串表示"""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def to_tree_string(self, indent: int = 0) -> str:
        """格式化为树形文本，便于人工查看"""
        pad = "  " * indent
        props = ""
        if self.properties:
            props = "(" + ", ".join(f"{k}={v}" for k, v in self.properties.items()) + ")"
        lines = f"{pad}{self.operator_type}{props}\n"
        for child in self.children:
            lines += child.to_tree_string(indent + 1)
        return lines

    def __repr__(self) -> str:
        return f"ExecutionPlan({self.operator_type}, {self.properties})"


class PlanGenerationError(Exception):
    """生成计划时的错误（携带位置信息方便定位）"""

    def __init__(self, message: str, line: int = 0, column: int = 0):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(f"Plan generation error at line {line}, column {column}: {message}")


class PlanGenerator(ASTVisitor):
    """执行计划生成器：访问 AST 节点并构建对应的执行计划树"""

    def __init__(self, catalog: Optional[Catalog] = None):
        self.catalog: Catalog = catalog or Catalog()
        self.current_plan: Optional[ExecutionPlan] = None

    def generate(self, ast: SQLProgram) -> List[ExecutionPlan]:
        """为 AST 中的每条语句生成一个执行计划（列表形式返回）"""
        plans: List[ExecutionPlan] = []
        for stmt in ast.statements:
            try:
                plan = stmt.accept(self)
                if plan:
                    plans.append(plan)
            except Exception as e:
                raise PlanGenerationError(
                    f"Failed to generate plan: {str(e)}",
                    getattr(stmt, "line", 0),
                    getattr(stmt, "column", 0),
                )
        return plans

    # ---- ASTVisitor 接口实现 ----
    def visit_sql_program(self, node: SQLProgram):
        """处理 SQLProgram 节点（将委托给 generate）"""
        return self.generate(node)

    def visit_create_table_statement(self, node: CreateTableStatement) -> ExecutionPlan:
        """为 CREATE TABLE 构造 CreateTable 操作节点"""
        columns = [
            {"name": col_def.name, "type": col_def.data_type.type_name, "size": col_def.data_type.size}
            for col_def in node.columns
        ]

        plan = ExecutionPlan("CreateTable", table_name=node.table_name, columns=columns)
        return plan

    def visit_insert_statement(self, node: InsertStatement) -> ExecutionPlan:
        """为 INSERT 构造 Insert 操作节点（仅支持字面量值）"""
        values: List[List[Any]] = []
        for row in node.values:
            row_vals: List[Any] = []
            for v in row:
                if isinstance(v, Literal):
                    row_vals.append(v.value)
                else:
                    raise PlanGenerationError(
                        f"Unsupported value type in INSERT: {type(v)}",
                        getattr(v, "line", 0),
                        getattr(v, "column", 0),
                    )
            values.append(row_vals)

        plan = ExecutionPlan("Insert", table_name=node.table_name, columns=node.columns, values=values)
        return plan

    def visit_select_statement(self, node: SelectStatement) -> ExecutionPlan:
        """为 SELECT 构造执行计划：SeqScan -> Filter -> Join -> Sort -> Project（从下到上构建）"""
        scan_plan = ExecutionPlan("SeqScan", table_name=node.from_table)
        current_plan: ExecutionPlan = scan_plan

        # WHERE -> Filter
        if node.where_clause:
            cond = self.convert_expression_to_dict(node.where_clause)
            filter_plan = ExecutionPlan("Filter", condition=cond)
            filter_plan.add_child(current_plan)
            current_plan = filter_plan

        # JOINs（按出现顺序构建，右表为新扫描）
        for join_clause in node.join_clauses:
            right_scan = ExecutionPlan("SeqScan", table_name=join_clause.table_name)
            join_cond = self.convert_expression_to_dict(join_clause.on_condition)
            join_plan = ExecutionPlan("Join", join_type=join_clause.join_type, condition=join_cond)
            join_plan.add_child(current_plan)  # 左侧输入
            join_plan.add_child(right_scan)    # 右侧输入
            current_plan = join_plan

        # ORDER BY -> Sort
        if node.order_by_clause:
            sort_expressions: List[Dict[str, Any]] = []
            for s in node.order_by_clause.expressions:
                if isinstance(s.expression, Identifier):
                    sort_expressions.append(
                        {"column": s.expression.name, "direction": s.direction or "ASC"}
                    )
                else:
                    raise PlanGenerationError(
                        f"Unsupported sort expression: {type(s.expression)}",
                        getattr(s, "line", 0),
                        getattr(s, "column", 0),
                    )
            sort_plan = ExecutionPlan("Sort", sort_expressions=sort_expressions)
            sort_plan.add_child(current_plan)
            current_plan = sort_plan

        # Project（投影）
        select_columns: List[str] = []
        for item in node.select_list:
            if isinstance(item, Identifier):
                select_columns.append(item.name)
            else:
                raise PlanGenerationError(
                    f"Unsupported select item: {type(item)}",
                    getattr(item, "line", 0),
                    getattr(item, "column", 0),
                )

        project_plan = ExecutionPlan("Project", columns=select_columns)
        project_plan.add_child(current_plan)
        return project_plan

    def visit_delete_statement(self, node: DeleteStatement) -> ExecutionPlan:
        """为 DELETE 构建 Delete 节点（可携带 WHERE 条件）"""
        plan = ExecutionPlan("Delete", table_name=node.table_name)
        if node.where_clause:
            plan.properties["condition"] = self.convert_expression_to_dict(node.where_clause)
        return plan

    def visit_update_statement(self, node: UpdateStatement) -> ExecutionPlan:
        """为 UPDATE 构建 Update 节点（包含赋值列表与可选 WHERE）"""
        assignments = [(col, self.convert_expression_to_dict(expr)) for col, expr in node.assignments]
        plan = ExecutionPlan("Update", table_name=node.table_name, assignments=assignments)
        if node.where_clause:
            plan.properties["condition"] = self.convert_expression_to_dict(node.where_clause)
        return plan

    # ---- 表达式 / 其他节点 ----
    def visit_binary_expression(self, node: BinaryExpression):
        """访问二元表达式并返回字典化表示"""
        return self.convert_expression_to_dict(node)

    def visit_identifier(self, node: Identifier):
        """标识符节点转字典"""
        return {"type": "identifier", "name": node.name}

    def visit_literal(self, node: Literal):
        """字面量节点转字典"""
        return {"type": "literal", "value": node.value, "data_type": node.data_type}

    def visit_column_def(self, node: ColumnDef):
        """列定义转字典"""
        return {"name": node.name, "type": node.data_type.type_name, "size": node.data_type.size}

    def visit_data_type(self, node: DataType):
        """数据类型节点转字典"""
        return {"type_name": node.type_name, "size": node.size}

    def visit_order_by_clause(self, node: OrderByClause):
        """ORDER BY 子句处理（返回排序表达式列表的字典表示）"""
        return [expr.accept(self) for expr in node.expressions]

    def visit_sort_expression(self, node: SortExpression):
        """排序项处理"""
        return {"expression": self.convert_expression_to_dict(node.expression), "direction": node.direction or "ASC"}

    def visit_join_clause(self, node: JoinClause):
        """JOIN 子句字典化表示"""
        return {"join_type": node.join_type, "table_name": node.table_name, "condition": self.convert_expression_to_dict(node.on_condition)}

    def convert_expression_to_dict(self, expr: Any) -> Dict[str, Any]:
        """将各种表达式类型归一化为 dict 表示（支持二元、标识符、字面量）"""
        if isinstance(expr, BinaryExpression):
            return {
                "type": "binary",
                "operator": expr.operator,
                "left": self.convert_expression_to_dict(expr.left),
                "right": self.convert_expression_to_dict(expr.right),
            }
        if isinstance(expr, Identifier):
            return {"type": "identifier", "name": expr.name}
        if isinstance(expr, Literal):
            return {"type": "literal", "value": expr.value, "data_type": expr.data_type}
        raise PlanGenerationError(f"Unsupported expression type: {type(expr)}")


# ---------- 辅助函数（用于调试/展示） ----------
def print_plans(plans: List[ExecutionPlan], format_type: str = "tree") -> None:
    """把计划集合打印到控制台（支持 tree/json/dict 三种格式）"""
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


def main() -> None:
    """简单示例：词法 -> 语法 -> 语义 -> 生成执行计划并展示"""
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
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()

        parser = SQLParser(tokens)
        ast = parser.parse()

        analyzer = SemanticAnalyzer()
        success, errors = analyzer.analyze(ast)

        if not success:
            print("语义分析失败:")
            for error in errors:
                print(f"  {error}")
            return

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
