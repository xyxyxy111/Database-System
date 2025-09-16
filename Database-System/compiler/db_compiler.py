from compiler import Catalog, PlanGenerator, SemanticAnalyzer, SQLLexer, SQLParser
from typing import Any, Dict, List


class dbCompiler:
    def __init__(self) -> None:
        """
        编译器包装器：初始化必要的编译组件并保存它们的实例。
        保持对外接口不变，只对内部成员增加类型注解以便更清晰。
        """
        self.catalog: Catalog = Catalog()
        self.semantic_analyzer: SemanticAnalyzer = SemanticAnalyzer(self.catalog)
        self.plan_generator: PlanGenerator = PlanGenerator(self.catalog)

    def compile_sql(self, sql: str) -> dict:
        """
        将 SQL 文本走完整个编译流程（词法 -> 语法 -> 语义 -> 执行计划）。
        返回一个字典，包含各阶段的产物与可能的错误信息。
        """
        result: Dict[str, Any] = {
            "success": False,
            "tokens": None,
            "ast": None,
            "semantic_errors": [],
            "execution_plans": [],
            "error_message": None,
        }

        try:
            # 1) 词法分析：得到 token 列表
            lexer: SQLLexer = SQLLexer(sql)
            tokens: List[Any] = lexer.tokenize()
            result["tokens"] = tokens

            # 2) 语法分析：根据 token 构建 AST
            parser: SQLParser = SQLParser(tokens)
            ast = parser.parse()
            result["ast"] = ast

            # 3) 语义检查：返回是否通过以及错误集合
            success, errors = self.semantic_analyzer.analyze(ast)
            result["semantic_errors"] = errors

            if success:
                # 4) 生成执行计划（可能为多条语句生成多个 plan）
                plans = self.plan_generator.generate(ast)
                result["execution_plans"] = plans
                result["success"] = True
            else:
                result["error_message"] = "语义分析未通过"

        except Exception as exc:
            # 捕获任意异常并记录错误信息（不抛出，便于外部统一查看）
            result["error_message"] = str(exc)

        return result

    def print_compilation_result(self, result: dict, verbose: bool = True) -> None:
        """
        将 compile_sql 的返回结果以可读格式打印到控制台。
        verbose=True 时输出更多详细信息（tokens/AST/计划等）。
        """
        if result.get("success"):
            print("✓ SQL 编译成功")

            if verbose:
                # tokens 数量（排除 EOF）
                tokens = result.get("tokens") or []
                token_count = sum(
                    1
                    for t in tokens
                    if getattr(getattr(t, "type", None), "name", None) != "EOF"
                )
                if token_count:
                    print(f"\n词法分析: 共生成 {token_count} 个 token（不含 EOF）")

                # AST 语句计数（健壮地获取属性）
                ast_obj = result.get("ast")
                stmt_count = len(getattr(ast_obj, "statements", [])) if ast_obj else 0
                if stmt_count:
                    print(f"语法分析: 解析出 {stmt_count} 条语句")

                # 打印每条执行计划的摘要
                plans = result.get("execution_plans") or []
                if plans:
                    print("\n执行计划摘要:")
                    print("-" * 40)
                    for idx, plan in enumerate(plans, start=1):
                        op_type = getattr(plan, "operator_type", "<unknown>")
                        print(f"Plan {idx}: {op_type}")
                        props = getattr(plan, "properties", None) or {}
                        for k, v in props.items():
                            print(f"  {k}: {v}")
                        print()
        else:
            print("✗ SQL 编译失败")
            if result.get("error_message"):
                print(f"错误信息: {result['error_message']}")

            sem_errs = result.get("semantic_errors") or []
            if sem_errs:
                print("语义检查发现以下问题:")
                for e in sem_errs:
                    print(f"  - {e}")
