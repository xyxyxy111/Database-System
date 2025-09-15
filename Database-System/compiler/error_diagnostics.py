"""
错误诊断扩展模块
用于分析 SQL 执行过程中的错误，给出定位说明、修复思路及上下文环境
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import re


@dataclass
class ErrorSuggestion:
    """错误修复提示"""

    error_type: str
    message: str
    suggestion: str
    confidence: float  # 置信度：范围 0.0 - 1.0
    fix_examples: List[str]


@dataclass
class DiagnosticResult:
    """诊断输出"""

    has_errors: bool
    error_count: int
    warnings: List[str]
    suggestions: List[ErrorSuggestion]
    context_info: Dict[str, str]


class ErrorDiagnostics:
    """SQL 错误诊断核心类"""

    def __init__(self):
        self.common_errors: Dict[str, str] = self._load_common_errors()
        self.sql_keywords: set[str] = {
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
            "CREATE", "TABLE", "INT", "VARCHAR", "CHAR",
            "AND", "OR", "ORDER", "BY", "ASC", "DESC",
            "JOIN", "INNER", "LEFT", "RIGHT",
        }

    def enhance_error_message(self, error_message: str, sql: str = "") -> str:
        """基于诊断结果生成更详细的错误报告"""
        try:
            diagnostic = self.diagnose_sql_error(sql, error_message)

            parts: List[str] = [f"执行SQL时发生错误: {error_message}"]

            # 附加修复建议
            if diagnostic.suggestions:
                parts.append("\n💡 建议修复:")
                for i, suggestion in enumerate(diagnostic.suggestions[:3], 1):
                    symbol = "🎯" if suggestion.confidence > 0.8 else "📝"
                    parts.append(f"  {i}. {symbol} {suggestion.suggestion}")
                    if suggestion.fix_examples:
                        parts.append(f"     示例: {suggestion.fix_examples[0]}")

            # 附加警告信息
            if diagnostic.warnings:
                parts.append("\n⚠️  注意:")
                for warn in diagnostic.warnings[:2]:
                    parts.append(f"  - {warn}")

            return "".join(parts)

        except Exception:
            return f"执行SQL时发生错误: {error_message}"

    def diagnose_sql_error(self, sql: str, error_message: str) -> DiagnosticResult:
        """执行错误分析，返回诊断结果"""
        suggestions: List[ErrorSuggestion] = []
        warnings: List[str] = []
        context_info: Dict[str, str] = {}

        error_type: str = self._classify_error(error_message)
        context_info["error_type"] = error_type

        if "syntax" in error_message.lower() or "parse" in error_message.lower():
            suggestions.extend(self._diagnose_syntax_error(sql, error_message))
        elif "semantic" in error_message.lower():
            suggestions.extend(self._diagnose_semantic_error(sql, error_message))
        elif "lexical" in error_message.lower():
            suggestions.extend(self._diagnose_lexical_error(sql, error_message))
        else:
            suggestions.extend(self._diagnose_general_error(sql, error_message))

        warnings.extend(self._check_common_issues(sql))
        context_info.update(self._analyze_sql_context(sql))

        return DiagnosticResult(
            has_errors=True,
            error_count=1,
            warnings=warnings,
            suggestions=suggestions,
            context_info=context_info,
        )

    def _classify_error(self, error_message: str) -> str:
        """依据错误文本推断错误类别"""
        msg = error_message.lower()
        if "table" in msg and "not" in msg and "exist" in msg:
            return "table_not_exists"
        if "column" in msg and "not" in msg and "exist" in msg:
            return "column_not_exists"
        if "syntax" in msg or "parse" in msg:
            return "syntax_error"
        if "type" in msg and "mismatch" in msg:
            return "type_mismatch"
        if "unexpected" in msg:
            return "unexpected_token"
        return "unknown_error"

    def _diagnose_syntax_error(self, sql: str, error_message: str) -> List[ErrorSuggestion]:
        """针对语法错误提供诊断建议"""
        suggestions: List[ErrorSuggestion] = []

        if not sql.strip().endswith(";"):
            suggestions.append(
                ErrorSuggestion(
                    error_type="missing_semicolon",
                    message="可能遗漏分号",
                    suggestion="在语句末尾添加分号 (;)",
                    confidence=0.8,
                    fix_examples=[f"{sql.strip()};"],
                )
            )

        if self._check_parentheses_mismatch(sql):
            suggestions.append(
                ErrorSuggestion(
                    error_type="parentheses_mismatch",
                    message="括号数量不匹配",
                    suggestion="检查括号配对是否完整",
                    confidence=0.9,
                    fix_examples=["CREATE TABLE users (id INT, name VARCHAR(50));"],
                )
            )

        if self._check_quotes_mismatch(sql):
            suggestions.append(
                ErrorSuggestion(
                    error_type="quotes_mismatch",
                    message="字符串引号不成对",
                    suggestion="确保所有字符串被完整的引号包裹",
                    confidence=0.9,
                    fix_examples=["SELECT * FROM users WHERE name = 'Alice';"],
                )
            )

        for word, target in self._check_keyword_spelling(sql):
            suggestions.append(
                ErrorSuggestion(
                    error_type="keyword_misspelling",
                    message=f"可能的关键字拼写错误: '{word}'",
                    suggestion=f"是否想要使用 '{target}'?",
                    confidence=0.7,
                    fix_examples=[sql.replace(word, target)],
                )
            )

        return suggestions

    def _diagnose_semantic_error(self, sql: str, error_message: str) -> List[ErrorSuggestion]:
        """针对语义错误给出修复提示"""
        suggestions: List[ErrorSuggestion] = []

        if "table" in error_message.lower() and "not exist" in error_message.lower():
            match = re.search(r"'([^']+)'.*not exist", error_message)
            if match:
                tbl = match.group(1)
                suggestions.append(
                    ErrorSuggestion(
                        error_type="table_not_exists",
                        message=f"表 '{tbl}' 不存在",
                        suggestion=f"请确认表名拼写，或先创建 '{tbl}'",
                        confidence=0.9,
                        fix_examples=[f"CREATE TABLE {tbl} (id INT, name VARCHAR(50));"],
                    )
                )

        elif "column" in error_message.lower() and "not exist" in error_message.lower():
            match = re.search(r"'([^']+)'.*not exist", error_message)
            if match:
                col = match.group(1)
                suggestions.append(
                    ErrorSuggestion(
                        error_type="column_not_exists",
                        message=f"列 '{col}' 不存在",
                        suggestion=f"确认表结构是否包含 '{col}'，或修改为正确列名",
                        confidence=0.9,
                        fix_examples=["-- 使用正确的列名", "-- 或检查表结构定义"],
                    )
                )

        return suggestions

    def _diagnose_lexical_error(self, sql: str, error_message: str) -> List[ErrorSuggestion]:
        """针对词法层面的错误提供提示"""
        suggestions: List[ErrorSuggestion] = []

        if "unexpected character" in error_message.lower():
            match = re.search(r"unexpected character '(.)'", error_message)
            if match:
                ch = match.group(1)
                suggestions.append(
                    ErrorSuggestion(
                        error_type="unexpected_character",
                        message=f"意外字符 '{ch}'",
                        suggestion=f"移除或修正字符 '{ch}'，它可能不是合法的 SQL 符号",
                        confidence=0.8,
                        fix_examples=["去掉该字符", "若是特殊字符，请使用引号包裹"],
                    )
                )
        return suggestions

    def _diagnose_general_error(self, sql: str, error_message: str) -> List[ErrorSuggestion]:
        """其他错误的通用提示"""
        return [
            ErrorSuggestion(
                error_type="general_help",
                message="遇到未分类错误",
                suggestion="检查 SQL 的基本结构是否符合规范",
                confidence=0.5,
                fix_examples=[
                    "SELECT column_name FROM table_name;",
                    "INSERT INTO table_name VALUES (value1, value2);",
                    "UPDATE table_name SET column = value WHERE condition;",
                ],
            )
        ]

    def _check_common_issues(self, sql: str) -> List[str]:
        """常见风险性写法检查"""
        warns: List[str] = []
        sql_upper = sql.upper()

        if "SELECT *" in sql_upper:
            warns.append("建议避免 SELECT *，应明确列出需要的字段")

        if "DELETE" in sql_upper and "WHERE" not in sql_upper:
            warns.append("DELETE 缺少 WHERE 条件，将删除整表数据")

        if "UPDATE" in sql_upper and "WHERE" not in sql_upper:
            warns.append("UPDATE 缺少 WHERE 条件，将更新整表数据")

        return warns

    def _analyze_sql_context(self, sql: str) -> Dict[str, str]:
        """提取 SQL 上下文特征"""
        ctx: Dict[str, str] = {}
        sql_upper = sql.upper().strip()

        if sql_upper.startswith("SELECT"):
            ctx["sql_type"] = "SELECT"
        elif sql_upper.startswith("INSERT"):
            ctx["sql_type"] = "INSERT"
        elif sql_upper.startswith("UPDATE"):
            ctx["sql_type"] = "UPDATE"
        elif sql_upper.startswith("DELETE"):
            ctx["sql_type"] = "DELETE"
        elif sql_upper.startswith("CREATE"):
            ctx["sql_type"] = "CREATE"
        else:
            ctx["sql_type"] = "UNKNOWN"

        ctx["sql_length"] = str(len(sql))
        ctx["line_count"] = str(sql.count("\n") + 1)

        if "JOIN" in sql_upper:
            ctx["has_joins"] = "true"
        if "(" in sql and "SELECT" in sql_upper:
            ctx["has_subqueries"] = "possibly"

        return ctx

    def _check_parentheses_mismatch(self, sql: str) -> bool:
        """验证括号是否成对"""
        count = 0
        for ch in sql:
            if ch == "(":
                count += 1
            elif ch == ")":
                count -= 1
                if count < 0:
                    return True
        return count != 0

    def _check_quotes_mismatch(self, sql: str) -> bool:
        """验证引号成对情况"""
        single, double = False, False
        for ch in sql:
            if ch == "'" and not double:
                single = not single
            elif ch == '"' and not single:
                double = not double
        return single or double

    def _check_keyword_spelling(self, sql: str) -> List[Tuple[str, str]]:
        """检测关键字拼写是否接近正确词"""
        results: List[Tuple[str, str]] = []
        words = re.findall(r"\b[A-Za-z]+\b", sql)

        for w in words:
            upper = w.upper()
            if upper not in self.sql_keywords:
                for kw in self.sql_keywords:
                    if self._levenshtein_distance(upper, kw) <= 1:
                        results.append((w, kw))
                        break
        return results

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """计算字符串编辑距离"""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        if not s2:
            return len(s1)

        prev = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            curr = [i + 1]
            for j, c2 in enumerate(s2):
                ins, dele, sub = prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)
                curr.append(min(ins, dele, sub))
            prev = curr
        return prev[-1]

    def _load_common_errors(self) -> Dict[str, str]:
        """预置常见错误模式"""
        return {
            "table_not_exists": "表不存在，请检查表名或先创建表",
            "column_not_exists": "列不存在，请确认列名或表结构",
            "syntax_error": "语法有误，请检查SQL格式",
            "type_mismatch": "数据类型不匹配，请检查字段类型",
        }


def main():
    """模块功能演示"""
    diag = ErrorDiagnostics()
    cases = [
        ("SELCT * FROM users", "Parse error: Expected SELECT"),
        ("SELECT * FROM nonexistent_table", "Table 'nonexistent_table' does not exist"),
        ("SELECT name FROM users WHERE age = 'invalid'", "Type mismatch"),
        ("SELECT * FROM users WHERE", "Unexpected end of input"),
    ]

    for sql, msg in cases:
        print(f"\nSQL: {sql}")
        print(f"错误: {msg}")
        result = diag.diagnose_sql_error(sql, msg)
        for sug in result.suggestions:
            print(f"  - {sug.message}")
            print(f"    建议: {sug.suggestion}")
            print(f"    置信度: {sug.confidence:.1%}")
        if result.warnings:
            print("  警告:", ", ".join(result.warnings))


if __name__ == "__main__":
    main()
