"""
错误诊断增强模块
提供更丰富的错误说明、修复指引和上下文信息
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import re


@dataclass
class ErrorSuggestion:
    """错误修复建议"""

    error_type: str
    message: str
    suggestion: str
    confidence: float  # 置信度范围：0.0 ~ 1.0
    fix_examples: List[str]


@dataclass
class DiagnosticResult:
    """诊断过程的结果"""

    has_errors: bool
    error_count: int
    warnings: List[str]
    suggestions: List[ErrorSuggestion]
    context_info: Dict[str, str]


class ErrorDiagnostics:
    """SQL错误诊断器"""

    def __init__(self) -> None:
        # 预置常见错误模式和关键字
        self.common_errors: Dict[str, str] = self._load_common_errors()
        self.sql_keywords: set[str] = {
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
            "DROP", "CREATE", "TABLE", "INT", "VARCHAR", "CHAR",
            "AND", "OR", "ORDER", "BY", "ASC", "DESC",
            "JOIN", "INNER", "LEFT", "RIGHT",
        }

    def enhance_error_message(self, error_message: str, sql: str = "") -> str:
        """
        增强错误信息，提供上下文分析和修改建议
        """
        try:
            diagnostic: DiagnosticResult = self.diagnose_sql_error(sql, error_message)
            parts: List[str] = [f"执行SQL时发生错误: {error_message}"]

            # 添加修复建议
            if diagnostic.suggestions:
                parts.append("\n💡 建议修复:")
                for i, suggestion in enumerate(diagnostic.suggestions[:3], 1):
                    emoji = "🎯" if suggestion.confidence > 0.8 else "📝"
                    parts.append(f"  {i}. {emoji} {suggestion.suggestion}")
                    if suggestion.fix_examples:
                        parts.append(f"     示例: {suggestion.fix_examples[0]}")

            # 添加提示警告
            if diagnostic.warnings:
                parts.append("\n⚠️ 注意事项:")
                for warning in diagnostic.warnings[:2]:
                    parts.append(f"  - {warning}")

            return "".join(parts)
        except Exception:
            return f"执行SQL时发生错误: {error_message}"

    def diagnose_sql_error(self, sql: str, error_message: str) -> DiagnosticResult:
        """
        核心诊断流程，根据错误信息和SQL文本分析问题
        """
        suggestions: List[ErrorSuggestion] = []
        warnings: List[str] = []
        context_info: Dict[str, str] = {}

        # 归类错误类型
        error_type: str = self._classify_error(error_message)
        context_info["error_type"] = error_type

        # 针对不同类型做专项分析
        if "syntax" in error_message.lower() or "parse" in error_message.lower():
            suggestions.extend(self._diagnose_syntax_error(sql, error_message))
        elif "semantic" in error_message.lower():
            suggestions.extend(self._diagnose_semantic_error(sql, error_message))
        elif "lexical" in error_message.lower():
            suggestions.extend(self._diagnose_lexical_error(sql, error_message))
        else:
            suggestions.extend(self._diagnose_general_error(sql, error_message))

        # 额外检查常见隐患
        warnings.extend(self._check_common_issues(sql))

        # 增加SQL上下文信息
        context_info.update(self._analyze_sql_context(sql))

        return DiagnosticResult(
            has_errors=True,
            error_count=1,
            warnings=warnings,
            suggestions=suggestions,
            context_info=context_info,
        )

    def _classify_error(self, error_message: str) -> str:
        """根据关键字大致分类错误类型"""
        msg = error_message.lower()
        if "table" in msg and "not" in msg and "exist" in msg:
            return "表不存在"
        elif "column" in msg and "not" in msg and "exist" in msg:
            return "列不存在"
        elif "syntax" in msg or "parse" in msg:
            return "语法错误"
        elif "type" in msg and "mismatch" in msg:
            return "类型不匹配"
        elif "unexpected" in msg:
            return "意外符号"
        return "未知错误"

    def _diagnose_syntax_error(self, sql: str, error_message: str) -> List[ErrorSuggestion]:
        """分析语法层面的错误"""
        suggestions: List[ErrorSuggestion] = []

        if not sql.strip().endswith(";"):
            suggestions.append(ErrorSuggestion(
                error_type="缺少分号",
                message="SQL结尾可能遗漏分号",
                suggestion="请在语句末尾补充 ';'",
                confidence=0.8,
                fix_examples=[f"{sql.strip()};"],
            ))

        if self._check_parentheses_mismatch(sql):
            suggestions.append(ErrorSuggestion(
                error_type="括号不匹配",
                message="括号数量不一致",
                suggestion="检查并修复括号闭合问题",
                confidence=0.9,
                fix_examples=["CREATE TABLE users (id INT, name VARCHAR(50));"],
            ))

        if self._check_quotes_mismatch(sql):
            suggestions.append(ErrorSuggestion(
                error_type="引号不匹配",
                message="存在未闭合的引号",
                suggestion="请检查字符串是否正确使用引号包裹",
                confidence=0.9,
                fix_examples=["SELECT * FROM users WHERE name = 'Alice';"],
            ))

        for word, suggestion_word in self._check_keyword_spelling(sql):
            suggestions.append(ErrorSuggestion(
                error_type="关键字拼写错误",
                message=f"可能拼写错误: '{word}'",
                suggestion=f"是否想写成 '{suggestion_word}'?",
                confidence=0.7,
                fix_examples=[sql.replace(word, suggestion_word)],
            ))

        return suggestions

    def _diagnose_semantic_error(self, sql: str, error_message: str) -> List[ErrorSuggestion]:
        """分析语义相关错误（如表不存在、列不存在）"""
        suggestions: List[ErrorSuggestion] = []

        if "table" in error_message.lower() and "not exist" in error_message.lower():
            match = re.search(r"'([^']+)'", error_message)
            if match:
                table_name = match.group(1)
                suggestions.append(ErrorSuggestion(
                    error_type="表不存在",
                    message=f"未找到表 '{table_name}'",
                    suggestion=f"检查表名拼写，或先创建表 '{table_name}'",
                    confidence=0.9,
                    fix_examples=[f"CREATE TABLE {table_name} (id INT, name VARCHAR(50));"],
                ))

        if "column" in error_message.lower() and "not exist" in error_message.lower():
            match = re.search(r"'([^']+)'", error_message)
            if match:
                col = match.group(1)
                suggestions.append(ErrorSuggestion(
                    error_type="列不存在",
                    message=f"未找到列 '{col}'",
                    suggestion=f"请确认表中是否包含列 '{col}'",
                    confidence=0.9,
                    fix_examples=["-- 使用 DESCRIBE table_name 检查结构"],
                ))

        return suggestions

    def _diagnose_lexical_error(self, sql: str, error_message: str) -> List[ErrorSuggestion]:
        """分析词法错误（意外字符等）"""
        suggestions: List[ErrorSuggestion] = []
        if "unexpected character" in error_message.lower():
            match = re.search(r"unexpected character '(.)'", error_message)
            if match:
                ch = match.group(1)
                suggestions.append(ErrorSuggestion(
                    error_type="非法字符",
                    message=f"出现了意外字符 '{ch}'",
                    suggestion="移除或转义该字符",
                    confidence=0.8,
                    fix_examples=["-- 删除该字符", "SELECT 'abc#123';"],
                ))
        return suggestions

    def _diagnose_general_error(self, sql: str, error_message: str) -> List[ErrorSuggestion]:
        """无法归类时提供通用建议"""
        return [ErrorSuggestion(
            error_type="通用错误",
            message="出现未分类错误",
            suggestion="检查SQL是否符合基本语法结构",
            confidence=0.5,
            fix_examples=[
                "SELECT col FROM tbl;",
                "INSERT INTO tbl VALUES (1, 'x');",
                "UPDATE tbl SET col = 1 WHERE id = 2;",
            ],
        )]

    def _check_common_issues(self, sql: str) -> List[str]:
        """检查常见风险点"""
        warns: List[str] = []
        upper_sql = sql.upper()

        if "SELECT *" in upper_sql:
            warns.append("建议不要使用 SELECT *，请显式指定列名")
        if "DELETE" in upper_sql and "WHERE" not in upper_sql:
            warns.append("DELETE 缺少 WHERE 子句，将删除所有数据")
        if "UPDATE" in upper_sql and "WHERE" not in upper_sql:
            warns.append("UPDATE 缺少 WHERE 子句，将更新所有行")

        return warns

    def _analyze_sql_context(self, sql: str) -> Dict[str, str]:
        """获取SQL上下文信息（类型、长度、复杂度等）"""
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
        ctx["line_count"] = str(len(sql.splitlines()))

        if "JOIN" in sql_upper:
            ctx["has_joins"] = "true"
        if "(" in sql and "SELECT" in sql_upper:
            ctx["has_subqueries"] = "possibly"

        return ctx

    def _check_parentheses_mismatch(self, sql: str) -> bool:
        """检测括号配对情况"""
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
        """检测单双引号是否成对"""
        in_single, in_double = False, False
        for ch in sql:
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
        return in_single or in_double

    def _check_keyword_spelling(self, sql: str) -> List[Tuple[str, str]]:
        """关键字拼写检查（基于编辑距离）"""
        words = re.findall(r"\b[A-Za-z]+\b", sql)
        result: List[Tuple[str, str]] = []
        for w in words:
            w_up = w.upper()
            if w_up not in self.sql_keywords:
                for kw in self.sql_keywords:
                    if self._levenshtein_distance(w_up, kw) <= 1:
                        result.append((w, kw))
                        break
        return result

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """莱文斯坦距离算法"""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        if not s2:
            return len(s1)

        prev = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            curr = [i + 1]
            for j, c2 in enumerate(s2):
                ins = prev[j + 1] + 1
                del_ = curr[j] + 1
                sub = prev[j] + (c1 != c2)
                curr.append(min(ins, del_, sub))
            prev = curr
        return prev[-1]

    def _load_common_errors(self) -> Dict[str, str]:
        """返回常见错误模式及描述"""
        return {
            "table_not_exists": "表不存在，请确认表是否已创建",
            "column_not_exists": "列不存在，请检查列名或表结构",
            "syntax_error": "语法错误，请检查SQL语句",
            "type_mismatch": "类型不匹配，请检查数据类型是否一致",
        }


def main() -> None:
    """测试示例"""
    diag = ErrorDiagnostics()
    test_cases = [
        ("SELCT * FROM users", "Parse error: Expected SELECT"),
        ("SELECT * FROM nonexistent_table", "Table 'nonexistent_table' does not exist"),
        ("SELECT name FROM users WHERE age = 'invalid'", "Type mismatch"),
        ("SELECT * FROM users WHERE", "Unexpected end of input"),
    ]

    for sql, err in test_cases:
        print(f"\nSQL: {sql}")
        print(f"错误: {err}")
        result = diag.diagnose_sql_error(sql, err)
        for sug in result.suggestions:
            print(f"  - {sug.message}")
            print(f"    建议: {sug.suggestion} (置信度 {sug.confidence:.0%})")
        if result.warnings:
            print("  警告:", "; ".join(result.warnings))


if __name__ == "__main__":
    main()
