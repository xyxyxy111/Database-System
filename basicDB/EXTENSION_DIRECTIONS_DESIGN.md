# MiniDB 数据库扩展方向设计方案

## 📋 项目背景分析

### 当前系统架构评估 ✅

**SQL 编译器架构**：
- **词法分析器** (lexer.py): 支持关键字扩展，Token类型可扩展，错误处理完善
- **语法分析器** (parser.py): 递归下降解析，易于添加新语法规则
- **语义分析器** (semantic_analyzer.py): 访问者模式，表/列验证，类型检查
- **执行计划生成器** (plan_generator.py): ExecutionPlan树结构，支持复杂算子组合

**查询执行引擎**：
- **查询执行器** (query_executor.py): 递归执行计划树，支持 PROJECT/FILTER/SEQSCAN 算子
- **存储引擎** (storage/): 页面管理，缓冲机制，完整的 CRUD 支持
- **目录管理** (catalog.py): 表/列元数据管理，类型系统

**扩展基础评估**：
- ✅ 架构模块化程度高，易于扩展
- ✅ 访问者模式支持新AST节点类型
- ✅ 执行计划树结构支持复杂算子
- ✅ 错误处理分层完善，位置信息准确

---

## 🚀 三大扩展方向详细设计

### 方向1: 语法扩展 (Syntax Extension)

#### 1.1 JOIN 语句支持

**新增语法**：
```sql
-- INNER JOIN
SELECT t1.id, t1.name, t2.score 
FROM students t1 
INNER JOIN scores t2 ON t1.id = t2.student_id;

-- LEFT JOIN
SELECT t1.name, t2.score
FROM students t1
LEFT JOIN scores t2 ON t1.id = t2.student_id;

-- RIGHT JOIN (可选)
SELECT t1.name, t2.score  
FROM students t1
RIGHT JOIN scores t2 ON t1.id = t2.student_id;
```

**实现计划**：
1. **AST 节点扩展**:
   ```python
   class JoinExpression(Expression):
       def __init__(self, left_table, right_table, join_type, on_condition):
           self.left_table = left_table
           self.right_table = right_table  
           self.join_type = join_type  # 'INNER', 'LEFT', 'RIGHT'
           self.on_condition = on_condition
   
   class SelectStatement(Statement):  # 扩展现有类
       def __init__(self, select_list, from_clause, joins=None, where_clause=None):
           # 添加 joins 参数支持多表查询
   ```

2. **词法分析器扩展**:
   ```python
   KEYWORDS.update({
       "JOIN", "INNER", "LEFT", "RIGHT", "ON", "AS"
   })
   ```

3. **语法分析器扩展**:
   ```python
   def parse_from_clause(self):
       """解析 FROM 子句，支持 JOIN"""
       from_table = self.parse_table_reference()
       joins = []
       
       while self.match_keyword("INNER", "LEFT", "RIGHT", "JOIN"):
           join = self.parse_join_clause()
           joins.append(join)
       
       return from_table, joins
   
   def parse_join_clause(self):
       """解析 JOIN 子句"""
       join_type = self.parse_join_type()
       self.consume_keyword("JOIN")
       right_table = self.parse_table_reference()
       self.consume_keyword("ON")
       on_condition = self.parse_expression()
       return JoinExpression(None, right_table, join_type, on_condition)
   ```

4. **执行计划扩展**:
   ```python
   class HashJoinPlan(ExecutionPlan):
       """哈希连接算子"""
       def __init__(self, left_plan, right_plan, join_condition, join_type):
           super().__init__("HashJoin")
           self.add_child(left_plan)
           self.add_child(right_plan)
           self.properties.update({
               "condition": join_condition,
               "join_type": join_type
           })
   ```

#### 1.2 ORDER BY 排序支持

**新增语法**：
```sql
SELECT * FROM students ORDER BY age DESC, name ASC;
SELECT name, score FROM students ORDER BY score DESC LIMIT 10;
```

**实现计划**：
1. **AST 节点**:
   ```python
   class OrderByClause(ASTNode):
       def __init__(self, expressions):
           self.expressions = expressions  # [(expression, direction), ...]
   
   class SortExpression(ASTNode):
       def __init__(self, expression, direction='ASC'):
           self.expression = expression
           self.direction = direction  # 'ASC' 或 'DESC'
   ```

2. **语法解析**:
   ```python
   def parse_order_by(self):
       """解析 ORDER BY 子句"""
       self.consume_keyword("ORDER")
       self.consume_keyword("BY")
       
       sort_expressions = []
       while True:
           expr = self.parse_expression()
           direction = 'ASC'
           if self.match_keyword("ASC", "DESC"):
               direction = self.current_token.value
               self.advance()
           
           sort_expressions.append(SortExpression(expr, direction))
           
           if not self.match(TokenType.COMMA):
               break
           self.advance()
       
       return OrderByClause(sort_expressions)
   ```

3. **执行计划**:
   ```python
   class SortPlan(ExecutionPlan):
       """排序算子"""
       def __init__(self, child_plan, sort_keys):
           super().__init__("Sort")
           self.add_child(child_plan)
           self.properties["sort_keys"] = sort_keys
   ```

#### 1.3 GROUP BY 和聚合函数

**新增语法**：
```sql
SELECT age, COUNT(*), AVG(score) 
FROM students 
GROUP BY age 
HAVING COUNT(*) > 1;

SELECT dept_id, MAX(salary), MIN(salary)
FROM employees
GROUP BY dept_id;
```

**实现计划**：
1. **AST 节点**:
   ```python
   class AggregateFunctionCall(Expression):
       def __init__(self, function_name, arguments):
           self.function_name = function_name  # COUNT, SUM, AVG, etc.
           self.arguments = arguments
   
   class GroupByClause(ASTNode):
       def __init__(self, grouping_expressions):
           self.grouping_expressions = grouping_expressions
   
   class HavingClause(ASTNode):
       def __init__(self, condition):
           self.condition = condition
   ```

2. **执行计划**:
   ```python
   class GroupAggregatePlan(ExecutionPlan):
       """分组聚合算子"""
       def __init__(self, child_plan, group_keys, aggregates):
           super().__init__("GroupAggregate")
           self.add_child(child_plan)
           self.properties.update({
               "group_keys": group_keys,
               "aggregates": aggregates
           })
   ```

**复杂度评估**: 中等 (2-3周)
- 新增约10个AST节点类
- 扩展词法关键字15个
- 语法分析器新增8个解析方法
- 执行器新增4个算子实现

---

### 方向2: 查询优化 (Query Optimization)

#### 2.1 基于规则的优化器 (RBO)

**优化规则设计**：

1. **谓词下推 (Predicate Pushdown)**:
   ```sql
   -- 优化前
   SELECT * FROM (SELECT * FROM students JOIN scores ON students.id = scores.id) 
   WHERE students.age > 20;
   
   -- 优化后  
   SELECT * FROM (SELECT * FROM students WHERE age > 20) s 
   JOIN scores ON s.id = scores.id;
   ```

2. **投影下推 (Projection Pushdown)**:
   ```sql
   -- 优化前
   SELECT name FROM (SELECT * FROM students WHERE age > 18);
   
   -- 优化后
   SELECT name FROM (SELECT id, name FROM students WHERE age > 18);
   ```

3. **连接重排序 (Join Reordering)**:
   ```sql
   -- 基于表大小统计，优化连接顺序
   -- 小表驱动大表
   ```

**实现架构**：
```python
class QueryOptimizer:
    """查询优化器"""
    
    def __init__(self, catalog, statistics):
        self.catalog = catalog
        self.statistics = statistics
        self.rules = [
            PredicatePushdownRule(),
            ProjectionPushdownRule(), 
            JoinReorderingRule(),
            ConstantFoldingRule()
        ]
    
    def optimize(self, plan: ExecutionPlan) -> ExecutionPlan:
        """优化执行计划"""
        optimized_plan = plan
        
        for rule in self.rules:
            if rule.can_apply(optimized_plan):
                optimized_plan = rule.apply(optimized_plan)
                
        return optimized_plan

class OptimizationRule(ABC):
    """优化规则基类"""
    
    @abstractmethod
    def can_apply(self, plan: ExecutionPlan) -> bool:
        """检查规则是否适用"""
        pass
    
    @abstractmethod  
    def apply(self, plan: ExecutionPlan) -> ExecutionPlan:
        """应用优化规则"""
        pass

class PredicatePushdownRule(OptimizationRule):
    """谓词下推规则"""
    
    def can_apply(self, plan: ExecutionPlan) -> bool:
        # 检查是否有FILTER算子可以下推
        return (plan.operator_type == "Filter" and 
                len(plan.children) == 1 and
                plan.children[0].operator_type in ["HashJoin", "Project"])
    
    def apply(self, plan: ExecutionPlan) -> ExecutionPlan:
        # 实现谓词下推逻辑
        pass
```

#### 2.2 成本模型和统计信息

**统计信息收集**：
```python
class TableStatistics:
    """表统计信息"""
    
    def __init__(self, table_name):
        self.table_name = table_name
        self.row_count = 0
        self.page_count = 0
        self.column_stats = {}  # 列级统计
    
    def update_stats(self, storage_engine):
        """更新统计信息"""
        self.row_count = storage_engine.get_row_count(self.table_name)
        self.page_count = storage_engine.get_page_count(self.table_name)

class ColumnStatistics:
    """列统计信息"""
    
    def __init__(self, column_name):
        self.column_name = column_name
        self.distinct_count = 0
        self.null_count = 0
        self.min_value = None
        self.max_value = None
        self.histogram = None

class CostModel:
    """成本模型"""
    
    def __init__(self, statistics):
        self.statistics = statistics
        # 成本参数
        self.seq_scan_cost = 1.0
        self.index_scan_cost = 0.1
        self.hash_join_cost = 2.0
        self.sort_cost = 3.0
    
    def estimate_cost(self, plan: ExecutionPlan) -> float:
        """估算执行计划成本"""
        if plan.operator_type == "SeqScan":
            table_stats = self.statistics.get_table_stats(
                plan.properties["table_name"]
            )
            return table_stats.row_count * self.seq_scan_cost
        
        elif plan.operator_type == "HashJoin":
            left_cost = self.estimate_cost(plan.children[0])
            right_cost = self.estimate_cost(plan.children[1])
            join_cost = left_cost * right_cost * 0.1  # 选择性估算
            return left_cost + right_cost + join_cost
        
        # ... 其他算子成本估算
```

**复杂度评估**: 高 (4-5周)
- 需要深入理解查询优化理论
- 统计信息收集和维护机制
- 成本模型参数调优
- 多种优化规则实现

---

### 方向3: 错误诊断增强 (Error Diagnosis Enhancement)

#### 3.1 智能纠错系统

**拼写纠错**：
```python
class SpellChecker:
    """SQL拼写检查器"""
    
    def __init__(self):
        self.keyword_suggestions = {
            'CREATE', 'SELECT', 'INSERT', 'UPDATE', 'DELETE',
            'FROM', 'WHERE', 'JOIN', 'ORDER', 'GROUP', 'HAVING'
        }
        self.function_suggestions = {
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DISTINCT'
        }
    
    def suggest_keyword(self, wrong_keyword: str) -> List[str]:
        """基于编辑距离的关键字建议"""
        suggestions = []
        for keyword in self.keyword_suggestions:
            distance = self.edit_distance(wrong_keyword.upper(), keyword)
            if distance <= 2:  # 允许最多2个字符差异
                suggestions.append((keyword, distance))
        
        return [kw for kw, _ in sorted(suggestions, key=lambda x: x[1])]
    
    def edit_distance(self, s1: str, s2: str) -> int:
        """计算编辑距离"""
        m, n = len(s1), len(s2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        
        for i in range(m + 1):
            dp[i][0] = i
        for j in range(n + 1):
            dp[0][j] = j
        
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if s1[i-1] == s2[j-1]:
                    dp[i][j] = dp[i-1][j-1]
                else:
                    dp[i][j] = min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]) + 1
        
        return dp[m][n]
```

**语法修复建议**：
```python
class SyntaxRepairer:
    """语法修复建议器"""
    
    def __init__(self):
        self.common_patterns = {
            # 常见语法错误模式
            "missing_semicolon": r".*[^;]$",
            "missing_quotes": r".*=\s*\w+\s*(?:WHERE|ORDER|GROUP|$)",
            "missing_parentheses": r"CREATE\s+TABLE\s+\w+\s+[^(]",
        }
    
    def suggest_fix(self, sql: str, error: ParseError) -> List[str]:
        """基于错误类型提供修复建议"""
        suggestions = []
        
        if "Expected ';'" in error.message:
            suggestions.append("在语句末尾添加分号 ';'")
        
        if "Expected 'FROM'" in error.message:
            suggestions.append("SELECT 语句需要 FROM 子句")
        
        if "Unexpected character" in error.message:
            char = error.token.value
            if char in ['"', "'"]:
                suggestions.append("检查字符串引号是否正确配对")
        
        return suggestions
```

#### 3.2 上下文感知错误提示

**智能错误消息**：

```python
class ContextAwareErrorReporter:
    """上下文感知错误报告器"""

    def __init__(self, catalog):
        self.catalog = catalog

    def enhance_error(self, error: SemanticError, sql: str) -> EnhancedError:
        """增强错误信息"""
        enhanced = EnhancedError(error)

        if error.error_type == "TABLE_NOT_EXISTS":
            # 建议相似的表名
            table_name = self.extract_table_name(error.message)
            similar_tables = self.find_similar_tables(table_name)
            if similar_tables:
                enhanced.suggestions.append(f"您是否想要使用: {', '.join(similar_tables)}?")

        elif error.error_type == "COLUMN_NOT_EXISTS":
            # 建议相似的列名
            column_name = self.extract_column_name(error.message)
            table_name = self.extract_table_name(error.message)
            similar_columns = self.find_similar_columns(table_name, column_name)
            if similar_columns:
                enhanced.suggestions.append(f"可用的列: {', '.join(similar_columns)}")

        return enhanced

    def find_similar_tables(self, table_name: str) -> List[str]:
        """查找相似的表名"""
        all_tables = self.catalog.get_tables()
        similar = []

        for table in all_tables:
            if self.similarity_score(table_name.upper(), table.upper()) > 0.6:
                similar.append(table)

        return similar


class EnhancedError:
    """增强的错误信息"""

    def __init__(self, original_error):
        self.original_error = original_error
        self.suggestions = []
        self.quick_fixes = []
        self.related_docs = []

    def format_message(self) -> str:
        """格式化错误消息"""
        message = str(self.original_error)

        if self.suggestions:
            message += "\n建议:"
            for suggestion in self.suggestions:
                message += f"\n  - {suggestion}"

        if self.quick_fixes:
            message += "\n快速修复:"
            for fix in self.quick_fixes:
                message += f"\n  - {fix}"

        return message
```

#### 3.3 错误恢复机制

**部分解析恢复**：
```python
class RecoveringParser(SQLParser):
    """支持错误恢复的解析器"""
    
    def __init__(self, tokens: List[Token]):
        super().__init__(tokens)
        self.errors = []
        self.recovery_tokens = {
            TokenType.SEMICOLON,
            TokenType.KEYWORD  # SELECT, INSERT, etc.
        }
    
    def parse_with_recovery(self) -> Tuple[SQLProgram, List[ParseError]]:
        """带错误恢复的解析"""
        statements = []
        
        while not self.is_at_end():
            try:
                stmt = self.parse_statement()
                if stmt:
                    statements.append(stmt)
            except ParseError as e:
                self.errors.append(e)
                self.recover_to_next_statement()
        
        return SQLProgram(statements), self.errors
    
    def recover_to_next_statement(self):
        """恢复到下一个语句"""
        while not self.is_at_end():
            if self.current_token.type in self.recovery_tokens:
                if self.current_token.type == TokenType.SEMICOLON:
                    self.advance()  # 跳过分号
                break
            self.advance()
```

**复杂度评估**: 中等 (3-4周)
- 错误检测和分类系统
- 拼写纠错算法实现
- 上下文感知逻辑
- 恢复机制设计

---

## 📊 优先级评估和实施建议

### 实现难度评估

| 扩展方向 | 技术复杂度 | 开发周期 | 价值收益 | 维护成本 | 推荐优先级 |
|----------|------------|----------|----------|----------|------------|
| **语法扩展** | ⭐⭐⭐ | 2-3周 | ⭐⭐⭐⭐⭐ | ⭐⭐ | **高 🔴** |
| **查询优化** | ⭐⭐⭐⭐⭐ | 4-5周 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | 中 🟡 |
| **错误诊断** | ⭐⭐⭐ | 3-4周 | ⭐⭐⭐ | ⭐⭐ | 中 🟡 |

### 推荐实施顺序

#### 阶段1: 语法扩展 (优先推荐)
**理由**：
- ✅ 技术风险低，基于现有架构扩展
- ✅ 用户价值高，显著提升数据库功能完整性
- ✅ 实现周期短，能快速看到成果
- ✅ 为后续优化奠定基础

**具体顺序**：
1. **ORDER BY** (1周) - 单表排序，实现简单
2. **基础 JOIN** (1.5周) - INNER JOIN，核心功能
3. **GROUP BY + 聚合** (1周) - 分析查询基础

#### 阶段2: 错误诊断增强  
**理由**：
- 提升用户体验，降低学习门槛
- 实现相对独立，不影响核心功能
- 有助于调试和教学

#### 阶段3: 查询优化
**理由**：  
- 技术挑战最大，需要前面功能完备后实施
- 收益主要体现在性能，非功能性需求
- 需要完整的测试用例和基准测试

### 最小可行实现 (MVP)

如果时间有限，建议优先实现：
1. **ORDER BY 基础排序** - 最高性价比
2. **INNER JOIN 双表连接** - 关系数据库核心特性  
3. **基础聚合函数** (COUNT, SUM) - 分析查询入门

这三个功能可以在 **2-3周** 内完成，显著提升 MiniDB 的实用性。