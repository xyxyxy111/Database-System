# MiniDB SQL编译器 - 运行调试指南

## 1. 环境准备

确保你的Python环境已经配置好：
```bash
# 检查Python版本 (需要3.7+)
python --version

# 安装依赖 (如果需要)
pip install -r requirements.txt
```

## 2. 快速开始

### 2.1 运行完整测试
```bash
# 进入项目目录
cd e:\MiniDB

# 运行SQL编译器测试程序
python test_sql_compiler.py
```

这将执行完整的编译器测试，包括：
- 词法分析测试
- 语法分析测试  
- 语义分析测试
- 执行计划生成测试
- 错误处理测试

### 2.2 交互模式使用
```bash
# 启动交互模式
python main.py
```

然后你可以输入SQL语句进行测试：
```sql
MiniDB> CREATE TABLE student(id INT, name VARCHAR(50), age INT);
MiniDB> INSERT INTO student VALUES (1, 'Alice', 20);
MiniDB> SELECT id, name FROM student WHERE age > 18;
MiniDB> show tables
MiniDB> help
MiniDB> exit
```

### 2.3 批处理模式使用
```bash
# 处理SQL文件
python main.py tests/test_sql_files/basic_test.sql
python main.py tests/test_sql_files/error_test.sql
```

## 3. 模块独立测试

### 3.1 词法分析器测试

```python
from compiler import SQLLexer

sql = "CREATE TABLE test(id INT, name VARCHAR(50));"
lexer = SQLLexer(sql)
tokens = lexer.tokenize()

for token in tokens:
    if token.type.name != 'EOF':
        print(f"{token.type.name}: {token.value}")
```

### 3.2 语法分析器测试

```python
from compiler import SQLLexer, SQLParser

sql = "SELECT id, name FROM student WHERE age > 18;"
lexer = SQLLexer(sql)
tokens = lexer.tokenize()

parser = SQLParser(tokens)
ast = parser.parse()
print(ast.statements[0])
```

### 3.3 语义分析器测试

```python
from compiler import SQLLexer, SQLParser, SemanticAnalyzer

sql = """
CREATE TABLE student(id INT, name VARCHAR(50));
SELECT id, grade FROM student;  -- grade列不存在，会报错
"""

lexer = SQLLexer(sql)
parser = SQLParser(lexer.tokenize())
ast = parser.parse()

analyzer = SemanticAnalyzer()
success, errors = analyzer.analyze(ast)

if not success:
    for error in errors:
        print(error)
```

## 4. 调试技巧

### 4.1 开启详细输出
修改main.py中的`verbose=True`来查看详细的编译过程：
```python
compiler.print_compilation_result(result, verbose=True)
```

### 4.2 查看Token序列
```python
lexer = SQLLexer(sql)
lexer.print_tokens()  # 打印所有Token
```

### 4.3 查看AST结构
```python
print("AST结构:")
for stmt in ast.statements:
    print(f"  {stmt}")
```

### 4.4 查看执行计划
```python
for plan in plans:
    print("树形结构:")
    print(plan.to_tree_string())
    
    print("JSON格式:")
    print(plan.to_json())
```

## 5. 常见问题及解决方案

### 5.1 导入模块失败
确保你在项目根目录下运行程序：
```bash
cd e:\MiniDB
python main.py
```

### 5.2 语法错误
检查SQL语句的语法，常见错误：
- 缺少分号
- 括号不匹配
- 关键字拼写错误
- 字符串未闭合

### 5.3 语义错误
检查以下问题：
- 表名是否存在
- 列名是否存在  
- 数据类型是否匹配
- INSERT值的数量是否正确

## 6. 支持的SQL语法

### 6.1 CREATE TABLE
```sql
CREATE TABLE table_name(
    column1 data_type,
    column2 data_type,
    ...
);
```

### 6.2 INSERT
```sql
-- 指定列名
INSERT INTO table_name(col1, col2) VALUES (val1, val2);

-- 不指定列名（按表定义顺序）
INSERT INTO table_name VALUES (val1, val2, val3);
```

### 6.3 SELECT
```sql
-- 基本查询
SELECT col1, col2 FROM table_name;

-- 带条件查询
SELECT col1, col2 FROM table_name WHERE condition;
```

### 6.4 DELETE  
```sql
-- 删除所有记录
DELETE FROM table_name;

-- 带条件删除
DELETE FROM table_name WHERE condition;
```

### 6.5 支持的数据类型
- `INT` / `INTEGER`: 整数类型
- `VARCHAR(n)`: 可变长字符串，n为最大长度
- `CHAR(n)`: 固定长字符串，n为长度

### 6.6 支持的运算符
- 比较运算符: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- 逻辑运算符: `AND`, `OR`, `NOT` (暂未完全实现)

## 7. 下一步开发

SQL编译器模块基本完成，接下来可以：

1. **完善错误处理**: 添加更详细的错误信息和位置提示
2. **扩展语法支持**: 添加UPDATE、JOIN、ORDER BY等语句  
3. **优化执行计划**: 实现查询优化算法
4. **集成存储系统**: 将编译器与存储引擎连接
5. **添加更多测试**: 增加边界情况和压力测试

## 8. 代码结构说明

```
sql_compiler/
├── __init__.py          # 模块导出
├── ast_nodes.py         # AST节点定义  
├── catalog.py           # 模式目录管理
├── lexer.py            # 词法分析器
├── parser.py           # 语法分析器  
├── semantic_analyzer.py # 语义分析器
└── plan_generator.py    # 执行计划生成器
```

每个模块都有独立的测试用例，可以单独运行和调试。
