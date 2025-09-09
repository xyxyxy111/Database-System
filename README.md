# 简易数据库系统（SQL 编译器 + 页式存储 + 执行引擎）

本项目用于课程实训与教学，贯通编译原理、操作系统与数据库三大模块，实现一个可持久化的简化数据库原型。它支持 CREATE TABLE、INSERT、SELECT（含 WHERE 比较）、DELETE 四类核心语句，采用固定 4KB 页式存储与 LRU 缓冲，内置系统目录持久化表结构。

## 一、实训目标
- SQL 编译器：掌握词法/语法/语义/计划生成；理解 SQL → 执行计划流程。
- 操作系统实践：页式存储、文件页 I/O、缓冲管理与替换策略。
- 数据库系统：执行引擎、存储引擎与查询语言的集成，实现核心 CRUD。

## 二、功能特性
- 语句支持：
  - DDL: `CREATE TABLE <name>(col TYPE, ...)`（TYPE ∈ {INT, VARCHAR}）
  - DML: `INSERT INTO <name>(cols...) VALUES (values...)`
  - 查询: `SELECT col_list | * FROM <name> [WHERE <col> <op> <value>]`
  - 删除: `DELETE FROM <name> [WHERE <col> <op> <value>]`
  - WHERE 比较运算符：`=, !=, <>, >, <, >=, <=`
- 编译器：
  - 词法：Token 含行、列位置；非法字符抛 `LexError`
  - 语法：递归下降；错误提示包含出错位置与期望符号
  - 语义：表/列存在性、类型一致性（INT/VARCHAR）、列数/列序检查；抛 `SemanticError`
  - 计划：`CreateTable/Insert/SeqScan/Filter/Project/Delete`
- 存储与缓存：
  - 页：4KB，pickle 序列化；append 插入、顺序扫描
  - 磁盘：页分配/读写，`free_page` 占位清零
  - 缓冲：LRU，`hits/misses/evictions` 统计，逐出日志
- 系统目录：
  - 特殊表 `__catalog__` 持久化每张表的列名与类型，启动自动加载

## 三、目录结构
- `compiler/`
  - `lexer.py`：词法分析；Token = `(type, lexeme, line, col)`
  - `parser.py`：AST 与解析（CREATE/INSERT/SELECT/DELETE）
  - `sematic_analyzer.py`：语义检查与 `Analyzed(kind,payload)`
  - `planner.py`：语义结果 → 物理算子树
- `execution/`
  - `operators.py`：`SeqScan/Filter/Project/Insert/CreateTable/Delete`
  - `executor.py`：拉模型执行器（`open/next/close` 循环）
  - `sytem_catalog.py`：系统目录、共享缓冲、`__catalog__` 持久化
- `storage/`
  - `page.py`：页对象与序列化
  - `disk_manager.py`：页分配/读写/清零释放
  - `buffer_manager.py`：LRU，统计与日志
  - `table.py`：堆表，`insert/scan/delete`
- `main.py`：CLI，支持 `--debug-pipeline`、`--stats` 与 `@file.sql`

## 四、快速开始
环境：Python 3.9+（推荐 3.10+）

1) 建表
```bash
python main.py .\my.db "CREATE TABLE student(id INT, name VARCHAR, age INT);"
```
2) 插入
```bash
python main.py .\my.db "INSERT INTO student(id,name,age) VALUES (1,'Alice',20);"
python main.py .\my.db "INSERT INTO student(id,name,age) VALUES (2,'Bob',17);"
```
3) 查询
```bash
python main.py .\my.db "SELECT id,name FROM student WHERE age >= 18;"
```
4) 删除
```bash
python main.py .\my.db "DELETE FROM student WHERE id = 1;"
```
5) 文件执行多语句
```bash
# script.sql
# CREATE TABLE student(id INT, name VARCHAR, age INT);
# INSERT INTO student(id,name,age) VALUES (1,'Alice',20);
# SELECT * FROM student;
python main.py .\my.db @script.sql
```

## 五、调试与统计
- 打印编译流水线：
```bash
python main.py .\my.db "SELECT * FROM student;" --debug-pipeline
# 输出 [Tokens]/[AST]/[Analyzed]/[PlanRoot]
```
- 打印缓冲统计：
```bash
python main.py .\my.db "SELECT * FROM student;" --stats
# 输出 [BufferStats] hits=..., misses=..., evictions=...
```

## 六、设计说明
- 词法（`lexer.py`）：正则驱动，关键字表 + 操作符优先匹配（多字符 > 单字符）
- 语法（`parser.py`）：递归下降，`parse_many()` 支持多语句与分号
- 语义（`sematic_analyzer.py`）：使用 `SystemCatalog` 获取 schema 并检查；不做隐式类型转换
- 计划（`planner.py`）：WHERE 使用 `make_predicate` 生成布尔函数并套在 `Filter` 上
- 存储（`page.py`/`disk_manager.py`/`table.py`）：行以 `dict` 存储；删除为页内过滤重写
- 缓冲（`buffer_manager.py`）：OrderedDict 实现 LRU；逐出打印日志；`stats()` 返回三项计数
- 目录（`sytem_catalog.py`）：`__catalog__` 存储 `(table, columns)`，columns 为 `(name,type)` 列表

## 七、正确性与测试建议
- 创建重复表：第二次 `CREATE TABLE` 报错
- 大量插入：检查数据分页与顺序扫描
- 条件查询：覆盖 `=, !=, <>, >, <, >=, <=`
- 删除后查询：被删记录不可见
- 重启持久性：进程退出后再次查询仍能读到数据与目录
- 错误用例：缺分号、列名错误、类型不匹配、字符串未闭合、非法字符

## 八、约束与可扩展
- 当前未实现：事务/并发/崩溃恢复、页回收与空闲列表、VARCHAR 长度限制与类型转换
- 可扩展：UPDATE、JOIN、ORDER BY、GROUP BY、索引、标记删除+重组、统计信息与优化、计划解释 explain()

## 九、实现要点清单（对照实训要求）
- [x] 词法：Token 含行列位置，错误定位
- [x] 语法：CREATE/INSERT/SELECT/DELETE，位置化错误提示
- [x] 语义：存在性、类型一致、列数/列序检查；目录维护
- [x] 计划：CreateTable/Insert/SeqScan/Filter/Project/Delete
- [x] 存储：4KB 页、分配/读写、表到页映射
- [x] 缓存：LRU、命中统计、逐出日志
- [x] 系统目录：特殊表持久化 schema 并启动加载
- [x] CLI：多语句、@file.sql、`--debug-pipeline`、`--stats`
