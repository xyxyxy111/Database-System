# MiniDB - 小型数据库系统

这是一个教学用的小型数据库系统，按照分层架构实现：

## 项目结构

```
MiniDB/
├── README.md                   # 项目说明文档
├── requirements.txt            # Python依赖包
├── main.py                     # 主程序入口
├── tests/                      # 测试文件目录
│   ├── test_lexer.py          # 词法分析测试
│   ├── test_parser.py         # 语法分析测试
│   ├── test_semantic.py       # 语义分析测试
│   ├── test_storage.py        # 存储系统测试
│   ├── test_database.py       # 数据库系统测试
│   └── test_sql_files/        # SQL测试文件
├── sql_compiler/               # SQL编译器模块
│   ├── __init__.py
│   ├── lexer.py               # 词法分析器
│   ├── parser.py              # 语法分析器
│   ├── semantic_analyzer.py   # 语义分析器
│   ├── plan_generator.py      # 执行计划生成器
│   ├── ast_nodes.py          # AST节点定义
│   └── catalog.py            # 模式目录
├── storage/                   # 存储系统模块
│   ├── __init__.py
│   ├── page_manager.py       # 页面管理器
│   ├── buffer_manager.py     # 缓存管理器
│   ├── disk_manager.py       # 磁盘管理器
│   └── storage_engine.py     # 存储引擎
└── database/                  # 数据库引擎模块
    ├── __init__.py
    ├── database_catalog.py    # 数据库目录管理
    ├── query_executor.py      # 查询执行器
    └── database_engine.py     # 数据库引擎核心
```

## 系统架构

### 1. SQL编译器 (sql_compiler/)
- **词法分析器** (lexer.py): 将SQL语句分解为Token序列
- **语法分析器** (parser.py): 构建抽象语法树(AST)
- **语义分析器** (semantic_analyzer.py): 进行语义检查和类型验证
- **执行计划生成器** (plan_generator.py): 生成逻辑执行计划

### 2. 存储系统 (storage/)
- **页面管理器** (page_manager.py): 管理4KB固定大小的数据页
- **缓存管理器** (buffer_manager.py): 实现LRU/FIFO页面缓存
- **磁盘管理器** (disk_manager.py): 处理磁盘I/O操作
- **存储引擎** (storage_engine.py): 提供统一的存储接口

### 3. 数据库引擎 (database/)
- **数据库目录** (database_catalog.py): 管理表结构和元数据
- **查询执行器** (query_executor.py): 执行SQL查询计划
- **数据库引擎** (database_engine.py): 整合SQL编译器和存储引擎的主要接口

## 支持的SQL语句

- ✅ `CREATE TABLE table_name(col1 type1, col2 type2, ...)`
- ✅ `INSERT INTO table_name VALUES (val1, val2, ...)`
- ✅ `SELECT col1, col2, ... FROM table_name`
- 🚧 `DELETE FROM table_name [WHERE condition]` (计划中)
- 🚧 `UPDATE table_name SET col1=val1 WHERE condition` (计划中)

## 支持的数据类型

- ✅ `INT`: 整型
- ✅ `VARCHAR(n)`: 变长字符串（需指定长度）
- 🚧 `CHAR(n)`: 固定长度字符串 (计划中)

## 运行方式

```bash
# 安装依赖
pip install -r requirements.txt

# 运行主程序
python main.py

# 运行数据库引擎测试
python test_simple_database.py

# 运行其他测试
python test_sql_compiler.py      # SQL编译器测试
python test_storage_engine.py    # 存储引擎测试
```

## 开发进度

- ✅ **SQL编译器** (已完成)
  - ✅ 词法分析器 - 支持SQL关键字和语法元素
  - ✅ 语法分析器 - 构建完整AST
  - ✅ 语义分析器 - 类型检查和表验证
  - ✅ 执行计划生成器 - 生成执行计划树
- ✅ **存储系统** (已完成)
  - ✅ 页面管理器 - 4KB页面管理
  - ✅ 缓存管理器 - LRU缓存策略
  - ✅ 磁盘管理器 - 文件I/O操作
  - ✅ 存储引擎 - 统一存储接口
- ✅ **数据库引擎** (已完成)
  - ✅ 数据库目录 - 元数据管理
  - ✅ 查询执行器 - SQL执行管道
  - ✅ 数据库引擎 - 系统集成

## 技术特点

1. **模块化设计**: 每个组件职责单一，便于测试和维护
2. **分层架构**: SQL编译器 → 存储系统 → 数据库系统
3. **教学友好**: 代码结构清晰，注释详细，便于理解
4. **完整功能**: 支持完整的DDL和DML操作
5. **性能考虑**: 实现了页面缓存和查询优化
