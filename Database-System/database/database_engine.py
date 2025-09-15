"""数据库引擎 - MiniDB的核心组件，整合所有子系统"""

import os
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

from storage import StorageEngine
from .database_catalog import DatabaseCatalog
from .query_executor import QueryExecutor, QueryResult

# 导入新的优化和诊断模块
try:
    from compiler.error_diagnostics import ErrorDiagnostics
    from compiler.performance_analyzer import PerformanceAnalyzer
    from compiler.query_optimizer import QueryOptimizer

    ADVANCED_FEATURES_AVAILABLE = True
except ImportError:
    ADVANCED_FEATURES_AVAILABLE = False
    print("警告: 高级功能模块未加载，某些优化功能可能不可用")


class DatabaseEngine:
    """MiniDB数据库引擎"""

    def __init__(self, db_path: str, buffer_size: int = 64):
        """
        初始化数据库引擎

        Args:
            db_path: 数据库文件路径
            buffer_size: 缓存池大小
        """
        self.db_path = Path(db_path)
        self.db_name = self.db_path.stem

        # 初始化存储引擎
        self.storage_engine = StorageEngine(str(self.db_path), buffer_size)

        # 初始化目录管理器
        self.catalog = DatabaseCatalog()

        # 初始化查询执行器
        self.query_executor = QueryExecutor(self.storage_engine, self.catalog)

        # 初始化高级功能组件
        if ADVANCED_FEATURES_AVAILABLE:
            self.query_optimizer = QueryOptimizer()
            self.error_diagnostics = ErrorDiagnostics()
            self.performance_analyzer = PerformanceAnalyzer()
        else:
            self.query_optimizer = None
            self.error_diagnostics = None
            self.performance_analyzer = None

        # 数据库状态
        self.is_open = True
        self.created_at = datetime.now()

        # 加载或创建目录
        self._initialize_catalog()

        print(f"MiniDB 数据库引擎已启动: {self.db_name}")

    def _initialize_catalog(self):
        """初始化或加载数据库目录"""
        try:
            # 尝试从存储引擎加载现有表的信息
            existing_tables = self.storage_engine.get_all_tables()

            for table_name in existing_tables:
                table_info = self.storage_engine.get_table_info(table_name)
                if table_info:
                    # 从存储引擎重建目录信息
                    # 注意：这是简化实现，实际应该从专门的元数据表加载
                    from .database_catalog import TableDefinition, ColumnDefinition

                    columns = []
                    for i, (col_name, col_type) in enumerate(
                        zip(table_info.column_names, table_info.column_types)
                    ):
                        column = ColumnDefinition(
                            name=col_name,
                            data_type=col_type,
                            primary_key=(i == 0),  # 简化：假设第一列是主键
                        )
                        columns.append(column)

                    table_def = TableDefinition(
                        name=table_name,
                        columns=columns,
                        created_at=datetime.now().isoformat(),
                        record_count=table_info.record_count,
                    )

                    self.catalog.create_table(table_def)

            print(f"加载了 {len(existing_tables)} 个现有表")

        except Exception as e:
            print(f"Warning: 初始化目录时出错: {e}")
            # 如果加载失败，确保至少有一个空的catalog
            pass

    def execute_sql(self, sql: str) -> QueryResult:
        """
        执行SQL语句（带查询优化和错误诊断）

        Args:
            sql: SQL语句字符串

        Returns:
            QueryResult: 查询结果
        """
        if not self.is_open:
            return QueryResult(False, "数据库引擎已关闭")

        if not sql or not sql.strip():
            return QueryResult(False, "SQL语句不能为空")

        try:
            # 开始性能分析
            query_start_time = None
            if self.performance_analyzer:
                query_start_time = self.performance_analyzer.start_query_timing()

            # 如果有查询优化器，先尝试优化查询
            optimized_sql = sql.strip()
            optimization_report = None

            if self.query_optimizer:
                try:
                    # 解析SQL获取AST用于优化
                    from compiler.lexer import SQLLexer
                    from compiler.parser import SQLParser

                    lexer = SQLLexer(sql.strip())
                    tokens = lexer.tokenize()
                    parser = SQLParser(tokens)
                    ast = parser.parse()

                    # 优化查询
                    optimization_result = self.query_optimizer.optimize_query(ast)

                    # 处理优化结果
                    if isinstance(optimization_result, tuple):
                        _, optimization_report = optimization_result
                    else:
                        optimization_report = None

                    # 如果有优化，显示优化信息
                    if optimization_report and optimization_report.get(
                        "optimizations_applied"
                    ):
                        print("🔧 查询优化:")
                        for opt in optimization_report["optimizations_applied"]:
                            print(f"  - {opt}")

                except Exception as opt_error:
                    # 优化失败不影响正常执行
                    print(f"查询优化警告: {opt_error}")

            # 执行查询
            result = self.query_executor.execute_sql(optimized_sql)

            # 结束性能分析
            if self.performance_analyzer and query_start_time is not None:
                rows_affected = len(result.data) if result.data else 0
                stats = self.performance_analyzer.end_query_timing(
                    query_start_time,
                    optimized_sql,
                    rows_affected=rows_affected,
                    optimization_applied=bool(optimization_report),
                )

                # 显示性能信息（如果查询较慢）
                if stats.execution_time > 0.1:  # 超过100ms显示性能信息
                    print(f"⏱️  执行时间: {stats.execution_time:.3f}秒")

            # 记录执行日志
            status = "SUCCESS" if result.success else "ERROR"
            sql_preview = sql.strip()[:50]
            if len(sql.strip()) > 50:
                sql_preview += "..."
            print(f"[{status}] SQL: {sql_preview}")

            return result

        except Exception as e:
            # 如果有错误诊断器，提供增强的错误信息
            error_message = f"执行SQL时发生错误: {str(e)}"

            if self.error_diagnostics:
                try:
                    enhanced_error = self.error_diagnostics.enhance_error_message(
                        str(e), sql.strip()
                    )
                    error_message = enhanced_error
                except Exception:
                    # 错误诊断失败，使用原始错误信息
                    pass

            return QueryResult(False, error_message)

    def get_optimization_report(self) -> Optional[Dict[str, Any]]:
        """获取查询优化报告"""
        if self.query_optimizer:
            return self.query_optimizer.get_optimization_report()
        return None

    def get_performance_report(self) -> Optional[Dict[str, Any]]:
        """获取性能分析报告"""
        if self.performance_analyzer:
            return self.performance_analyzer.get_performance_report()
        return None

    def execute_batch(self, sql_statements: List[str]) -> List[QueryResult]:
        """
        批量执行SQL语句

        Args:
            sql_statements: SQL语句列表

        Returns:
            List[QueryResult]: 结果列表
        """
        results = []

        for sql in sql_statements:
            result = self.execute_sql(sql)
            results.append(result)

            # 如果出现错误，可以选择是否继续执行
            if not result.success:
                print(f"批量执行中止: {result.message}")
                break

        return results

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        获取表信息

        Args:
            table_name: 表名

        Returns:
            表信息字典或None
        """
        table_def = self.catalog.get_table(table_name)
        storage_info = self.storage_engine.get_table_info(table_name)

        if table_def and storage_info:
            return {
                "name": table_def.name,
                "columns": [
                    {
                        "name": col.name,
                        "type": col.data_type,
                        "nullable": col.nullable,
                        "primary_key": col.primary_key,
                        "unique": col.unique,
                        "default": col.default_value,
                    }
                    for col in table_def.columns
                ],
                "record_count": table_def.record_count,
                "created_at": table_def.created_at,
                "page_count": len(storage_info.page_ids),
                "storage_pages": storage_info.page_ids,
            }

        return None

    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        return self.catalog.get_all_tables()

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return self.catalog.table_exists(table_name)

    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库信息"""
        catalog_stats = self.catalog.get_statistics()
        storage_stats = self.storage_engine.get_statistics()

        return {
            "name": self.db_name,
            "path": str(self.db_path),
            "created_at": self.created_at.isoformat(),
            "is_open": self.is_open,
            "tables": catalog_stats["table_count"],
            "total_records": catalog_stats["total_records"],
            "total_columns": catalog_stats["total_columns"],
            "indexes": catalog_stats["index_count"],
            "database_size": storage_stats["database_size"],
            "buffer_stats": storage_stats["buffer_stats"],
        }

    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计"""
        storage_stats = self.storage_engine.get_statistics()

        return {
            "cache_hit_rate": storage_stats["buffer_stats"]["hit_rate"],
            "cache_hits": storage_stats["buffer_stats"]["hit_count"],
            "cache_misses": storage_stats["buffer_stats"]["miss_count"],
            "total_pages": storage_stats["total_pages"],
            "database_size": storage_stats["database_size"],
        }

    def backup_database(self, backup_path: str) -> bool:
        """
        备份数据库（简化实现）

        Args:
            backup_path: 备份文件路径

        Returns:
            是否成功
        """
        try:
            # 刷新所有数据到磁盘
            self.storage_engine.flush()

            # 简单的文件复制备份
            import shutil

            shutil.copy2(str(self.db_path), backup_path)

            print(f"数据库已备份到: {backup_path}")
            return True

        except Exception as e:
            print(f"备份失败: {e}")
            return False

    def vacuum(self) -> bool:
        """
        优化数据库（清理碎片等）

        Returns:
            是否成功
        """
        try:
            # 刷新所有脏页
            self.storage_engine.flush()

            # 这里可以添加更多优化操作
            # 如：重新组织页面、清理未使用的空间等

            print("数据库优化完成")
            return True

        except Exception as e:
            print(f"数据库优化失败: {e}")
            return False

    def close(self):
        """关闭数据库引擎"""
        if self.is_open:
            try:
                # 刷新所有数据
                self.storage_engine.flush()

                # 关闭存储引擎
                self.storage_engine.close()

                self.is_open = False
                print(f"数据库引擎已关闭: {self.db_name}")

            except Exception as e:
                print(f"关闭数据库时发生错误: {e}")

    def __enter__(self):
        """支持with语句"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持with语句"""
        self.close()

    def __repr__(self):
        status = "OPEN" if self.is_open else "CLOSED"
        return f"DatabaseEngine(name='{self.db_name}', status={status})"


def main():
    """演示数据库引擎功能"""

    print("MiniDB 数据库引擎演示")
    print("=" * 40)

    # 创建测试数据库
    db_path = "demo_minidb.db"

    # 清理现有文件
    if os.path.exists(db_path):
        os.remove(db_path)

    try:
        with DatabaseEngine(db_path, buffer_size=16) as db:

            print("\n1. 创建用户表...")
            result = db.execute_sql(
                """
                CREATE TABLE users (
                    id INT PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    email VARCHAR UNIQUE,
                    age INT DEFAULT 18,
                    created_at VARCHAR
                )
            """
            )
            print(f"   结果: {result}")

            print("\n2. 插入测试数据...")
            test_users = [
                "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 25)",
                "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 30)",
                "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@example.com', 22)",
                "INSERT INTO users (id, name, email, age) VALUES (4, 'Diana', 'diana@example.com', 28)",
            ]

            for sql in test_users:
                result = db.execute_sql(sql)
                print(f"   插入结果: {'成功' if result.success else '失败'}")

            print("\n3. 查询所有用户...")
            result = db.execute_sql("SELECT * FROM users")
            print(f"   查询结果: {result}")
            if result.success and result.data:
                for i, user in enumerate(result.data, 1):
                    print(f"   用户{i}: {user}")

            print("\n4. 条件查询...")
            result = db.execute_sql("SELECT name, age FROM users WHERE age > 25")
            print(f"   条件查询结果: {result}")
            if result.success and result.data:
                for user in result.data:
                    print(f"   {user['name']}: {user['age']}岁")

            print("\n5. 创建产品表...")
            result = db.execute_sql(
                """
                CREATE TABLE products (
                    pid INT PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    price FLOAT,
                    category VARCHAR
                )
            """
            )
            print(f"   结果: {result}")

            print("\n6. 插入产品数据...")
            products = [
                "INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics')",
                "INSERT INTO products VALUES (2, 'Book', 29.99, 'Education')",
                "INSERT INTO products VALUES (3, 'Coffee', 4.99, 'Food')",
            ]

            results = db.execute_batch(products)
            success_count = sum(1 for r in results if r.success)
            print(f"   批量插入: {success_count}/{len(products)} 成功")

            print("\n7. 数据库信息...")
            db_info = db.get_database_info()
            print(f"   数据库名: {db_info['name']}")
            print(f"   表数量: {db_info['tables']}")
            print(f"   总记录数: {db_info['total_records']}")
            print(f"   数据库大小: {db_info['database_size']} 字节")

            print("\n8. 性能统计...")
            perf_stats = db.get_performance_stats()
            print(f"   缓存命中率: {perf_stats['cache_hit_rate']:.2%}")
            print(f"   缓存命中: {perf_stats['cache_hits']}")
            print(f"   缓存失误: {perf_stats['cache_misses']}")

            print("\n9. 表详细信息...")
            for table_name in db.get_all_tables():
                table_info = db.get_table_info(table_name)
                if table_info:
                    print(f"   表 {table_name}:")
                    print(f"     记录数: {table_info['record_count']}")
                    print(f"     列数: {len(table_info['columns'])}")
                    print(f"     页面数: {table_info['page_count']}")

            print("\n10. 数据库备份...")
            backup_success = db.backup_database("demo_minidb_backup.db")
            print(f"    备份结果: {'成功' if backup_success else '失败'}")

    except Exception as e:
        print(f"演示过程中出现错误: {e}")
        import traceback

        traceback.print_exc()

    # 清理演示文件
    cleanup_files = ["demo_minidb.db", "demo_minidb_backup.db"]
    for file_name in cleanup_files:
        if os.path.exists(file_name):
            try:
                os.remove(file_name)
            except OSError:
                pass

    print("\n🎉 MiniDB 数据库引擎演示完成!")


if __name__ == "__main__":
    main()
