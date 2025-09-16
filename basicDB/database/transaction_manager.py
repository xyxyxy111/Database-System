"""
事务管理器
实现基础的事务控制和回滚机制
"""

from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import json


class TransactionState(Enum):
    """事务状态"""

    ACTIVE = auto()  # 活跃状态
    COMMITTED = auto()  # 已提交
    ABORTED = auto()  # 已回滚


class OperationType(Enum):
    """操作类型"""

    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    CREATE_TABLE = auto()
    DROP_TABLE = auto()


class TransactionLogEntry:
    """事务日志条目"""

    def __init__(
        self,
        transaction_id: int,
        operation_type: OperationType,
        table_name: str,
        old_data: Any = None,
        new_data: Any = None,
        position: Optional[int] = None,
    ):
        self.transaction_id = transaction_id
        self.operation_type = operation_type
        self.table_name = table_name
        self.old_data = old_data
        self.new_data = new_data
        self.position = position
        self.timestamp = datetime.now()

    def __repr__(self):
        return (
            f"LogEntry(txn={self.transaction_id}, "
            f"op={self.operation_type.name}, "
            f"table={self.table_name})"
        )


class Transaction:
    """事务对象"""

    def __init__(self, transaction_id: int):
        self.transaction_id = transaction_id
        self.state = TransactionState.ACTIVE
        self.start_time = datetime.now()
        self.log_entries: List[TransactionLogEntry] = []
        self.modified_tables: set = set()

    def add_log_entry(self, entry: TransactionLogEntry):
        """添加日志条目"""
        self.log_entries.append(entry)
        self.modified_tables.add(entry.table_name)

    def __repr__(self):
        return (
            f"Transaction(id={self.transaction_id}, "
            f"state={self.state.name}, "
            f"entries={len(self.log_entries)})"
        )


class TransactionManager:
    """事务管理器"""

    def __init__(self, storage_engine, database_catalog):
        self.storage_engine = storage_engine
        self.database_catalog = database_catalog
        self.current_transaction_id = 0
        self.active_transactions: Dict[int, Transaction] = {}
        self.global_transaction_log: List[TransactionLogEntry] = []

        # 每个连接的当前事务（简化实现，假设单连接）
        self.current_transaction: Optional[Transaction] = None

    def begin_transaction(self) -> int:
        """开始新事务"""
        self.current_transaction_id += 1
        transaction = Transaction(self.current_transaction_id)

        # 如果已有活跃事务，先提交
        if (
            self.current_transaction
            and self.current_transaction.state == TransactionState.ACTIVE
        ):
            self.commit_transaction(self.current_transaction.transaction_id)

        self.active_transactions[self.current_transaction_id] = transaction
        self.current_transaction = transaction

        return self.current_transaction_id

    def commit_transaction(self, transaction_id: Optional[int] = None) -> bool:
        """提交事务"""
        if transaction_id is None:
            transaction = self.current_transaction
        else:
            transaction = self.active_transactions.get(transaction_id)

        if not transaction or transaction.state != TransactionState.ACTIVE:
            return False

        # 标记事务为已提交
        transaction.state = TransactionState.COMMITTED

        # 将事务日志添加到全局日志
        self.global_transaction_log.extend(transaction.log_entries)

        # 持久化变更（在这个简化实现中，变更已经应用到存储）
        # 在真实的数据库中，这里会将事务的变更写入磁盘

        # 清理
        if transaction == self.current_transaction:
            self.current_transaction = None

        del self.active_transactions[transaction.transaction_id]

        return True

    def rollback_transaction(self, transaction_id: Optional[int] = None) -> bool:
        """回滚事务"""
        if transaction_id is None:
            transaction = self.current_transaction
        else:
            transaction = self.active_transactions.get(transaction_id)

        if not transaction or transaction.state != TransactionState.ACTIVE:
            return False

        # 标记事务为已回滚
        transaction.state = TransactionState.ABORTED

        # 逆序回滚操作
        for entry in reversed(transaction.log_entries):
            self._undo_operation(entry)

        # 清理
        if transaction == self.current_transaction:
            self.current_transaction = None

        del self.active_transactions[transaction.transaction_id]

        return True

    def _undo_operation(self, entry: TransactionLogEntry):
        """撤销单个操作 - 简化版本"""
        print(f"撤销操作: {entry.operation_type.name} on {entry.table_name}")

        # 注意：这是一个简化的实现
        # 真正的数据库系统需要更复杂的撤销日志

        if entry.operation_type == OperationType.INSERT:
            # 对于插入操作，我们无法精确删除特定记录
            # 因为我们的存储引擎没有提供按位置删除的功能
            # 这里仅作演示
            print(f"  警告: 无法精确撤销插入操作 (表: {entry.table_name})")

        elif entry.operation_type == OperationType.UPDATE:
            print(f"  警告: 无法精确撤销更新操作 (表: {entry.table_name})")

        elif entry.operation_type == OperationType.DELETE:
            print(f"  警告: 无法精确撤销删除操作 (表: {entry.table_name})")

        elif entry.operation_type == OperationType.CREATE_TABLE:
            # 撤销创建表：删除表
            try:
                self.storage_engine.drop_table(entry.table_name)
                self.database_catalog.drop_table(entry.table_name)
                print(f"  成功撤销表创建: {entry.table_name}")
            except Exception as e:
                print(f"  撤销表创建失败: {e}")

        elif entry.operation_type == OperationType.DROP_TABLE:
            print(f"  警告: 无法撤销表删除操作 (表: {entry.table_name})")

    def log_insert(
        self, table_name: str, record_data: List[Any], position: Optional[int] = None
    ):
        """记录插入操作"""
        if self.current_transaction:
            entry = TransactionLogEntry(
                self.current_transaction.transaction_id,
                OperationType.INSERT,
                table_name,
                None,  # 插入操作没有旧数据
                record_data,
                position,
            )
            self.current_transaction.add_log_entry(entry)

    def log_update(
        self, table_name: str, position: int, old_data: List[Any], new_data: List[Any]
    ):
        """记录更新操作"""
        if self.current_transaction:
            entry = TransactionLogEntry(
                self.current_transaction.transaction_id,
                OperationType.UPDATE,
                table_name,
                old_data,
                new_data,
                position,
            )
            self.current_transaction.add_log_entry(entry)

    def log_delete(
        self, table_name: str, record_data: List[Any], position: Optional[int] = None
    ):
        """记录删除操作"""
        if self.current_transaction:
            entry = TransactionLogEntry(
                self.current_transaction.transaction_id,
                OperationType.DELETE,
                table_name,
                record_data,
                None,  # 删除操作没有新数据
                position,
            )
            self.current_transaction.add_log_entry(entry)

    def log_create_table(self, table_name: str):
        """记录创建表操作"""
        if self.current_transaction:
            entry = TransactionLogEntry(
                self.current_transaction.transaction_id,
                OperationType.CREATE_TABLE,
                table_name,
            )
            self.current_transaction.add_log_entry(entry)

    def log_drop_table(self, table_name: str, table_definition: Dict):
        """记录删除表操作"""
        if self.current_transaction:
            entry = TransactionLogEntry(
                self.current_transaction.transaction_id,
                OperationType.DROP_TABLE,
                table_name,
                table_definition,  # 保存表定义以便回滚
            )
            self.current_transaction.add_log_entry(entry)

    def is_in_transaction(self) -> bool:
        """检查是否在事务中"""
        return (
            self.current_transaction is not None
            and self.current_transaction.state == TransactionState.ACTIVE
        )

    def get_current_transaction_id(self) -> Optional[int]:
        """获取当前事务ID"""
        if self.current_transaction:
            return self.current_transaction.transaction_id
        return None

    def get_transaction_info(self) -> Dict[str, Any]:
        """获取当前事务信息"""
        if not self.current_transaction:
            return {"in_transaction": False}

        return {
            "in_transaction": True,
            "transaction_id": self.current_transaction.transaction_id,
            "state": self.current_transaction.state.name,
            "start_time": self.current_transaction.start_time.isoformat(),
            "operations_count": len(self.current_transaction.log_entries),
            "modified_tables": list(self.current_transaction.modified_tables),
        }
