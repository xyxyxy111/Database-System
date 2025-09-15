"""缓存管理器 - 实现页面缓存机制，支持LRU和FIFO替换策略"""

import time
from collections import OrderedDict
from typing import Dict, Optional, Set, List
from enum import Enum
from .disk_manager import DiskManager
from .page_manager import Page, PageManager


class ReplacementPolicy(Enum):
    """替换策略枚举"""
    LRU = "LRU"    # 最近最少使用
    FIFO = "FIFO"  # 先进先出


class CacheStats:
    """缓存统计信息"""
    
    def __init__(self):
        self.hit_count = 0
        self.miss_count = 0
        self.eviction_count = 0
        self.flush_count = 0
        self.start_time = time.time()
    
    def record_hit(self):
        """记录缓存命中"""
        self.hit_count += 1
    
    def record_miss(self):
        """记录缓存未命中"""
        self.miss_count += 1
    
    def record_eviction(self):
        """记录页面驱逐"""
        self.eviction_count += 1
    
    def record_flush(self):
        """记录页面刷新"""
        self.flush_count += 1
    
    def get_hit_rate(self) -> float:
        """获取缓存命中率"""
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0.0
    
    def get_runtime(self) -> float:
        """获取运行时间（秒）"""
        return time.time() - self.start_time
    
    def reset(self):
        """重置统计信息"""
        self.hit_count = 0
        self.miss_count = 0
        self.eviction_count = 0
        self.flush_count = 0
        self.start_time = time.time()
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'hit_count': self.hit_count,
            'miss_count': self.miss_count,
            'eviction_count': self.eviction_count,
            'flush_count': self.flush_count,
            'hit_rate': self.get_hit_rate(),
            'runtime': self.get_runtime()
        }


class BufferManager:
    """缓存管理器"""
    
    def __init__(self, disk_manager: DiskManager, 
                 buffer_size: int = 64, 
                 replacement_policy: ReplacementPolicy = ReplacementPolicy.LRU):
        self.disk_manager = disk_manager
        self.buffer_size = buffer_size
        self.replacement_policy = replacement_policy
        self.page_manager = PageManager(disk_manager.PAGE_SIZE)
        
        # 缓存相关数据结构
        self.buffer_pool: Dict[int, Page] = {}
        self.dirty_pages: Set[int] = set()
        self.pinned_pages: Set[int] = set()
        
        # 替换策略相关
        if replacement_policy == ReplacementPolicy.LRU:
            self.access_order = OrderedDict()  # page_id -> access_time
        else:  # FIFO
            self.access_order = OrderedDict()  # page_id -> insertion_time
        
        # 统计信息
        self.stats = CacheStats()
        
        # 日志
        self.replacement_log: List[str] = []
    
    def get_page(self, page_id: int) -> Optional[Page]:
        """获取页面（支持缓存）"""
        # 检查页面是否在缓存中
        if page_id in self.buffer_pool:
            self.stats.record_hit()
            self._update_access(page_id)
            self._log(f"Cache HIT: Page {page_id}")
            return self.buffer_pool[page_id]
        
        # 缓存未命中，从磁盘加载
        self.stats.record_miss()
        self._log(f"Cache MISS: Page {page_id}")
        
        page_data = self.disk_manager.read_page(page_id)
        if not page_data:
            return None
        
        # 如果缓存已满，执行页面替换
        if len(self.buffer_pool) >= self.buffer_size:
            if not self._evict_page():
                return None  # 无法驱逐页面
        
        # 创建新页面并加载数据
        page = self.page_manager.get_page(page_id)
        if not page:
            # 页面不存在，创建新页面
            page = self.page_manager.create_page(page_id)
        
        page.from_bytes(page_data)
        
        # 添加到缓存
        self.buffer_pool[page_id] = page
        self._update_access(page_id)
        
        return page
    
    def pin_page(self, page_id: int) -> Optional[Page]:
        """固定页面（防止被替换）"""
        page = self.get_page(page_id)
        if page:
            page.pin()
            self.pinned_pages.add(page_id)
            self._log(f"PIN: Page {page_id} (pin_count={page.pin_count})")
        return page
    
    def unpin_page(self, page_id: int, is_dirty: bool = False):
        """取消固定页面"""
        if page_id in self.buffer_pool:
            page = self.buffer_pool[page_id]
            page.unpin()
            
            if page.pin_count == 0:
                self.pinned_pages.discard(page_id)
            
            if is_dirty:
                page.mark_dirty()
                self.dirty_pages.add(page_id)
            
            self._log(f"UNPIN: Page {page_id} (pin_count={page.pin_count}, dirty={is_dirty})")
    
    def flush_page(self, page_id: int) -> bool:
        """刷新页面到磁盘"""
        if page_id not in self.buffer_pool:
            return False
        
        page = self.buffer_pool[page_id]
        
        if page.is_dirty():
            success = self.disk_manager.write_page(page_id, page.to_bytes())
            if success:
                page.clear_dirty()
                self.dirty_pages.discard(page_id)
                self.stats.record_flush()
                self._log(f"FLUSH: Page {page_id}")
                return True
            else:
                self._log(f"FLUSH FAILED: Page {page_id}")
                return False
        
        return True  # 页面不脏，无需刷新
    
    def flush_all_pages(self) -> bool:
        """刷新所有脏页到磁盘"""
        all_success = True
        dirty_page_ids = list(self.dirty_pages)
        
        for page_id in dirty_page_ids:
            if not self.flush_page(page_id):
                all_success = False
        
        self._log(f"FLUSH ALL: {len(dirty_page_ids)} pages")
        return all_success
    
    def _evict_page(self) -> bool:
        """驱逐页面"""
        victim_page_id = self._select_victim()
        
        if victim_page_id is None:
            self._log("EVICT FAILED: No victim found")
            return False
        
        # 刷新脏页
        if victim_page_id in self.dirty_pages:
            if not self.flush_page(victim_page_id):
                return False
        
        # 从缓存移除
        del self.buffer_pool[victim_page_id]
        self.access_order.pop(victim_page_id, None)
        self.dirty_pages.discard(victim_page_id)
        self.pinned_pages.discard(victim_page_id)
        
        self.stats.record_eviction()
        self._log(f"EVICT: Page {victim_page_id}")
        
        return True
    
    def _select_victim(self) -> Optional[int]:
        """选择被驱逐的页面"""
        # 寻找未被固定的页面
        for page_id in self.access_order:
            if page_id not in self.pinned_pages:
                return page_id
        
        return None  # 没有可驱逐的页面
    
    def _update_access(self, page_id: int):
        """更新页面访问信息"""
        current_time = time.time()
        
        if self.replacement_policy == ReplacementPolicy.LRU:
            # LRU: 更新访问时间
            self.access_order[page_id] = current_time
            # 重新排序（移到最后）
            self.access_order.move_to_end(page_id)
        else:  # FIFO
            # FIFO: 如果页面不存在，记录插入时间
            if page_id not in self.access_order:
                self.access_order[page_id] = current_time
    
    def _log(self, message: str):
        """记录日志"""
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        log_entry = f"[{timestamp}] {message}"
        self.replacement_log.append(log_entry)
        
        # 限制日志大小
        if len(self.replacement_log) > 1000:
            self.replacement_log = self.replacement_log[-500:]
    
    def get_buffer_stats(self) -> Dict:
        """获取缓存统计信息"""
        return {
            'buffer_size': self.buffer_size,
            'current_pages': len(self.buffer_pool),
            'dirty_pages': len(self.dirty_pages),
            'pinned_pages': len(self.pinned_pages),
            'replacement_policy': self.replacement_policy.value,
            **self.stats.to_dict()
        }
    
    def print_stats(self):
        """打印统计信息"""
        stats = self.get_buffer_stats()
        print("缓存管理器统计信息:")
        print("=" * 40)
        print(f"缓存大小: {stats['buffer_size']}")
        print(f"当前页面数: {stats['current_pages']}")
        print(f"脏页数: {stats['dirty_pages']}")
        print(f"固定页面数: {stats['pinned_pages']}")
        print(f"替换策略: {stats['replacement_policy']}")
        print(f"命中次数: {stats['hit_count']}")
        print(f"未命中次数: {stats['miss_count']}")
        print(f"命中率: {stats['hit_rate']:.2%}")
        print(f"驱逐次数: {stats['eviction_count']}")
        print(f"刷新次数: {stats['flush_count']}")
        print(f"运行时间: {stats['runtime']:.2f}秒")
    
    def print_recent_logs(self, count: int = 10):
        """打印最近的日志"""
        print(f"\n最近的 {min(count, len(self.replacement_log))} 条日志:")
        print("-" * 50)
        for log in self.replacement_log[-count:]:
            print(log)
    
    def clear_stats(self):
        """清空统计信息"""
        self.stats.reset()
        self.replacement_log.clear()
    
    def shutdown(self):
        """关闭缓存管理器"""
        self.flush_all_pages()


def main():
    """测试用例"""
    import os
    from pathlib import Path
    
    print("测试缓存管理器...")
    print("=" * 50)
    
    # 清理测试文件
    db_path = "test_buffer.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # 创建磁盘管理器和缓存管理器
    dm = DiskManager(db_path)
    bm = BufferManager(dm, buffer_size=3, replacement_policy=ReplacementPolicy.LRU)
    
    print("测试页面访问和缓存...")
    
    # 分配一些页面并写入数据
    page_ids = []
    for i in range(5):
        page_id = dm.allocate_page()
        page_ids.append(page_id)
        
        # 写入测试数据
        test_data = f"Test data for page {page_id}".encode().ljust(dm.PAGE_SIZE, b'\x00')
        dm.write_page(page_id, test_data)
        
        print(f"创建页面 {page_id}")
    
    print(f"\n访问页面 {page_ids}...")
    
    # 访问页面（测试缓存命中和未命中）
    for page_id in page_ids:
        page = bm.get_page(page_id)
        if page:
            data = page.read_data(0, 20)
            if data:
                print(f"读取页面 {page_id}: {data.decode().strip()}")
    
    print("\n再次访问前3个页面（应该命中）...")
    for page_id in page_ids[:3]:
        page = bm.get_page(page_id)
    
    print("\n测试页面固定...")
    pin_page = bm.pin_page(page_ids[0])
    if pin_page:
        print(f"固定页面 {page_ids[0]}")
    
    print("\n访问更多页面，触发替换...")
    for page_id in page_ids:
        bm.get_page(page_id)
    
    # 打印统计信息
    print()
    bm.print_stats()
    
    # 打印日志
    bm.print_recent_logs(15)
    
    # 清理
    bm.shutdown()
    dm.close()
    
    # 清理测试文件
    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    main()
