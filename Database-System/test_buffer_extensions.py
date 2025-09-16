#!/usr/bin/env python3
"""
测试多种缓存替换策略和预读机制
"""

import os
import sys
import time
import tempfile

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.buffer_manager import BufferManager, ReplacementPolicy
from storage.disk_manager import DiskManager


def test_replacement_policies():
    """测试不同的替换策略"""
    print("=" * 60)
    print("🔧 测试多种缓存替换策略")
    print("=" * 60)

    # 创建临时数据库文件
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_file.close()

    try:
        policies = [
            ReplacementPolicy.LRU,
            ReplacementPolicy.FIFO,
            ReplacementPolicy.CLOCK,
            ReplacementPolicy.LFU,
        ]

        for policy in policies:
            print(f"\n📊 测试 {policy.value} 替换策略")
            print("-" * 40)

            dm = DiskManager(temp_file.name)
            bm = BufferManager(
                dm, buffer_size=3, replacement_policy=policy, enable_prefetch=False
            )

            # 创建测试页面数据
            test_pages = [0, 1, 2, 3, 4, 5]
            for page_id in test_pages:
                data = f"Page {page_id} data".encode().ljust(dm.PAGE_SIZE, b"\x00")
                dm.write_page(page_id, data)

            # 测试访问模式
            access_pattern = [0, 1, 2, 3, 0, 1, 4, 0, 5, 2]

            print(f"访问模式: {access_pattern}")
            print("缓存状态变化:")

            for i, page_id in enumerate(access_pattern):
                page = bm.get_page(page_id)
                cached_pages = list(bm.buffer_pool.keys())
                hit_rate = bm.stats.get_hit_rate()

                print(f"  步骤 {i+1}: 访问页面 {page_id}")
                print(f"    缓存内容: {cached_pages}")
                print(f"    命中率: {hit_rate:.2%}")

                if policy == ReplacementPolicy.CLOCK:
                    ref_bits = {
                        pid: bm.reference_bits.get(pid, False) for pid in cached_pages
                    }
                    print(f"    引用位: {ref_bits}")
                elif policy == ReplacementPolicy.LFU:
                    frequencies = {
                        pid: bm.frequency_counter.get(pid, 0) for pid in cached_pages
                    }
                    print(f"    访问频率: {frequencies}")

            # 最终统计
            stats = bm.stats.to_dict()
            print(f"\n最终统计:")
            print(f"  命中次数: {stats['hit_count']}")
            print(f"  未命中次数: {stats['miss_count']}")
            print(f"  驱逐次数: {stats['eviction_count']}")
            print(f"  命中率: {stats['hit_rate']:.2%}")

            bm.shutdown()
            dm.close()

    finally:
        # 清理临时文件
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)


def test_prefetch_mechanism():
    """测试预读机制"""
    print("\n" + "=" * 60)
    print("🚀 测试预读机制")
    print("=" * 60)

    # 创建临时数据库文件
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_file.close()

    try:
        dm = DiskManager(temp_file.name)

        # 创建测试页面数据
        for page_id in range(10):
            data = f"Sequential page {page_id}".encode().ljust(dm.PAGE_SIZE, b"\x00")
            dm.write_page(page_id, data)

        print("\n📊 不启用预读的情况:")
        print("-" * 30)

        bm_no_prefetch = BufferManager(
            dm,
            buffer_size=5,
            replacement_policy=ReplacementPolicy.LRU,
            enable_prefetch=False,
        )

        # 顺序访问
        start_time = time.time()
        for page_id in range(5):
            page = bm_no_prefetch.get_page(page_id)
            print(f"访问页面 {page_id}, 缓存大小: {len(bm_no_prefetch.buffer_pool)}")

        no_prefetch_time = time.time() - start_time
        no_prefetch_stats = bm_no_prefetch.stats.to_dict()

        print(f"耗时: {no_prefetch_time:.4f} 秒")
        print(f"命中率: {no_prefetch_stats['hit_rate']:.2%}")
        print(f"未命中次数: {no_prefetch_stats['miss_count']}")

        bm_no_prefetch.shutdown()

        print("\n📊 启用预读的情况:")
        print("-" * 30)

        bm_with_prefetch = BufferManager(
            dm,
            buffer_size=5,
            replacement_policy=ReplacementPolicy.LRU,
            enable_prefetch=True,
        )

        start_time = time.time()
        for page_id in range(5):
            page = bm_with_prefetch.get_page(page_id)
            print(f"访问页面 {page_id}, 缓存大小: {len(bm_with_prefetch.buffer_pool)}")
            # 显示预读的页面
            cached_pages = list(bm_with_prefetch.buffer_pool.keys())
            print(f"  缓存中的页面: {sorted(cached_pages)}")

        prefetch_time = time.time() - start_time
        prefetch_stats = bm_with_prefetch.stats.to_dict()

        print(f"耗时: {prefetch_time:.4f} 秒")
        print(f"命中率: {prefetch_stats['hit_rate']:.2%}")
        print(f"未命中次数: {prefetch_stats['miss_count']}")

        print(f"\n📈 预读效果:")
        print(
            f"  未命中次数减少: {no_prefetch_stats['miss_count'] - prefetch_stats['miss_count']}"
        )
        print(
            f"  命中率提升: {prefetch_stats['hit_rate'] - no_prefetch_stats['hit_rate']:.2%}"
        )

        # 显示预读日志
        print(f"\n📋 预读日志（最近10条）:")
        prefetch_logs = [
            log for log in bm_with_prefetch.replacement_log if "PREFETCH" in log
        ]
        for log in prefetch_logs[-10:]:
            print(f"  {log}")

        bm_with_prefetch.shutdown()
        dm.close()

    finally:
        # 清理临时文件
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)


if __name__ == "__main__":
    test_replacement_policies()
    test_prefetch_mechanism()

    print("\n" + "=" * 60)
    print("🎉 缓存替换策略和预读机制测试完成！")
    print("=" * 60)
