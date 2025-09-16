#!/usr/bin/env python3
"""
直接检查磁盘文件内容的验证脚本
"""

import os
import sys
import json

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.storage_engine import StorageEngine


def inspect_database_file(file_path):
    """直接检查数据库文件的内容"""
    print("=" * 60)
    print(f"🔍 直接检查数据库文件: {file_path}")
    print("=" * 60)

    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return

    file_size = os.path.getsize(file_path)
    print(f"📁 文件大小: {file_size} 字节")

    with open(file_path, "rb") as f:
        content = f.read()

    # 分析文件头部（前8字节）
    header = content[:8]
    print(f"📋 文件头部: {header.hex()}")

    # 尝试解析页面数量
    if len(header) >= 4:
        page_count = int.from_bytes(header[:4], byteorder="little")
        print(f"📊 页面数量: {page_count}")

    # 检查每个页面
    page_size = 4096
    file_header_size = 8

    for page_num in range(page_count):
        page_offset = file_header_size + page_num * page_size
        if page_offset + page_size <= len(content):
            page_data = content[page_offset : page_offset + page_size]

            print(f"\n📄 页面 {page_num}:")
            print(f"  偏移量: {page_offset}")
            print(f"  页面头部: {page_data[:32].hex()}")

            # 尝试找到可读的文本内容
            text_content = ""
            for i, byte in enumerate(page_data):
                if 32 <= byte <= 126:  # 可打印ASCII字符
                    text_content += chr(byte)
                elif byte == 0:
                    text_content += "."
                else:
                    text_content += "?"

            # 显示前200个字符
            print(f"  内容预览: {text_content[:200]}")

            # 查找JSON数据（元数据）
            try:
                # 寻找JSON起始标记
                json_start = page_data.find(b"{")
                if json_start != -1:
                    # 寻找JSON结束标记
                    json_end = page_data.rfind(b"}")
                    if json_end != -1 and json_end > json_start:
                        json_data = page_data[json_start : json_end + 1].decode(
                            "utf-8", errors="ignore"
                        )
                        try:
                            parsed_json = json.loads(json_data)
                            print(
                                f"  📊 解析的元数据: {json.dumps(parsed_json, indent=2, ensure_ascii=False)}"
                            )
                        except json.JSONDecodeError:
                            print(f"  📄 原始JSON数据: {json_data[:100]}...")
            except Exception as e:
                print(f"  ⚠️  JSON解析失败: {e}")

            # 查找记录数据模式
            record_patterns = []
            i = 32  # 跳过页面头部
            while i < len(page_data) - 4:
                # 查找可能的记录长度标记
                record_len = int.from_bytes(page_data[i : i + 4], byteorder="little")
                if 10 <= record_len <= 200:  # 合理的记录长度
                    record_data = page_data[i + 4 : i + 4 + record_len]
                    # 检查是否包含可读字符
                    readable_chars = sum(1 for b in record_data if 32 <= b <= 126)
                    if readable_chars > len(record_data) * 0.3:  # 至少30%可读字符
                        readable_text = "".join(
                            chr(b) if 32 <= b <= 126 else "." for b in record_data
                        )
                        record_patterns.append(
                            f"记录@{i}: 长度={record_len}, 内容='{readable_text[:50]}'"
                        )
                        i += 4 + record_len
                    else:
                        i += 1
                else:
                    i += 1

            if record_patterns:
                print(f"  📝 可能的记录:")
                for pattern in record_patterns[:5]:  # 只显示前5个
                    print(f"    {pattern}")


def cross_verify_persistence():
    """交叉验证持久化功能"""
    print("\n" + "=" * 60)
    print("🔄 交叉验证持久化功能")
    print("=" * 60)

    test_file = "cross_verify.db"

    # 清理旧文件
    if os.path.exists(test_file):
        os.remove(test_file)

    print("第1轮：写入数据")
    print("-" * 30)

    # 第一次写入
    storage1 = StorageEngine(test_file)
    storage1.create_table(
        "users", ["id", "username", "email"], ["INT", "VARCHAR", "VARCHAR"]
    )

    users_data = [
        [1, "admin", "admin@example.com"],
        [2, "user1", "user1@example.com"],
        [3, "test", "test@example.com"],
    ]

    for user in users_data:
        storage1.insert_record("users", user)
        print(f"  写入: {user}")

    storage1.flush()
    storage1.close()

    print(f"  文件大小: {os.path.getsize(test_file)} 字节")

    # 检查文件内容
    inspect_database_file(test_file)

    print("\n第2轮：程序重启后读取")
    print("-" * 30)

    # 模拟程序重启
    storage2 = StorageEngine(test_file)
    recovered_users = storage2.scan_table("users")

    print(f"  恢复的记录数: {len(recovered_users)}")
    for user in recovered_users:
        print(f"  恢复: {user.values}")

    # 验证数据一致性
    original = [user for user in users_data]
    recovered = [user.values for user in recovered_users]

    print(f"  数据一致性: {'✅ 通过' if original == recovered else '❌ 失败'}")

    storage2.close()

    print("\n第3轮：追加更多数据")
    print("-" * 30)

    # 追加数据
    storage3 = StorageEngine(test_file)
    new_users = [[4, "newuser", "new@example.com"], [5, "guest", "guest@example.com"]]

    for user in new_users:
        storage3.insert_record("users", user)
        print(f"  追加: {user}")

    storage3.flush()
    storage3.close()

    print(f"  文件大小: {os.path.getsize(test_file)} 字节")

    print("\n第4轮：最终验证")
    print("-" * 30)

    # 最终验证
    storage4 = StorageEngine(test_file)
    final_users = storage4.scan_table("users")

    print(f"  最终记录数: {len(final_users)}")
    expected_total = len(users_data) + len(new_users)
    print(f"  期望记录数: {expected_total}")
    print(
        f"  验证结果: {'✅ 通过' if len(final_users) == expected_total else '❌ 失败'}"
    )

    print("  所有记录:")
    for i, user in enumerate(final_users, 1):
        print(f"    {i}. {user.values}")

    storage4.close()

    # 最终文件检查
    print("\n最终文件内容检查:")
    inspect_database_file(test_file)


if __name__ == "__main__":
    # 检查现有的演示文件
    demo_files = ["minidb_demo.db", "persistence_test.db"]

    for file_name in demo_files:
        if os.path.exists(file_name):
            inspect_database_file(file_name)
            print("\n")

    # 进行交叉验证
    cross_verify_persistence()
