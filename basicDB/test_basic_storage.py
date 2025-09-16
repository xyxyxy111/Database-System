"""简单的存储引擎测试 - 步骤调试"""

import os
from storage.disk_manager import DiskManager
from storage.page_manager import Page
from storage.buffer_manager import BufferManager

def test_basic_operations():
    """测试基本的存储操作"""
    
    db_path = "debug_test.db"
    
    # 清理测试文件
    if os.path.exists(db_path):
        os.remove(db_path)
    
    print("基本存储操作测试")
    print("=" * 30)
    
    try:
        # 步骤1: 创建磁盘管理器
        print("1. 创建磁盘管理器...")
        disk_manager = DiskManager(db_path)
        print(f"   数据库文件: {db_path}")
        
        # 步骤2: 分配页面
        print("2. 分配页面...")
        page_id = disk_manager.allocate_page()
        print(f"   分配的页面ID: {page_id}")
        
        # 步骤3: 创建页面对象
        print("3. 创建页面对象...")
        page = Page(page_id)
        page.page_type = "test"
        print(f"   页面ID: {page.page_id}")
        print(f"   页面类型: {page.page_type}")
        
        # 步骤4: 写入数据到页面
        print("4. 写入数据到页面...")
        test_data = b"Hello, World!"
        success = page.write_data(0, test_data)
        print(f"   写入成功: {success}")
        
        # 步骤5: 将页面写入磁盘
        print("5. 将页面写入磁盘...")
        page_bytes = page.to_bytes()
        print(f"   页面字节长度: {len(page_bytes)}")
        success = disk_manager.write_page(page_id, page_bytes)
        print(f"   写入磁盘成功: {success}")
        
        # 步骤6: 从磁盘读取页面
        print("6. 从磁盘读取页面...")
        read_data = disk_manager.read_page(page_id)
        print(f"   读取数据长度: {len(read_data) if read_data else 0}")
        
        if read_data:
            print(f"   前24字节: {read_data[:24].hex()}")
            
            # 步骤7: 创建新页面并恢复数据
            print("7. 从字节数据恢复页面...")
            new_page = Page(page_id)
            new_page.from_bytes(read_data)
            print(f"   恢复的页面ID: {new_page.page_id}")
            print(f"   恢复的页面类型: {new_page.header.page_type}")
            
            # 读取之前写入的数据
            data_region = new_page.get_data_region()
            recovered_data = data_region[:len(test_data)]
            print(f"   恢复的数据: {recovered_data}")
        
        # 关闭磁盘管理器
        disk_manager.close()
        
        print("\n✓ 基本操作测试成功!")
        
    except Exception as e:
        print(f"\n✗ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_basic_operations()
