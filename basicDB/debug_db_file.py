"""调试存储引擎的文件格式"""

import os
import struct

def debug_db_file():
    """调试数据库文件内容"""
    
    db_path = "simple_test.db"
    
    if os.path.exists(db_path):
        print(f"检查文件: {db_path}")
        print(f"文件大小: {os.path.getsize(db_path)} 字节")
        
        with open(db_path, 'rb') as f:
            # 读取前面一些字节查看内容
            data = f.read(100)
            
            print("前100字节的十六进制内容:")
            print(data.hex())
            
            print("\n前24字节尝试解包:")
            if len(data) >= 24:
                try:
                    header = struct.unpack('IIIIii', data[:24])
                    print(f"成功解包: {header}")
                except struct.error as e:
                    print(f"解包失败: {e}")
            else:
                print(f"数据长度不足: {len(data)} < 24")
            
            print(f"\n实际数据内容 (前50字节):")
            print(repr(data[:50]))
    else:
        print(f"文件不存在: {db_path}")

if __name__ == "__main__":
    debug_db_file()
