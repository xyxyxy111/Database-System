from execution.sytem_catalog import SystemCatalog

class CompilerCatalog:
    def __init__(self, syscat: SystemCatalog) -> None:
        self.syscat = syscat

    def table_exists(self, name: str) -> bool:
        # 简化：总是允许按需创建
        return True
