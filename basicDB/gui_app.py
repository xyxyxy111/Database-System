# gui_app.py
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from pathlib import Path
import sys
import traceback
from typing import Any, List, Dict

# 将项目根目录加入路径（按你原来的方式）
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database import dbEngine  # 你的数据库引擎接口


class MiniDBGUI:
    """MiniDB 图形界面主类（已修复：包含 switch_mode 与全部功能）"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Database System")
        self.root.geometry("900x850")
        self.root.configure(bg="#f7f7f7")

        # 数据库引擎
        self.db_path = "minidb_gui.db"
        self.db = dbEngine(self.db_path, buffer_size=16)

        # 模式和表
        self.current_mode = 'browse'  # 默认打开为浏览模式（可改为 'query'）
        self.current_table = None

        # 设置样式与界面
        self.setup_styles()
        self.setup_ui()

        # 启动时刷新表列表并根据当前模式显示对应面板
        try:
            self.refresh_table_list()
        except Exception:
            # 刷新失败也不要让程序崩溃
            traceback.print_exc()
        self.switch_mode()  # 应用初始模式显示

    def setup_styles(self):
        """定义现代化样式（尽量简单兼容各平台）"""
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("TLabel", font=("Segoe UI", 11), background="#f7f7f7")
        style.configure("TButton", font=("Segoe UI", 10), padding=6)
        style.map("TButton", background=[("active", "#e6f0ff")])

        style.configure("Treeview.Heading", font=("Segoe UI", 11, "bold"), background="#4a90e2", foreground="white")
        style.configure("Treeview", font=("Consolas", 10), rowheight=24)

        style.configure("TLabelframe.Label", font=("Segoe UI", 12, "bold"))

    def setup_ui(self):
        """创建主界面结构"""
        main_frame = ttk.Frame(self.root, padding=12)
        main_frame.grid(row=0, column=0, sticky="nsew")

        # 网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(3, weight=1)

        # 标题
        title_label = ttk.Label(main_frame, text="Database System", font=("Segoe UI", 18, "bold"), background="#f7f7f7")
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 16))

        # 模式切换
        self.setup_mode_switcher(main_frame)

        # 查询与浏览区（都创建，显示由 switch_mode 控制）
        self.setup_query_mode(main_frame)
        self.setup_browse_mode(main_frame)

        # 结果显示和状态栏
        self.setup_result_area(main_frame)
        self.setup_status_bar(main_frame)

    def setup_mode_switcher(self, parent):
        """操作模式切换区域"""
        mode_frame = ttk.LabelFrame(parent, text="操作模式", padding=10)
        mode_frame.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(0, 10))

        self.mode_var = tk.StringVar(value=self.current_mode)
        ttk.Radiobutton(mode_frame, text="查询模式", variable=self.mode_var, value="query", command=self.switch_mode).grid(row=0, column=0, padx=12)
        ttk.Radiobutton(mode_frame, text="浏览模式", variable=self.mode_var, value="browse", command=self.switch_mode).grid(row=0, column=1, padx=12)

        self.db_info_label = ttk.Label(mode_frame, text=f"数据库: {self.db_path}")
        self.db_info_label.grid(row=0, column=2, padx=(20, 0))

    def setup_query_mode(self, parent):
        """查询模式界面"""
        self.query_frame = ttk.LabelFrame(parent, text="SQL 查询", padding=10)
        self.query_frame.grid(row=2, column=0, columnspan=3, sticky="nsew", pady=(0, 10))
        self.query_frame.rowconfigure(0, weight=1)
        self.query_frame.columnconfigure(0, weight=1)
        self.query_frame.columnconfigure(1, weight=0)
        self.query_frame.columnconfigure(2, weight=0)
        sql_label = ttk.Label(self.query_frame, text="SQL 语句:")
        sql_label.grid(row=0, column=0, sticky="w", pady=(0, 6))

        self.sql_text = scrolledtext.ScrolledText(self.query_frame, height=6, width=60,font=("Consolas", 11))
        self.sql_text.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=5)

        # 执行按钮区
        button_frame = ttk.Frame(self.query_frame)
        button_frame.grid(row=1, column=0, columnspan=1, sticky="w")

        ttk.Button(button_frame, text="执行查询", command=self.execute_sql).grid(row=0, column=0, padx=3)
        ttk.Button(button_frame, text="清空", command=self.clear_sql).grid(row=0, column=1, padx=3)
        ttk.Button(button_frame, text="建表插入", command=self.show_examples1).grid(row=0, column=2, padx=3)
        ttk.Button(button_frame, text="更新删除", command=self.show_examples2).grid(row=0, column=3, padx=3)
        ttk.Button(button_frame, text="查询", command=self.show_examples3).grid(row=0, column=4, padx=3)
        # ttk.Button(button_frame, text="JOIN", command=self.show_examples4).grid(row=1, column=0, padx=3)
        ttk.Button(button_frame, text="ORDER BY", command=self.show_examples5).grid(row=0, column=5, padx=3)
        ttk.Button(button_frame, text="事务", command=self.show_examples6).grid(row=0, column=6, padx=3)
      #  ttk.Button(button_frame, text="表达式", command=self.show_examples7).grid(row=1, column=3, padx=3)
        ttk.Button(button_frame, text="聚合函数", command=self.show_examples8).grid(row=0, column=7, padx=3)

        # SQL 模板栏
        template_frame = ttk.LabelFrame(self.query_frame, text="SQL 模板", padding=6)
        template_frame.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 0))

        templates = {
            "SELECT": "SELECT * FROM table_name;",
            "INSERT": "INSERT INTO table_name (col1, col2) VALUES (val1, val2);",
            "UPDATE": "UPDATE table_name SET col1 = val WHERE condition;",
            "DELETE": "DELETE FROM table_name WHERE condition;",
            "DROP": "DROP TABLE table_name;"
        }

        col = 0
        for label, sql in templates.items():
            ttk.Button(template_frame, text=label, command=lambda s=sql: self.insert_sql_template(s)).grid(row=0, column=col, padx=6, pady=4)
            col += 1



    def setup_browse_mode(self, parent):
        """设置浏览模式界面"""
        self.browse_frame = ttk.LabelFrame(parent, text="表浏览", padding=10)
        self.browse_frame.grid(row=2, column=0, columnspan=3, sticky="nsew", pady=(0, 10))
        self.browse_frame.rowconfigure(0, weight=1)
        self.browse_frame.columnconfigure(0, weight=1)
        table_select_frame = ttk.Frame(self.browse_frame)
        table_select_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))

        ttk.Label(table_select_frame, text="选择表:").grid(row=0, column=0, padx=5)
        self.table_var = tk.StringVar()
        self.table_combo = ttk.Combobox(table_select_frame, textvariable=self.table_var, state="readonly", width=36)
        self.table_combo.grid(row=0, column=1, padx=5)
        self.table_combo.bind("<<ComboboxSelected>>", self.on_table_selected)

        # 把按钮保存在实例属性，方便后续启/禁用或修改行为
        self.refresh_btn = ttk.Button(table_select_frame, text="刷新", command=self.refresh_table_list)
        self.refresh_btn.grid(row=0, column=2, padx=5)

        self.table_info_label = ttk.Label(self.browse_frame, text="请选择一个表")
        self.table_info_label.grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 10))

        self.setup_data_table()

    def refresh_table_list(self):
        self.on_table_selected(self);

    def setup_data_table(self):
        """创建数据 Treeview（带滚动）"""
        columns_frame = ttk.Frame(self.browse_frame)
        columns_frame.grid(row=3, column=0, columnspan=3, sticky="nsew")

        scrollbar_y = ttk.Scrollbar(columns_frame, orient="vertical")
        scrollbar_x = ttk.Scrollbar(columns_frame, orient="horizontal")

        self.data_tree = ttk.Treeview(columns_frame, yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
        scrollbar_y.config(command=self.data_tree.yview)
        scrollbar_x.config(command=self.data_tree.xview)

        self.data_tree.grid(row=0, column=0, sticky="nsew")
        scrollbar_y.grid(row=0, column=1, sticky="ns")
        scrollbar_x.grid(row=1, column=0, sticky="ew")

        columns_frame.columnconfigure(0, weight=1)
        columns_frame.rowconfigure(0, weight=1)

    def setup_result_area(self, parent):
        """设置结果显示区域"""
        self.result_frame = ttk.LabelFrame(parent, text="查询结果", padding=10)
        self.result_frame.grid(row=3, column=0, columnspan=3, sticky="nsew", pady=(0, 10))

        # 上方文本区：显示执行信息
        self.result_text = scrolledtext.ScrolledText(
            self.result_frame, height=6, width=80, font=('Consolas', 9)
        )
        self.result_text.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=(0, 5))

        # 下方表格区：显示 SELECT 结果
        table_frame = ttk.Frame(self.result_frame)
        table_frame.grid(row=1, column=0, columnspan=2, sticky="nsew")

        self.result_table = ttk.Treeview(table_frame, show="headings")
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.result_table.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.result_table.xview)
        self.result_table.configure(yscroll=vsb.set, xscroll=hsb.set)

        self.result_table.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        # 布局权重
        self.result_frame.rowconfigure(0, weight=1)
        self.result_frame.rowconfigure(1, weight=2)
        self.result_frame.columnconfigure(0, weight=1)
        self.result_frame.grid_remove()  # 初始先隐藏

    def setup_status_bar(self, parent):
        """状态栏"""
        self.status_frame = ttk.Frame(parent)
        self.status_frame.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        ttk.Separator(self.status_frame, orient="horizontal").grid(row=0, column=0, sticky="ew")
        self.status_label = ttk.Label(self.status_frame, text="就绪")
        self.status_label.grid(row=1, column=0, sticky="w", pady=(6, 0))

    # ----------------- 功能函数 -----------------

    def switch_mode(self):
        """切换查询/浏览模式的显示"""
        self.current_mode = self.mode_var.get()
        if self.current_mode == 'query':
            self.query_frame.grid()
            self.browse_frame.grid_remove()
            self.result_frame.grid()  # 显示查询结果模块
            self.status_label.config(text="查询模式 - 输入SQL语句执行查询")
        else:
            self.query_frame.grid_remove()
            self.browse_frame.grid()
            self.result_frame.grid_remove()
            self.status_label.config(text="浏览模式 - 选择表查看数据")
            # 切换到浏览模式时确保表列表是最新的
            try:
                self.refresh_table_list()
            except Exception:
                traceback.print_exc()

    def execute_sql(self):
        """执行 SQL（支持单条和多条）"""
        sql = self.sql_text.get("1.0", tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        try:
            self.status_label.config(text="正在执行SQL...")
            self.root.update_idletasks()

            # 预处理：按行分割、跳过以 -- 开头的注释
            lines = [ln.rstrip() for ln in sql.splitlines() if ln.strip() != ""]
            statements = []
            current = ""
            for ln in lines:
                if ln.strip().startswith("--"):
                    continue
                current += ln + " "
                if ln.strip().endswith(";"):
                    statements.append(current.strip())
                    current = ""
            if current.strip():
                statements.append(current.strip())

            # 执行
            if len(statements) == 1:
                result = self.db.execute_sql(statements[0])
                self.show_result(result)
                if getattr(result, "success", False):
                    self.status_label.config(text=f"执行成功 - {result.message}")
                else:
                    self.status_label.config(text=f"执行失败 - {result.message}")
            else:
                results = self.db.execute_batch(statements)
                self.show_batch_results(results)
                success_count = sum(1 for r in results if getattr(r, "success", False))
                self.status_label.config(text=f"批量执行完成: {success_count}/{len(results)} 成功")

        except Exception as e:
            traceback.print_exc()
            self.status_label.config(text=f"执行错误: {str(e)}")
            messagebox.showerror("错误", f"执行SQL时发生错误:\n{str(e)}")

    def show_result(self, result):
        """显示查询结果（分为文本信息 + 表格数据）"""
        # 清空文本和表格
        self.result_text.delete("1.0", tk.END)
        for item in self.result_table.get_children():
            self.result_table.delete(item)
        self.result_table["columns"] = ()

        if result.success:
            # 基本执行信息
            self.result_text.insert(tk.END, f"✓ 执行成功: {result.message}\n")
            self.result_text.insert(tk.END, f"影响行数: {getattr(result, 'affected_rows', 0)}\n")
            self.result_text.insert(tk.END, f"执行时间: {getattr(result, 'execution_time', 0):.3f}秒\n\n")

            # 如果有数据，显示在表格
            if result.data:
                # 确定列名
                if isinstance(result.data[0], dict):
                    col_names = list(result.data[0].keys())
                else:
                    # 如果返回的是元组/列表，就生成列名 col1, col2...
                    col_names = [f"col{i + 1}" for i in range(len(result.data[0]))]

                self.result_table["columns"] = col_names
                for col in col_names:
                    self.result_table.heading(col, text=col)
                    self.result_table.column(col, width=100, minwidth=50)

                # 插入行
                count = 0
                for row in result.data:
                    if not row:  # 跳过 None 或空行
                        continue
                    if isinstance(row, dict):
                        values = [row.get(col, "") for col in col_names]
                    else:
                        values = list(row)
                    self.result_table.insert("", "end", values=values)
                    count += 1

                self.result_text.insert(tk.END, f"返回 {count} 条记录\n")
            else:
                self.result_text.insert(tk.END, "(无数据返回)\n")
        else:
            self.result_text.insert(tk.END, f"✗ 执行失败: {result.message}\n")

    def show_batch_results(self, results):
        """显示多条语句的执行结果汇总"""
        self.result_text.delete("1.0", tk.END)
        success_count = sum(1 for r in results if getattr(r, "success", False))
        self.result_text.insert(tk.END, f"批量执行结果: {success_count}/{len(results)} 成功\n")
        self.result_text.insert(tk.END, "=" * 60 + "\n\n")
        for i, r in enumerate(results, 1):
            self.result_text.insert(tk.END, f"语句 {i}:\n")
            if getattr(r, "success", False):
                self.result_text.insert(tk.END, f"  ✓ 成功: {getattr(r, 'message', '')}\n")
                if getattr(r, "affected_rows", 0) > 0:
                    self.result_text.insert(tk.END, f"  影响行数: {r.affected_rows}\n")
                if getattr(r, "data", None):
                    self.result_text.insert(tk.END, f"  返回数据: {len(r.data)} 条记录\n")
            else:
                self.result_text.insert(tk.END, f"  ✗ 失败: {getattr(r, 'message', '')}\n")
            self.result_text.insert(tk.END, "\n")

    def clear_sql(self):
        self.sql_text.delete("1.0", tk.END)

    def show_examples1(self):
        """插入示例 SQL"""
        examples = """

CREATE TABLE users (
    id INT,
    name VARCHAR(50),
    age INT
);
CREATE TABLE orders (
    order_id INT,
    user_id INT,
    amount INT
);
INSERT INTO users VALUES (1, 'Alice', 25);
INSERT INTO users VALUES (2, 'Bob', 30);
INSERT INTO users VALUES (3, 'Charlie', 22);
INSERT INTO users VALUES (4, 'David', 28);
INSERT INTO users VALUES (5, 'Eve', 35);
INSERT INTO users VALUES (6, 'Frank', 40);
INSERT INTO users VALUES (7, 'Grace', 19);
INSERT INTO users VALUES (8, 'Hank', 33);
INSERT INTO users VALUES (9, 'Ivy', 27);
INSERT INTO users VALUES (10, 'Jack', 45);
INSERT INTO orders VALUES (101, 1, 300);
INSERT INTO orders VALUES (102, 2, 150);
INSERT INTO orders VALUES (103, 1, 200);
"""
        self.sql_text.delete("1.0", tk.END)
        self.sql_text.insert("1.0", examples.strip())

    def show_examples2(self):
            """插入示例 SQL"""
            examples = """
UPDATE users SET age = 26 WHERE name = 'Alice';
DELETE FROM users WHERE age < 28;
"""
            self.sql_text.delete("1.0", tk.END)
            self.sql_text.insert("1.0", examples.strip())

    def show_examples3(self):
            """插入示例 SQL"""
            examples = """
SELECT id, name, age FROM users;
"""
            self.sql_text.delete("1.0", tk.END)
            self.sql_text.insert("1.0", examples.strip())

    def show_examples4(self):
            """插入示例 SQL"""
            examples = """
SELECT users.id, users.name, orders.order_id, orders.amount
FROM users INNER JOIN orders  ON users.id = orders.user_id;
"""
            self.sql_text.delete("1.0", tk.END)
            self.sql_text.insert("1.0", examples.strip())

    def show_examples5(self):
        """插入示例 SQL"""
        examples = """
SELECT name, age FROM users ORDER BY age DESC, name ASC;
    """
        self.sql_text.delete("1.0", tk.END)
        self.sql_text.insert("1.0", examples.strip())

    def show_examples6(self):
        """插入示例 SQL"""
        examples = """
BEGIN;
UPDATE users SET age = 28 WHERE name = 'Alice';
COMMIT;

ROLLBACK;       """
        self.sql_text.delete("1.0", tk.END)
        self.sql_text.insert("1.0", examples.strip())

    def show_examples7(self):
        """插入示例 SQL"""
        examples = """
SELECT (1 + 2) * age AS score 
FROM users 
WHERE (age * 2) >= (10 + 5);       """
        self.sql_text.delete("1.0", tk.END)
        self.sql_text.insert("1.0", examples.strip())

    def show_examples8(self):
            """插入示例 SQL"""
            examples = """
SELECT COUNT(*) FROM users;      """
            self.sql_text.delete("1.0", tk.END)
            self.sql_text.insert("1.0", examples.strip())

    def insert_sql_template(self, template: str):
        """把模板追加到输入框末尾（保持可重复插入）"""
        if not template.endswith("\n"):
            template = template + "\n"
        self.sql_text.insert(tk.END, template)
        self.sql_text.see(tk.END)

    def refresh_table_list(self):
        """刷新数据库中的所有表名并更新下拉框"""
        try:
            tables = self.db.get_all_tables() or []
            # 确保是字符串列表
            tables = [str(t) for t in tables]
            self.table_combo['values'] = tables
            if tables:
                # 如果当前 table_var 为空，则选中第一个
                if not self.table_var.get():
                    self.table_combo.set(tables[0])
                    self.on_table_selected(None)
                else:
                    self.on_table_selected(self)
            else:
                self.table_combo.set('')
                self.table_info_label.config(text="数据库中没有表")
                self.clear_data_table()
        except Exception as e:
            traceback.print_exc()
            messagebox.showerror("错误", f"刷新表列表失败:\n{str(e)}")

    def on_table_selected(self, event):
        """表选择事件"""
        table_name = self.table_var.get()
        if not table_name:
            return
        self.current_table = table_name
        self.load_table_data(table_name)

    def load_table_data(self, table_name: str):
        """从数据库加载表信息与数据并显示"""
        try:
            table_info = None
            try:
                table_info = self.db.get_table_info(table_name)
            except Exception:
                # 有些实现没有 get_table_info，忽略
                table_info = None

            if table_info:
                info_text = f"表: {table_name} "
                self.table_info_label.config(text=info_text)
            else:
                self.table_info_label.config(text=f"表: {table_name}")

            # 获取表数据
            sql = f"SELECT * FROM {table_name}"
            result = self.db.execute_sql(sql)

            if getattr(result, "success", False) and getattr(result, "data", None):
                self.display_data_in_table(result.data, table_info.get('columns', []) if table_info else [])
            else:
                self.clear_data_table()
                if not getattr(result, "success", False):
                    self.table_info_label.config(text=f"加载数据失败: {getattr(result, 'message', '')}")
                else:
                    self.table_info_label.config(text=f"表 {table_name} 为空或无返回数据")
        except Exception as e:
            traceback.print_exc()
            messagebox.showerror("错误", f"加载表数据失败:\n{str(e)}")

    def display_data_in_table(self, data: List[Any], columns: List[Dict[str, Any]]):
        """在 Treeview 中展示表数据（兼容 dict 和 tuple/list）"""
        self.clear_data_table()
        if not data:
            return

        # 推断列名
        if columns:
            column_names = [c.get('name', f"col{i+1}") for i, c in enumerate(columns)]
        else:
            first = data[0]
            if isinstance(first, dict):
                column_names = list(first.keys())
            elif isinstance(first, (list, tuple)):
                column_names = [f"col{i+1}" for i in range(len(first))]
            else:
                # 兼容其它可迭代情况，直接把元素用 str 展示为一列
                column_names = ["value"]

        # 设置 Treeview 列和标题
        self.data_tree['columns'] = column_names
        self.data_tree['show'] = 'headings'
        for col in column_names:
            self.data_tree.heading(col, text=col)
            self.data_tree.column(col, width=120, minwidth=50, anchor="w")

        # 插入每一行
        for row in data:
            if isinstance(row, dict):
                values = [row.get(col, "") for col in column_names]
            elif isinstance(row, (list, tuple)):
                # 如果列名比元素多/少，尽量对齐
                values = list(row)[:len(column_names)]
                if len(values) < len(column_names):
                    values += [""] * (len(column_names) - len(values))
            else:
                values = [str(row)]
            try:
                self.data_tree.insert("", "end", values=values)
            except Exception:
                # 防御性：确保不会因为单条插入失败而崩溃
                traceback.print_exc()

    def clear_data_table(self):
        """清空 Treeview 内容与列"""
        for item in self.data_tree.get_children():
            self.data_tree.delete(item)
        try:
            self.data_tree['columns'] = ()
        except Exception:
            pass

    def run(self):
        """启动主循环"""
        self.root.mainloop()

    def __del__(self):
        """析构时关闭数据库连接（防御性）"""
        try:
            if hasattr(self, "db") and getattr(self.db, "close", None):
                self.db.close()
        except Exception:
            pass


# 可直接作为脚本运行
def main():
    try:
        app = MiniDBGUI()
        app.run()
    except Exception as e:
        print(f"启动GUI失败: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
