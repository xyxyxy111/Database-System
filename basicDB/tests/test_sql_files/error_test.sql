-- 错误测试SQL

-- 语法错误: 缺少分号
CREATE TABLE test(id INT)

-- 语义错误: 表不存在
SELECT id FROM nonexistent_table;

-- 语义错误: 列不存在  
CREATE TABLE employee(id INT, name VARCHAR(30));
SELECT id, salary FROM employee;
