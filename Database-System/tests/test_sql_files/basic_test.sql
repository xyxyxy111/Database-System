-- 基本功能测试SQL

-- 创建学生表
CREATE TABLE students(
    id INT,
    name VARCHAR(50),
    age INT
);

-- 插入测试数据
INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
INSERT INTO student(id, name, age) VALUES (2, 'Bob', 22);
INSERT INTO student(id, name, age) VALUES (3, 'Charlie', 19);

-- 查询测试
SELECT id, name FROM student WHERE age > 18;
SELECT id, name, age FROM student WHERE name = 'Alice';

-- 删除测试  
DELETE FROM student WHERE id = 1;
