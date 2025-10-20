CREATE DATABASE test_company;
   USE test_company;

   CREATE TABLE employees (
       id INT PRIMARY KEY AUTO_INCREMENT,
       name VARCHAR(100),
       department VARCHAR(50),
       salary DECIMAL(10, 2),
       hire_date DATE
   );

   CREATE TABLE departments (
       id INT PRIMARY KEY AUTO_INCREMENT,
       name VARCHAR(50),
       manager VARCHAR(100)
   );

   -- Insert sample data
   INSERT INTO employees (name, department, salary, hire_date) VALUES
   ('John Doe', 'Engineering', 75000.00, '2020-01-15'),
   ('Jane Smith', 'Marketing', 65000.00, '2019-03-20'),
   ('Bob Johnson', 'Engineering', 80000.00, '2018-06-10'),
   ('Alice Williams', 'HR', 60000.00, '2021-02-28');

   INSERT INTO departments (name, manager) VALUES
   ('Engineering', 'Sarah Connor'),
   ('Marketing', 'John Marketing'),
   ('HR', 'Emily HR');

   EXIT;