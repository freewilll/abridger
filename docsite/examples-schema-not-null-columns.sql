CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT,
    department_id INTEGER REFERENCES departments
);

ALTER TABLE departments ADD COLUMN primary_employee_id INTEGER REFERENCES employees;

INSERT INTO departments (id, name) VALUES
    (1, 'Managers'),
    (2, 'Engineers');

INSERT INTO employees (id, name, department_id) VALUES
    (1, 'John', 1),
    (2, 'Jane', 2),
    (3, 'Janet', NULL);

UPDATE departments SET primary_employee_id=1 WHERE id=1;
