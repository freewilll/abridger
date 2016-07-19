CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT,
    department_id INTEGER NOT NULL REFERENCES departments
);

ALTER TABLE employees ADD COLUMN boss_id INTEGER REFERENCES employees;

CREATE TABLE addresses (
    id INTEGER PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES employees,
    details TEXT
);

INSERT INTO departments (id, name) VALUES
    (1, 'Managers'),
    (2, 'Engineers');

INSERT INTO employees (id, name, department_id, boss_id) VALUES
    (1, 'John', 1, NULL),
    (2, 'Jane', 2, 1),
    (3, 'Janet', 2, 2);

INSERT INTO addresses (id, employee_id, details) VALUES
    (1, 1, 'John''s address'),
    (2, 2, 'Jane''s adddress'),
    (3, 3, 'Janet''s first address'),
    (4, 3, 'Janet''s second address');
