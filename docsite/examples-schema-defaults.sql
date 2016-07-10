CREATE TABLE buildings (
    id INTEGER PRIMARY KEY,
    name TEXT
);

CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT,
    building_id INTEGER REFERENCES buildings
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT,
    department_id INTEGER NOT NULL REFERENCES departments
);

INSERT INTO buildings (id, name) VALUES
    (1, 'London'),
    (2, 'Paris');

INSERT INTO departments (id, name, building_id) VALUES
    (1, 'Research', 1),
    (2, 'Accounting', NULL);

INSERT INTO employees (id, name, department_id) VALUES
    (1, 'John', 1),
    (2, 'Jane', 1),
    (3, 'Janet', 2);
