CREATE TABLE test1 (
    id SERIAL PRIMARY KEY
);

CREATE TABLE test2 (
    id SERIAL PRIMARY KEY REFERENCES test1
);

CREATE TABLE test3 (
    id SERIAL PRIMARY KEY,
    test2_id INTEGER NOT NULL REFERENCES test2
);

CREATE TABLE test4 (
    id SERIAL PRIMARY KEY,
    test2_id INTEGER REFERENCES test2
);
