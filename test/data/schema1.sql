CREATE TABLE offices (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE people (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE user_states (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO user_states (name) VALUES
    ('active'),
    ('disabled');

CREATE TABLE users (
    id SERIAL PRIMARY KEY REFERENCES people,
    state INT NOT NULL REFERENCES user_states,
    office INT REFERENCES offices,
    login TEXT UNIQUE
);

CREATE TABLE groups (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE user_groups (
    user_id INT NOT NULL REFERENCES users,
    group_id INT NOT NULL REFERENCES groups,
    UNIQUE(user_id, group_id)
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE services (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    customer INT NOT NULL REFERENCES customers,
    created_by INT NOT NULL REFERENCES users,
    UNIQUE(customer, name)
);

CREATE TABLE servers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    service INT NOT NULL REFERENCES services,
    parent INT REFERENCES servers,
    created_by INT NOT NULL REFERENCES users,
    UNIQUE(name)
);

CREATE TABLE pollers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    server INT NOT NULL REFERENCES servers,
    created_by INT NOT NULL REFERENCES users,
    UNIQUE(name),
    UNIQUE(server)
);

ALTER TABLE services ADD COLUMN poller INT REFERENCES pollers;
