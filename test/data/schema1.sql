CREATE TABLE offices (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE people (
    id SERIAL PRIMARY KEY
);

CREATE TABLE user_states (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO user_states (id, name) VALUES
    (DEFAULT, 'active'),
    (DEFAULT, 'disabled');

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    state INT NOT NULL REFERENCES user_states,
    office INT REFERENCES offices
);

ALTER TABLE users ADD CONSTRAINT users_people_fkey FOREIGN KEY (id) REFERENCES people;

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
    name TEXT NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    customer INT NOT NULL REFERENCES customers,
    created_by INT NOT NULL REFERENCES users
);

CREATE TABLE services (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    customer INT NOT NULL REFERENCES customers,
    created_by INT NOT NULL REFERENCES users
);

CREATE TABLE servers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    service INT NOT NULL REFERENCES services,
    parent INT REFERENCES servers,
    poller INT REFERENCES servers,
    created_by INT NOT NULL REFERENCES users
);

CREATE TABLE pollers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    server INT NOT NULL REFERENCES servers,
    created_by INT NOT NULL REFERENCES users
);
