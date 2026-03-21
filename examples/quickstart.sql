-- Dust quickstart: run with `dust query -f examples/quickstart.sql`

-- Create a schema
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    active INTEGER NOT NULL
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    author_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL
);

-- Insert sample data
INSERT INTO users (id, name, email, active) VALUES
    (1, 'Alice', 'alice@example.com', 1),
    (2, 'Bob', 'bob@example.com', 1),
    (3, 'Charlie', 'charlie@example.com', 0);

INSERT INTO posts (id, author_id, title, body) VALUES
    (1, 1, 'Getting started with Dust', 'Dust is a branchable SQL runtime...'),
    (2, 1, 'Schema identity explained', 'Every object gets a stable ID...'),
    (3, 2, 'Why I switched from Docker', 'I was tired of waiting 5 seconds...');

-- Query: find active users
SELECT id, name, email FROM users WHERE active = 1;

-- Query: update a user
UPDATE users SET name = 'ALICE' WHERE id = 1;

-- Query: delete inactive users
DELETE FROM users WHERE active = 0;

-- Query: final state
SELECT * FROM users
