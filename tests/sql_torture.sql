-- DDL
CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, active INTEGER);
CREATE TABLE IF NOT EXISTS t1 (id INTEGER);
DROP TABLE t1;
DROP TABLE IF EXISTS t1;
CREATE TABLE t2 (a INTEGER, b TEXT, c INTEGER, d TEXT, e INTEGER);

CREATE TABLE constrained (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    score INTEGER DEFAULT 0,
    team_id INTEGER REFERENCES teams(id)
);

CREATE INDEX idx_name ON t1 (name);
CREATE UNIQUE INDEX idx_email ON t1 (name, age);
DROP INDEX idx_name;

ALTER TABLE t1 ADD COLUMN bio TEXT;
ALTER TABLE t1 DROP COLUMN bio;
ALTER TABLE t1 RENAME COLUMN name TO full_name;
ALTER TABLE t1 RENAME TO users;

-- DML INSERT
INSERT INTO t1 (id, name, age, active) VALUES (1, 'alice', 30, 1);
INSERT INTO t1 (id, name) VALUES (2, 'bob');
INSERT INTO t1 VALUES (3, 'charlie', 25, 1);
INSERT INTO t1 (id, name, age, active) VALUES (4, 'dave', 40, 0), (5, 'eve', 35, 1);
INSERT INTO t1 (id, name) VALUES (6, '');
INSERT INTO t1 (id, name) VALUES (7, 'O''Brien');
INSERT INTO t1 (id, name) VALUES (8, NULL);

-- DML SELECT
SELECT 1;
SELECT 1 + 2;
SELECT * FROM t1;
SELECT id, name FROM t1;
SELECT DISTINCT name FROM t1;

SELECT * FROM t1 WHERE id = 1;
SELECT * FROM t1 WHERE id != 1;
SELECT * FROM t1 WHERE id <> 1;
SELECT * FROM t1 WHERE id > 3;
SELECT * FROM t1 WHERE id >= 3;
SELECT * FROM t1 WHERE id < 3;
SELECT * FROM t1 WHERE id <= 3;
SELECT * FROM t1 WHERE name = 'alice';
SELECT * FROM t1 WHERE active = 1 AND age > 25;
SELECT * FROM t1 WHERE active = 1 OR age > 35;
SELECT * FROM t1 WHERE NOT active = 0;
SELECT * FROM t1 WHERE (active = 1 AND age > 25) OR name = 'dave';

SELECT * FROM t1 WHERE age IS NULL;
SELECT * FROM t1 WHERE age IS NOT NULL;

SELECT * FROM t1 WHERE id IN (1, 3, 5);
SELECT * FROM t1 WHERE id NOT IN (1, 3, 5);
SELECT * FROM t1 WHERE age BETWEEN 25 AND 35;
SELECT * FROM t1 WHERE age NOT BETWEEN 25 AND 35;
SELECT * FROM t1 WHERE name LIKE 'a%';
SELECT * FROM t1 WHERE name LIKE '%li%';
SELECT * FROM t1 WHERE name LIKE '_lice';
SELECT * FROM t1 WHERE name NOT LIKE 'a%';

SELECT * FROM t1 ORDER BY name ASC;
SELECT * FROM t1 ORDER BY age DESC;
SELECT * FROM t1 ORDER BY active DESC, name ASC;

SELECT * FROM t1 LIMIT 3;
SELECT * FROM t1 LIMIT 3 OFFSET 2;
SELECT * FROM t1 ORDER BY id LIMIT 2 OFFSET 1;

SELECT count(*) FROM t1;
SELECT count(age) FROM t1;
SELECT sum(age) FROM t1;
SELECT avg(age) FROM t1;
SELECT min(age) FROM t1;
SELECT max(age) FROM t1;
SELECT min(name) FROM t1;
SELECT max(name) FROM t1;

SELECT id, age + 10 FROM t1;
SELECT id, name || ' (active)' FROM t1 WHERE active = 1;
SELECT id, -age FROM t1;

SELECT id AS user_id, name AS user_name FROM t1;

SELECT lower(name) FROM t1;
SELECT upper(name) FROM t1;
SELECT coalesce(age, 0) FROM t1;

-- DML UPDATE
UPDATE t1 SET name = 'ALICE' WHERE id = 1;
UPDATE t1 SET age = age + 1 WHERE active = 1;
UPDATE t1 SET active = 0;
UPDATE t1 SET name = 'new', age = 99 WHERE id = 1;

-- DML DELETE
DELETE FROM t1 WHERE id = 1;
DELETE FROM t1 WHERE active = 0;
DELETE FROM t1;

-- Transactions
BEGIN;
COMMIT;
ROLLBACK;

-- Multi-statement batch
CREATE TABLE batch_test (x INTEGER); INSERT INTO batch_test (x) VALUES (1), (2), (3); SELECT * FROM batch_test; DROP TABLE batch_test;

-- Error cases
SELECT * FROM nonexistent_table;
INSERT INTO nonexistent_table (x) VALUES (1);
UPDATE nonexistent_table SET x = 1;
DELETE FROM nonexistent_table;
CREATE TABLE t1 (id INTEGER); CREATE TABLE t1 (id INTEGER);
SELECT nonexistent_column FROM t1;
INSERT INTO t1 (nonexistent) VALUES (1);
INSERT INTO t1 (id) VALUES (1, 2);
SELECT * FROM t1 WHERE;
SELECT * FROM;
SELECT;
