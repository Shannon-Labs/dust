#!/bin/bash
# Terminal demo: Dust end-to-end workflow
# Import CSV -> Query -> Branch -> Diff
#
# Run this script to see the workflow, or use asciinema to record it:
#   asciinema rec -c 'bash assets/benchmarks/terminal-demos/demo_workflow.sh' demo.cast
#
set -euo pipefail

DUST="${DUST:-target/release/dust}"
DEMO_DIR=$(mktemp -d)
trap "rm -rf $DEMO_DIR" EXIT

# Slow-print for demo effect (set FAST=1 to skip)
type_cmd() {
    if [ "${FAST:-0}" = "1" ]; then
        echo -e "\033[1;32m\$\033[0m $1"
    else
        echo -ne "\033[1;32m\$\033[0m "
        for (( i=0; i<${#1}; i++ )); do
            echo -n "${1:$i:1}"
            sleep 0.03
        done
        echo
        sleep 0.3
    fi
}

section() {
    echo
    echo -e "\033[1;34m--- $1 ---\033[0m"
    echo
    sleep 0.5
}

cd "$DEMO_DIR"

section "1. Initialize a Dust project"
type_cmd "dust init ."
$DUST init --force .
sleep 0.5

section "2. Create a table and insert data"
type_cmd 'dust query "CREATE TABLE users (id INTEGER, name TEXT, email TEXT, score INTEGER)"'
$DUST query "CREATE TABLE users (id INTEGER, name TEXT, email TEXT, score INTEGER)"
sleep 0.3

type_cmd 'dust query "INSERT INTO users VALUES (1, '\''Alice'\'', '\''alice@example.com'\'', 95), (2, '\''Bob'\'', '\''bob@example.com'\'', 82), (3, '\''Carol'\'', '\''carol@example.com'\'', 91), (4, '\''Dave'\'', '\''dave@example.com'\'', 77), (5, '\''Eve'\'', '\''eve@example.com'\'', 88)"'
$DUST query "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 95), (2, 'Bob', 'bob@example.com', 82), (3, 'Carol', 'carol@example.com', 91), (4, 'Dave', 'dave@example.com', 77), (5, 'Eve', 'eve@example.com', 88)"
sleep 0.5

section "3. Query the data"
type_cmd 'dust query "SELECT * FROM users ORDER BY score DESC"'
$DUST query "SELECT * FROM users ORDER BY score DESC"
sleep 0.5

type_cmd 'dust query "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rank FROM users"'
$DUST query "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rank FROM users"
sleep 0.5

section "4. Create a branch for experimentation"
type_cmd "dust branch create experiment"
$DUST branch create experiment
sleep 0.3

type_cmd "dust branch list"
$DUST branch list
sleep 0.3

type_cmd "dust branch switch experiment"
$DUST branch switch experiment
sleep 0.5

section "5. Modify data on the branch"
type_cmd 'dust query "INSERT INTO users VALUES (6, '\''Frank'\'', '\''frank@example.com'\'', 99)"'
$DUST query "INSERT INTO users VALUES (6, 'Frank', 'frank@example.com', 99)"
sleep 0.3

type_cmd 'dust query "SELECT count(*) FROM users"'
$DUST query "SELECT count(*) FROM users"
sleep 0.5

section "6. Compare branches"
type_cmd "dust branch diff main experiment"
$DUST branch diff main experiment
sleep 0.5

section "7. Switch back to main (unchanged)"
type_cmd "dust branch switch main"
$DUST branch switch main
sleep 0.3

type_cmd 'dust query "SELECT count(*) FROM users"'
$DUST query "SELECT count(*) FROM users"
sleep 0.5

echo
echo -e "\033[1;32mDone.\033[0m Branch isolation kept main at 5 rows while experiment has 6."
echo
