#!/bin/bash
# Terminal demo: CSV import workflow
#
# Run this script to see the workflow, or use asciinema to record it:
#   asciinema rec -c 'bash assets/benchmarks/terminal-demos/demo_import.sh' demo_import.cast
#
set -euo pipefail

DUST="${DUST:-target/release/dust}"
DEMO_DIR=$(mktemp -d)
trap "rm -rf $DEMO_DIR" EXIT

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

section "1. Initialize project"
type_cmd "dust init ."
$DUST init --force .
sleep 0.3

section "2. Generate sample CSV"
type_cmd "cat employees.csv"
cat <<'CSV' > employees.csv
id,name,department,salary
1,Alice,Engineering,120000
2,Bob,Marketing,95000
3,Carol,Engineering,115000
4,Dave,Sales,88000
5,Eve,Engineering,130000
6,Frank,Marketing,92000
7,Grace,Sales,97000
CSV
cat employees.csv
sleep 0.5

section "3. Import CSV into Dust"
type_cmd "dust import employees.csv"
$DUST import employees.csv
sleep 0.5

section "4. Explore the imported data"
type_cmd 'dust query "SELECT * FROM employees"'
$DUST query "SELECT * FROM employees"
sleep 0.5

type_cmd 'dust query "SELECT department, count(*) AS headcount FROM employees GROUP BY department"'
$DUST query "SELECT department, count(*) AS headcount FROM employees GROUP BY department"
sleep 0.5

section "5. Check project status"
type_cmd "dust status"
$DUST status
sleep 0.3

echo
echo -e "\033[1;32mDone.\033[0m CSV to queryable table in one command."
echo
