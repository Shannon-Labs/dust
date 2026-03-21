# Benches

Dust benchmarks should eventually cover three categories:

- workflow operations such as `init`, `generate`, `branch`, and `snapshot`
- OLTP-ish reads and writes over the row-store path
- analytics-lite queries that can use covering or columnar indexes

This directory exists now so those benchmark contracts can be tracked as the engine lands.

