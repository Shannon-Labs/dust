# Inventory Demo

This template is a quick path for evaluating Dust as a local inventory and replenishment workspace.

## Scaffold It

```sh
dust init inventory-demo && cd inventory-demo
cp -R ../dust/templates/samples/inventory-demo/* .
dust query -f db/schema.sql
dust seed --profile demo
dust query --format json -f queries/reorder_report.sql
```

## What It Exercises

- schema apply from repo files
- deterministic seed profiles
- JSON query output
- typed codegen over checked-in query files
