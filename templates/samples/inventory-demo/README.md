# Inventory Demo

This template is a quick path for evaluating Dust as a local inventory and replenishment workspace.

## Scaffold It

```sh
dust init inventory-demo && cd inventory-demo
cp -R ../dust/templates/samples/inventory-demo/* .
dust query -f db/schema.sql
dust seed --profile demo
dust codegen
dust query --format json -f db/queries/reorder_report.sql
```

## What It Exercises

- schema apply from repo files
- deterministic seed profiles
- JSON query output
- typed codegen over checked-in query files
- executable Rust/TypeScript helpers for parameterized queries like `products_by_category`

## Generated Helper Flow

After `dust codegen`, both generated files expose a small `DustClient` plus executable helpers.

Rust:

```rust
use db::generated::queries::{
    products_by_category, DustClient, ProductsByCategoryParams,
};

let client = DustClient::new(".");
let rows = products_by_category(
    &client,
    &ProductsByCategoryParams {
        category: "gadgets".to_string(),
    },
)?;
```

TypeScript:

```ts
import {
  DustClient,
  products_by_category,
} from "./db/generated/queries";

const client = new DustClient(".");
const rows = await products_by_category(client, { category: "gadgets" });
```
