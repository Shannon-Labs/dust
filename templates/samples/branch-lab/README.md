# Branch Lab

This template is a small ledger-style project for testing branch, switch, and diff flows.

## Scaffold It

```sh
dust init branch-lab && cd branch-lab
cp -R ../dust/templates/samples/branch-lab/* .
dust query -f db/schema.sql
dust seed --profile demo
dust branch create promo-cut
dust branch switch promo-cut
```

From there, mutate the data and compare the result back to `main` with `dust diff main promo-cut`.
