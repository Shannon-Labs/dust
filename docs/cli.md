# CLI Reference

Dust ships one CLI with the following command groups.

## Core

- `dust init [path]`
- `dust query <sql> | -f <path>`
- `dust explain <sql> | -f <path>`
- `dust shell`
- `dust status`
- `dust version`

## State and Workflow

- `dust branch create|list|current|switch|delete|diff`
- `dust snapshot create|checkout|delete|list`
- `dust diff [from] [to]`
- `dust merge preview|execute|resolve`
- `dust remote push|pull`

## Project Health

- `dust doctor`
- `dust lint`
- `dust migrate plan|apply|status|replay`
- `dust codegen [--lang rust|typescript]`
- `dust test`
- `dust bench`
- `dust deploy`

## Data Movement

- `dust import ...`
- `dust export ...`
- `dust seed [--profile <name>]`

## Integrations

- `dust dev [--profile <name>] [--serve]`
- `dust serve`
- `dust mcp`
- `dust lsp`
- `dust demo`

## Notes

- `dust query --format json` is the stable thin-client path for wrappers and SDKs.
- `dust dev` watches schema, query, and seed files, reruns codegen, and can optionally expose pgwire.
- `dust branch` and `dust diff` are explicit about the current row-count-based diff behavior.
