# RFC: Plugin Model and Extension Registry

## Goal

Define a public extension surface for Dust without turning the engine into an unsafe native plugin free-for-all.

## Principles

- Capability declarations must be explicit.
- Sandbox boundaries matter more than extension breadth.
- Extensions should compose with the local-first workflow rather than bypass it.
- Registry distribution must not imply binary ABI stability that the core project does not yet promise.

## Public Extension Points

The likely public surfaces are:

- MCP tools and resources
- CLI-oriented wrappers and generators
- Query/codegen annotations
- Wasm-based UDF or execution hooks where deterministic boundaries can be enforced

## Non-Goals

- Arbitrary in-process native plugins
- Hidden privileged hooks that bypass workspace or branch semantics
- An extension ecosystem before compatibility and safety constraints are written down

## Registry Shape

A future registry should record:

- Package identity and version
- Required capabilities
- Runtime target
- Signing/provenance metadata
- Human-readable scope and safety notes

The registry should be treated as a distribution layer over explicit extension points, not as proof that every extension API is frozen forever.
