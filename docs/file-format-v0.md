# File Format v0

This bootstrap does not implement the storage engine yet, but it does lock the first visible storage contracts:

- workspace shape under `.dust/workspace`
- manifest-centric branch heads
- append-only WAL as the durability primitive
- checksums on persistent structures
- portable `.dustdb` and `.dustpack` names for flattened and bundled artifacts

The working assumption remains:

- page size defaults to 16 KiB
- manifests are immutable snapshots of reachable state
- WAL records are append-only and checksum-protected
- checkpoints produce immutable segments and advance GC horizons only when refs permit

The workspace layout is intentionally explicit:

- `refs/` holds branch and snapshot references
- `manifests/` holds immutable reachable-state descriptions
- `catalog/` holds versioned catalog blobs
- `wal/` holds append-only durability records
- `segments/` holds checkpointed payloads
- `tmp/` holds transient build and recovery data

That shape is enough for the current bootstrap to create and inspect a project without implying the storage engine is done. It also leaves room for later branch, merge, and pack work without rewriting the outer contract.

These contracts should remain unstable pre-1.0, but they should still be treated as repo-local design constraints.
