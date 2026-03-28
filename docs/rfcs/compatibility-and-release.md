# RFC: 1.0 Compatibility and Release Distribution

## Goal

Define what Dust can credibly promise at 1.0 and what remains explicitly unstable until then.

## Candidate Compatibility Surface

These are the formats most likely to need an eventual compatibility promise:

- `dust.lock`
- migration metadata
- schema fingerprints and object identity rules
- generated query artifact contracts
- workspace metadata and refs
- packed/exported artifact formats

## Pre-1.0 Policy

Before 1.0:

- Breakage is allowed when it materially improves the workflow.
- Format changes should be called out in release notes and migration docs.
- The repo should prefer written transition guidance over silent churn.

## 1.0 Direction

At 1.0, the project should freeze:

- lockfile schema
- migration metadata semantics
- generated artifact freshness checks
- package/distribution naming
- signed-binary and package-install expectations

## Release and Distribution Plan

The credible distribution story is:

- signed release artifacts attached to GitHub releases
- install script for direct binary download
- `cargo install dust-cli` for source-driven users
- documented platform matrix and checksum verification steps

The project should not claim stronger supply-chain guarantees than it actually enforces.
