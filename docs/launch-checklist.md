# Launch Checklist

## Preflight

- Release binaries are published and checksumed.
- Install script resolves to the intended release artifact.
- README, docs route, pricing, waitlist, FAQ, and benchmark links are live.
- Policy pages are reachable.
- `dust demo` and sample template smoke tests pass in CI.
- Benchmarks cited in public copy map back to committed source artifacts.

## Day-0 Smoke Tests

- Install from the public path on a clean machine.
- Run `dust demo`.
- Run the quickstart path from docs.
- Verify docs search and key marketing links.
- Verify the waitlist route and pricing page render correctly.

## Go / No-Go

Go only if:

- install path works
- docs path works
- policy surface exists
- sample templates still execute
- benchmark copy is backed by repo artifacts

If any of those fail, the launch is not ready.

## First 24 Hours

- Watch install attempts and docs traffic.
- Watch inbound beta/waitlist volume.
- Watch bug reports or support requests for repeated confusion.
- Keep rollback instructions ready for the site and docs deploy.
