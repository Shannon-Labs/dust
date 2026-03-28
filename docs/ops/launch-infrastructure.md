# Launch Infrastructure

## Hosting Targets

- Marketing and docs site: static hosting with preview deploy support
- GitHub repository: canonical code, releases, benchmark artifacts, issue/support routing

## Deployment Contract

The repo now contains:

- `apps/www` as the static site root
- `cargo run -p xtask -- site` to generate the docs route from markdown
- GitHub workflows for CI and site deployment

## DNS and Domains

- Primary public domain should point at the marketing/docs site
- Preview environments should use host-provided preview URLs
- GitHub releases remain under the repository domain

## Secrets Contract

The launch-facing surfaces should expect only a small set of secrets:

- analytics provider key, if enabled
- waitlist form endpoint or token, if enabled
- optional billing webhook secret, when billing is wired

If those are absent, the site copy should remain honest and degrade to documentation-only behavior.

## Manual vs Repo-Owned Work

Repo-owned:

- static site files
- docs generation
- CI and deploy workflows
- policy, FAQ, and launch documentation

Manual operator work:

- DNS changes
- host account setup
- analytics account setup
- billing account setup
