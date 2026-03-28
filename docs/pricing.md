# Pricing

Dust has three public packaging buckets. Only one of them is fully self-serve today.

## Free

The open-source CLI stays free.

- Local project init, query, shell, branch, snapshot, diff, merge, and import/export flows.
- Repo-local schema, lockfile, and migration workflow.
- MCP, pgwire, LSP alpha, and typed codegen surfaces that ship in the repository.

## Team Beta

The Team tier is an invite-only beta path, not a GA SaaS promise.

- Shared workflow polish around launch docs, examples, and support.
- Beta intake focused on teams that already feel the pain of local DB setup and branchable state.
- Manual onboarding, manual support, and manual plan changes during the beta phase.

## Enterprise

Enterprise is currently a design-partner conversation, not a product SKU with self-serve provisioning.

- Architecture, rollout, and support expectations scoped by direct conversation.
- Focus on teams that need a clear local-to-remote story, not generic seat-based SaaS packaging.

## Feature Matrix

| Surface | Free | Team beta | Enterprise |
|---|---|---|---|
| Local CLI and repo workflow | Included | Included | Included |
| pgwire / MCP / LSP alpha | Included | Included | Included |
| Public docs and sample templates | Included | Included | Included |
| Shared beta onboarding | No | Manual invite-only | Manual design-partner |
| Commercial support expectations | Community / best effort | Manual beta support | Directly scoped |
| Hosted automation claims | None | Limited beta-only | Case-by-case |

## Packaging Principles

- The CLI remains the public wedge and should stay broadly usable.
- Hosted/commercial claims should lag reality, not lead it.
- Pricing copy must distinguish what is real now from what is invite-only or roadmap.
