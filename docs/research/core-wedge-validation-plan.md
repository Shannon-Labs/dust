# Core Wedge Validation Plan

This is the operator-ready plan for validating whether Dust solves a painful-enough problem for real developers to switch local database workflows.

## Goal

Answer three questions with direct evidence:

1. Which developer segment feels the local DB setup problem sharply enough to care?
2. Which Dust hook lands hardest in practice: zero-setup local loop, branchable state, or agent/MCP workflow?
3. Which objections stop adoption even after a successful first-run experience?

## Evidence Threshold

Do not make launch or roadmap decisions from one enthusiastic session.

- Minimum: 5 completed sessions across at least 2 target segments.
- Better: 8-10 sessions with at least 3 who do not convert emotionally.
- Each session must produce a filled evidence template in `docs/research/core-wedge-evidence-template.md`.

## Target Segments

Prioritize these in order:

1. Developers who regularly spin up Docker Postgres or SQLite + seed/migration glue for app development.
2. Engineers writing test fixtures or ephemeral branch environments for backend/data-heavy repos.
3. Agent-heavy developers who want a local DB surface accessible from pgwire or MCP.

Deprioritize for this round:

- Production database platform teams.
- BI/warehouse-first workflows.
- Buyers looking for hosted/team admin features first.

## Session Formats

Run one of these two formats per participant:

- Interview only: use `core-wedge-interview-script.md` sections 1-4, skip live product tasks.
- Interview + onboarding: run the full script, including the live workflow tasks.

Preferred default: interview + onboarding.

## Recruiting Criteria

Good participants:

- touched a local DB workflow in the last 30 days
- can describe current setup from memory
- are willing to screen-share or narrate commands
- are not already emotionally committed to Dust

Avoid:

- friends doing you a favor with no real problem to solve
- people whose only real need is hosted Postgres, auth, or enterprise controls
- people who have not recently felt the problem

## Artifacts To Use

- Interview/onboarding moderator guide: `docs/research/core-wedge-interview-script.md`
- Session note template: `docs/research/core-wedge-evidence-template.md`
- Synthesis memo template: `docs/research/core-wedge-decision-memo-template.md`

## Immediate Next Actions

1. Recruit 5 participants from waitlist/support/beta interest who currently use Docker Postgres or equivalent local DB glue.
2. Run 3 interview + onboarding sessions before changing launch copy again.
3. Fill one evidence template per participant on the same day as the session.
4. Write a first decision memo after session 5, even if the answer is "the wedge is weaker than we thought".
5. Only after that memo decide whether to lean into branching, agent workflows, or the zero-setup loop as the primary story.
