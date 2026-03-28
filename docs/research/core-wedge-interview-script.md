# Core Wedge Interview And Onboarding Script

Use this for a 35-45 minute session. Read the prompts as written unless the participant needs clarification.

## Moderator Rules

- Do not pitch the product before you understand the participant's current workflow.
- Do not rescue them too early during the onboarding task.
- Ask for concrete examples, not general opinions.
- Capture exact quotes when the participant describes pain, delight, or distrust.

## 0. Session Setup (2 minutes)

Say:

> Thanks for doing this. I am trying to understand your real local database workflow, where it hurts, and whether Dust is actually useful for that job. I am not testing you. I am testing whether the product is solving a problem you genuinely have.

Confirm:

- recent local DB usage in a real project
- permission to take notes
- whether the session is interview-only or interview + onboarding

## 1. Current Workflow (8 minutes)

Ask, in order:

1. Tell me about the last time you needed a local database for development, testing, or an experiment.
2. What exact tools did you use?
3. Where did time or frustration show up?
4. Which parts are annoying but tolerated, and which parts actually break flow?
5. When you need a second branch, fixture, or isolated state, what do you do today?

Probe for:

- Docker startup and container management
- seeds/migrations drift
- fixture setup and teardown
- schema/codegen glue
- editor/agent/client connection friction
- branch or snapshot workarounds

## 2. Pain Severity (5 minutes)

Ask:

1. On a 1-5 scale, how painful is your current local DB setup?
2. Why is it that number and not one lower?
3. If that pain vanished tomorrow, what would improve in your actual week?

Record whether the pain is:

- speed / setup latency
- reliability / drift
- branching / isolation
- agent access / tooling integration
- cognitive overhead / too many moving parts

## 3. Live Product Intro (3 minutes)

Say:

> I am going to show you a local SQL workflow CLI that keeps database state in the repo, supports branching and snapshots, and exposes the same local state through pgwire or MCP. I want to see which part feels actually valuable to you.

Do not mention hosted/team/commercial ideas unless the participant asks.

## 4. Onboarding Task (12-15 minutes)

If you cannot run a live install in the session, narrate the commands and ask the participant what they expect to happen.

Use this path:

1. Install or confirm the binary.
2. Run `dust demo`.
3. Initialize a project.
4. Run a query.
5. Create a branch, switch, make a change, and run `dust diff`.
6. If relevant to the participant, show either pgwire or MCP.

Ask during the flow:

1. What feels clearer than your current setup?
2. What feels suspicious or unfinished?
3. At this moment, what would stop you from trying this in a real repo?

## 5. Hook Test (5 minutes)

Ask:

1. Which part is most compelling so far?
2. If you told a teammate one reason to care, what would it be?
3. Is the strongest hook:
   - zero-setup local loop
   - branchable state
   - agent/MCP workflow
   - something else

Force a choice. Do not accept "all of it" without a ranking.

## 6. Objections And Trust (5 minutes)

Ask:

1. What would make you hesitate to adopt this even if you liked the demo?
2. What proof would you need before trying it in a real project?
3. Which claims would you distrust if you saw them on the homepage?

Probe for:

- durability / correctness concerns
- Postgres compatibility skepticism
- branch/diff trust
- team adoption friction
- too many rough edges for daily use

## 7. Close (2 minutes)

Ask:

1. Would you try this in a real repo in the next two weeks?
2. If yes, for what exact workflow?
3. If no, what would need to change first?

End with:

> Thank you. I am going to turn this into evidence, not just vibes, and use it to decide whether the product story should narrow or double down.
