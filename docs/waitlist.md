# Waitlist

The Team beta intake should qualify real adoption intent instead of collecting a vague email list. The public site now routes that intake through GitHub issue forms so submissions land in a repo-owned queue instead of disappearing into private inboxes.

## Capture These Fields

- Team or company name
- Contact name and email
- Current database stack
- Primary use case for Dust
- Approximate team size
- Urgency or evaluation timeline
- Whether the interest is Team beta or Enterprise design-partner oriented

## Routing Rules

- Team beta requests should land in a queue that can be triaged manually through the [Team beta intake form](https://github.com/Shannon-Labs/dust/issues/new?template=team_beta.yml).
- Enterprise conversations should route to a higher-touch follow-up path through the [Enterprise contact form](https://github.com/Shannon-Labs/dust/issues/new?template=enterprise_contact.yml).
- Intake copy should state plainly that the beta is invite-only.

## Current Repo Surface

- The marketing site includes a `/waitlist/` route with direct CTA routing into public intake forms.
- The support route documents the parallel paths for support, docs feedback, bugs, and product requests.
- The launch infrastructure doc still defines the environment contract for a future dedicated form or CRM endpoint.
- Until automated billing and CRM wiring exist, beta qualification remains a manual operator workflow with repo-visible submissions.
