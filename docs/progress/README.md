# Progress entries

One file per unit of work from `docs/kdb-open-issues-plan.md`, written in the same PR as the work:
what landed, the PR and commit, the evidence, and anything found along the way.

**Name:** `YYYY-MM-DD-<slug>.md`, e.g. `2026-10-12-f1-merge-base.md`. The date is when the PR was
opened; the slug names the unit (`u6c-...`, `n5-...`, `f1-...`).

**Why one file each:** until 2026-10-11 every PR appended to the progress log at the bottom of
`docs/kdb-open-issues-plan.md`. With several PRs open at once, the first to merge changed the end
of that file and every other open PR then conflicted on it — the only conflicts that whole series
of PRs ever had. A new file per entry never conflicts. The entries written before the change stay
where they were.

**Status lines** on each unit in the plan are still edited in place; keep that to the one line
naming the unit's PR, so two PRs rarely touch the same line.
