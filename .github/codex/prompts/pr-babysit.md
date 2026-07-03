You are maintaining this pull request until automated reviewers are satisfied.

Objectives:

- Inspect the pull request comments, review threads, and CI failures with `gh`.
- Address all actionable Codex review comments.
- Address all actionable Greptile comments until the Greptile result can reach 5/5.
- Keep changes scoped to the current pull request.
- Run the narrowest meaningful checks for the files you change.

Rules:

- Do not rewrite history.
- Do not merge the pull request.
- Do not change repository secrets, workflow credentials, or branch protection.
- Do not comment to trigger Greptile; Greptile reviews automatically on PR creation and new commits.
- Do not create, draft, ready, or otherwise change PR lifecycle state from this babysitter prompt.
- Operator trigger reference: open PRs as drafts with `gh pr create --draft`, then run `gh pr ready <pr#>` when Codex review should start; the connector reacts with eyes when review begins.
- Operator trigger reference: for a fresh Codex review of new commits, cycle draft/ready around the push with `gh pr ready <pr#> --undo`, push, then `gh pr ready <pr#>`.
- If the operator sees no eyes reaction within about 5 minutes of marking ready, stop and ask Adam whether Codex is down or the repo is not enabled. Never use trigger comments.
- Prefer small, reviewable fixes over broad refactors.
- If a reviewer comment is incorrect, leave a concise PR comment explaining why instead of changing unrelated code.
- Before finishing, summarize the files changed, checks run, and any reviewer comments that remain unaddressed.
