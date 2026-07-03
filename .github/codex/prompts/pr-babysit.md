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
- Codex reviews start automatically on push; for a fresh review of new commits, push the branch again, using an empty commit only when the current HEAD needs a new review without code changes.
- If no eyes reaction or review appears within about 5 minutes of the push, stop and ask Adam whether Codex is down or the repo is not enabled.
- Prefer small, reviewable fixes over broad refactors.
- If a reviewer comment is incorrect, leave a concise PR comment explaining why instead of changing unrelated code.
- Before finishing, summarize the files changed, checks run, and any reviewer comments that remain unaddressed.
