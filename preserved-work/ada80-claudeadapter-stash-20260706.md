# ADA-80 ClaudeAdapter Stash Preservation

Source: `/home/adam/wt-ada80-stuck-running` stash `stash@{0}`.

Original stash message:
`ada80 rejected attempt: turn-id-free inline + deferred-to-stream-exit (gate-refused round 4); superseded by detached-flag plumbing per advisor 3de8f6d8`

This branch preserves the exact two-file stash diff off-box as a patch artifact. Applying it directly to current `origin/main` produced conflicts in both ClaudeAdapter files, so no product-code resolution was attempted in this backup pass.
