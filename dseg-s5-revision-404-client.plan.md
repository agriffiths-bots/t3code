# DSEG slice 5 revision 404 client plan

## Intent

Treat the revision endpoint's typed `thread_not_found` response as definitive evidence that the cached thread is gone, while retaining cached state for all transient revision failures. Reuse the thread state's existing deleted/cache-eviction terminal state and stop its revision polling once that terminal outcome is reached.

## Blast radius and smallest change

- `packages/client-runtime/src/state/threadSnapshotHttp.ts`: add a `gone` revision-loader result and map only `EnvironmentResourceNotFoundError` to it; web and mobile consume this shared layer unchanged.
- `packages/client-runtime/src/state/threads.ts`: the sole production consumer reconciles `gone` through the existing `setDeleted` state/cache path, clears unresolved reconciliation cursors, logs loudly, and becomes ineligible for further polling.
- `packages/client-runtime/src/state/threads-sync.test.ts`: extend the loader harness and prove cached eviction, transient retention, and terminal polling behavior.
- Server projection readers, HTTP contracts, schemas, migrations, persistence formats, restart/resume inputs, and server tests are unchanged. Cached restart state is removed through the existing cache API, so a remount cannot resurrect the gone thread.
- This is the smallest owner-boundary change: one new internal result kind plus one terminal consumer branch; no server marker, protocol, persistence, projection, or UI contract changes.

## Red-first edge-case matrix

| Edge case                                                     | Named red-first test                                                                                        | Expected result                                                                                                            |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Revision 404 before a missed delete/audience-revocation event | `reconciles a cached thread away when its revision endpoint reports gone`                                   | State becomes deleted, cache entry is removed once, no detail request is made, and later clock advances do not poll again. |
| Delete event before revision 404                              | Existing `removes cached data when the thread is deleted` plus the new terminal-poll assertion              | Existing event path remains idempotently deleted and does not need the revision endpoint.                                  |
| Transient/network revision failure                            | `retains a cached thread while its revision endpoint is unavailable`                                        | Cached/live state remains visible, cache is not removed, the unresolved check backs off and retries.                       |
| Duplicate/replayed 404                                        | New gone test advances multiple polling windows after the first result                                      | No hot loop and no duplicate cache removal because terminal state is no longer eligible.                                   |
| Crash/restart between state deletion and cache removal        | Existing `setDeleted` ordering (state first, cache removal second) plus cache-error catch; no schema change | Current mount stays deleted; a failed cache removal is loudly logged and may conservatively reappear only after remount.   |
| Same-timestamp tie                                            | Not applicable: reconciliation uses typed outcome/state, not timestamps                                     | 404 wins deterministically regardless of event timestamps.                                                                 |
| Version skew: old server/network/undeclared failure           | Transient-unavailable test                                                                                  | Any response other than the declared typed not-found error retains cached data and follows bounded backoff.                |
| Concurrent revision 404 and live stream frame                 | Existing `applyLock` serializes deletion with event application; gone branch uses it                        | Whichever enters the lock first is ordered, while definitive gone leaves the terminal deleted state.                       |

## Verification

Run the focused client-runtime sync tests red before implementation and green after it, then the existing server revision/audience tests, `vp check`, `vp run typecheck`, and the factory autoreview gate.
