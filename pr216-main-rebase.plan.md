# PR 216 Current-Main Rebase Plan

Intent: rebase the existing PR 216 bulkhead commits onto `origin/main` at `c419ead21` or newer without changing product behavior. Preserve both the provider-isolation guarantee and main's newer ACP/string-ID and undecodable-notification handling.

Blast radius: conflict resolution may touch only the PR's existing provider reactor/service/reaper files, `packages/effect-acp`, their focused tests, and this plan; persistence schemas, public contracts, clients, migrations, and unrelated upstream-sync files stay unchanged.

Edge-case proof matrix: provider A hangs/fails while provider B starts -> existing `ProviderCommandReactor` wedged-start regression; reaper races a replacement -> existing `ProviderSessionReaper` identity-fence regressions; numeric/string ACP IDs coexist and notifications fail decoding in either ordering -> existing `effect-acp` protocol regressions from PRs 216, 215, and 219. No new red-first test is warranted for a history-only rebase; any dropped behavior should make one of these named existing suites fail.

Smallest change: replay the four existing commits, resolve conflicts textually from the code and tests, and make only the minimum compatibility edit required by a legitimate main-side change; defer any genuine behavior choice instead of guessing.
