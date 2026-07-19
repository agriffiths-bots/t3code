# Decisions inbox

## PR #208: surface-relay rollout and desktop preview partition

The factory gate found two P1 integration constraints after the hosted same-origin relay was implemented and tested:

1. Desktop HTML/PDF previews load in a per-environment `persist:t3code-preview-*` Electron partition. A binding POST made by the app renderer stores the cookie in the wrong cookie jar, so the preview receives a masking 404.
2. `surfaceCredential` cannot become a required result field while mobile and remote clients may update independently. New clients must decode old results, while old clients cannot consume a capability-bound relay URL without a rollout signal.

Options:

1. **Explicit fail-closed capability negotiation (recommended).** Make the result credential optional for old-server decoding; add a `same-origin-relay-v1` request capability; have new servers return an explicit upgrade-required asset error to clients that do not advertise it. Add a desktop IPC operation that performs the bind POST through the target environment's preview `Session`, then navigates only after the partition owns the cookie. This preserves the ruling, but private assets intentionally require a client update when the server is newer.
2. **Coordinated minimum-version rollout.** Land optional decoding and the desktop partition bridge, but keep relay issuance disabled behind a negotiated server flag until all supported clients advertise `same-origin-relay-v1`. This preserves compatibility and security but needs release orchestration and delays enforcement.
3. **Legacy grace path.** Serve old clients unbound private signed URLs until a deadline. This offers the smoothest skew behavior but violates the ruling's no-bypass and cross-surface replay requirements, so it is not recommended and should remain rejected.

Recommendation: choose option 1 unless product policy requires old-client private-asset availability during skew; in that case choose option 2. Do not choose option 3.
