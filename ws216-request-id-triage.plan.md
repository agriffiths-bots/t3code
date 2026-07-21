# PR 216 request-id triage plan

Intent: Preserve JSON-RPC request-id type for inbound extension requests, and quarantine string-typed peer responses before they can resolve numeric local extension requests. Keep the landing fix limited to `packages/effect-acp/src/protocol.ts` and focused tests in `protocol.test.ts`.

Blast radius: inbound raw-frame preparation, extension request dispatch/reply encoding, extension pending-response resolution (`Exit` and `Chunk`), JSON-RPC batch bookkeeping, and existing core-request alias/control routing. No provider, persistence, client API, migration, or restart-path behavior changes.

Edge-case matrix / red-first tests:

- String extension request ID and numeric-looking string extension request ID → `preserves string ids for inbound extension request replies` proves exact wire value/type restoration.
- String `Exit` matching a pending numeric extension ID → `drops a string-typed Exit that resembles a pending numeric extension request` proves the pending request remains live until the numeric response.
- String `Chunk` matching a pending numeric extension ID → `drops a string-typed Chunk that resembles a pending numeric extension request` proves no false unsupported-streaming failure.
- Reply before/after unrelated traffic, duplicate/replayed foreign response, crash-between-steps, same-timestamp ties, version skew → no new state or persistence; aliases remain request-lifetime scoped and foreign responses are idempotently dropped.
- Concurrent/batched core and extension requests → existing mixed-batch and alias-collision tests guard parser bookkeeping; run the full effect-acp protocol test file.

Smallest change: prepare every inbound request with an ID (not just configured core methods), reusing the existing alias/restoration machinery; move the existing string/non-integer quarantine ahead of pending-extension lookup for both terminal and streaming responses.
