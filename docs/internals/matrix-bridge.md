# Matrix bridge

> For maintainers. Using T3 Code? See [docs/user/matrix-bridge.md](../user/matrix-bridge.md).

The production bridge is a thin in-process reactor: one `MatrixBridgeReactor`, one versioned `ServerSecretStore` blob, one encrypted Matrix room, one owner pointer, and one adapter that hides `matrix-bot-sdk` plus the native crypto binding. There is no SQL migration, public HTTP API, extra auth scope, durable outbox, or unencrypted fallback.

## Placement

Configuration RPCs live on the existing WebSocket group:

- `matrixBridge.configure` / `matrixBridge.disconnect` require `access:write`
- `matrixBridge.setOwner` requires `orchestration:operate`
- `matrixBridge.subscribeStatus` requires `orchestration:read`

The access token is write-only. Status is lifecycle, owner thread id, encryption readiness, and a sanitized reason. Room id and allowed MXIDs are returned only on the privileged configure response.

`environment.capabilities.matrixBridge` is advertised only when pairing, inbound dispatch, and the encrypted adapter are present. No config means no native load and no Matrix fibers.

## Secret blob

`matrix-bridge-config` is a versioned JSON document: homeserver URL, access token, allowed MXIDs, room id, unpaired/paired pairing state, owner thread id, ownership epoch, and crypto-store generation. SDK sync state and the Rust crypto directory live beside the blob under the 0700 secrets directory. Changing homeserver, token, room, or allowlist rotates the crypto-store generation and resets room, pairing, and ownership.

HTTP homeservers are accepted only for literal loopback hosts.

## Outbound finals

Subscribe to the live domain stream. On each owner-thread `thread.message-sent` with assistant role and `streaming:false`, schedule the drainable worker. The worker re-reads the projection and sends only if that turn is terminal (`completed` / `interrupted` / `error`, or awareness phase `completed`), then drops stale-terminal cases where a newer user message exists after the terminal timestamp. Dedupe `(threadId, turnId)` runs after the terminal check. Never send `event.payload.text`; take the last matching non-streaming assistant message from the projection.

A turn with tool calls and mid-turn assistant segments therefore produces exactly one Matrix `m.text`. An approval-opening segment produces none.

Immediately before encrypt/send and before every retry, re-read owner and epoch. Moving ownership drops the previous owner's in-progress turn. Transaction ids are stable per environment/thread/turn.

## Inbound

Decrypted `m.text` from the paired user in the one room becomes `thread.turn.start` through `BootstrapTurnStartDispatcher`. Idle and mid-turn steering share that command. Bot, non-allowed, non-paired, wrong-room, historical, and duplicate events are ignored. Unexpected joined members pause outbound (`degraded`) rather than auto-kicking.

Pairing consumes a settings-minted one-time credential through an internal proof path that does not create an access-token session. Exact room messages:

- `T3 bridge is locked. Reply with a pairing code from T3 Settings > Connections.`
- `Pairing code rejected. It is invalid, expired, revoked, or already used.`
- `Pairing complete. T3 bridging is active when a thread is selected.`
- `Pairing could not be completed. Create a new code in T3 Settings > Connections and try again.`

## Crypto pin

Pin `@matrix-org/matrix-sdk-crypto-nodejs` exactly to `0.4.0` so upgrades cannot bump the native binding. `0.6.6` declares Node `>=24` while the server still supports 22 and 23. Load it only when config exists. Import/init failure publishes `unavailable` and leaves the rest of T3 running. There is no production plaintext mode.

The `0.4.0` binding wraps matrix-rust-sdk `0.9.0`, which is outside the `>= 0.12.0, < 0.16.1` range of GHSA-wfq4-36m3-9g42 (CVE-2026-45056, medium): that to-device sender-spoofing gap is in `sender_device_keys` handling this crate predates. Re-check that range, and any newer advisory, before moving the pin.

## Local E2E

`e2e/matrix-bridge.mjs` is a manual/release gate, not ordinary CI. It boots a pinned loopback conduwuit, two registered Matrix accounts, a tiny encrypted CLI client, and an ephemeral T3 home. `--smoke` runs the same bring-up and then proves the two accounts can hold an encrypted conversation without the bridge, so a red gate can be attributed to the bridge rather than the harness. Without `environment.capabilities.matrixBridge` the gate stops on its first check. See [e2e/README.md](../../e2e/README.md) and [the rollout runbook](../operations/matrix-bridge-rollout.md).
