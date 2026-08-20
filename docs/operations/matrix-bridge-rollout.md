# Matrix bridge rollout

> For maintainers. Using T3 Code? See [docs/user/matrix-bridge.md](../user/matrix-bridge.md).

## Sequence

1. Land the reactor-startup restore first. Confirm scheduled-task and child-thread coordinators each log one start.
2. Land configuration contracts and owner RPCs with the capability still unadvertised.
3. Land the fake-client reactor, then the encrypted adapter, then pairing/inbound/capability advertisement, only after native package gates pass.
4. Land web/desktop Connections and thread-menu controls.
5. Run the disposable local E2E gate, then the production-room verification below.

Rollback is `matrixBridge.disconnect`, then a server without the advertised capability if needed. There is no schema migration or room-history import to undo.

## Local E2E gate

From the repo root, on Linux, never against live T3 state:

```sh
node e2e/matrix-bridge.mjs            # release gate: the full bridge flow
node e2e/matrix-bridge.mjs --smoke    # self-check: the harness itself
```

The script downloads a pinned conduwuit `v0.5.0-rc4` static musl binary into `~/.cache/t3-matrix-e2e`, copies it into a temp root, and never reads or writes a live T3 home or an existing Matrix deployment. T3 boots with `T3CODE_HOME=<tmp>` `T3CODE_NO_BROWSER=1` on a loopback port in 13910–13940.

The release gate requires a server that advertises `environment.capabilities.matrixBridge`; against one that does not, it stops on the first check and says so rather than failing somewhere confusing. Run the self-check first when the gate is red: it exercises the homeserver, both registrations, the encrypted client, and the T3 boot without involving the bridge, so it separates a harness fault from a bridge fault.

This is a release/manual gate. Mid-turn proof uses a real installed Codex (or other authenticated) provider. Unit and integration tests stay on `TestProviderAdapter` and a fake Matrix transport.

Progress: `/tmp/t3-matrix-e2e.progress`. Failure keeps redacted logs at `/tmp/t3-matrix-e2e-failure.log`. Both outcomes delete the temp root, which holds Matrix access tokens, a crypto store, and a T3 home.

## Production-room verification

On the deployment artifact:

1. Native-import smoke on Linux x64 and a packaged-server start with Matrix unconfigured. Startup must not load crypto or change idle behavior.
2. `server.getConfig` advertises the Matrix capability. Connections shows the subsection only on the upgraded environment.
3. Configure the bot. Inspect the room: invite join rule, Megolm `m.megolm.v1.aes-sha2`, bot plus the allowed MXID only, no public alias.
4. Send an invalid pairing code, then a fresh Settings-minted code. Check the exact responses, one-time consumption, and no new Authorized Client session.
5. Select a disposable thread. Record one outbound final and one inbound idle turn. Each bridge hop must be under five seconds excluding model execution.
6. During a running turn, send a Matrix steer onto the same T3 turn. On a separate running turn, move ownership and confirm the old final is dropped. Unbridge and confirm silence.
7. Archive the owner and confirm owner status becomes null. Messages from the bot and from a non-allowed sender must not dispatch T3 turns.
8. Block the homeserver briefly, restore it, and confirm one queued final arrives once.
9. Resource gate: capture process telemetry before config, after crypto warm-up, after 15 minutes idle `/sync`, after 20 turns, and after disconnect/reconnect. Block rollout if steady-state RSS rises by more than 100 MiB or grows monotonically across three idle windows.
10. Logs and traces may contain lifecycle, queue depth, retry count, and sanitized errors. Fail verification if a Matrix token, pairing code, or message body appears.

## Configuration scopes

- `access:write` for configure, disconnect, and minting pairing codes. Do not grant this to the Matrix bot.
- `orchestration:operate` to set, move, or unset the owner.
- `orchestration:read` for status; a Matrix pairing code has both `orchestration:read` and `orchestration:operate` because the paired room can start and steer turns.
