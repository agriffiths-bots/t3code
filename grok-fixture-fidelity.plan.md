# Grok recorded-fixture fidelity plan

## Intent

Add a recorded Grok 0.2.93 ACP PONG fixture whose first-turn wire ordering matches the settled live probe: assistant deltas `P`, `ONG`, then xAI prompt completion. Replay it through the real Grok adapter and ProviderService with session start isolated in a short-lived caller, proving the notification consumer survives through the first turn without changing production behavior.

## Proof and blast radius

- `apps/server/src/provider/Layers/GrokAdapter.ts:943-948` owns the ACP notification consumer; #224 changed `forkChild` to `forkIn(sessionScope)` at line 947 so it outlives the start caller.
- Live probe `d54bb5b7-afea-4426-af23-bf3cadfd4530` recorded Grok 0.2.93 deltas `P` then `ONG`, followed by `_x.ai/session/prompt_complete`; the settled turn had `turnCount=1`.
- Direct test surface: the recorded JSONL fixture, its deterministic replay peer, and a ProviderService/Grok adapter E2E test. Indirect consumers are ACP decoding, Grok event mapping, ProviderService fan-out/session routing, adapter `readThread`, and session-scope teardown.
- Persistence projections, migrations, clients, quarantine flags, provider settings, and all production provider/ACP code are out of scope.

## Edge-case matrix

| Case                                      | Named red-first proof                                                     | Expected result                                                              |
| ----------------------------------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Start returns before first prompt         | `replays a recorded Grok first turn after the session start caller exits` | RED with pre-#224 `forkChild`; GREEN with first delta and completion         |
| Live notification order                   | Same test replays `P`, `ONG`, then xAI prompt completion                  | ProviderService emits both deltas in order before `turn.completed`           |
| Prompt fallback races RPC                 | Fixture leaves the standard prompt RPC pending after xAI completion       | The fallback settles only after the event barrier drains                     |
| Turn history                              | Same test reads the adapter thread after completion                       | Turn history length is greater than zero (`turnCount > 0`)                   |
| Duplicate/replay or same-timestamp frames | Not introduced by the captured first-turn contract                        | Existing ACP parser/adapter coverage remains authoritative                   |
| Crash, version skew, concurrency          | Outside this fixture-only slice                                           | Existing process-death, schema, and provider bulkhead tests remain unchanged |

## Smallest change

Use one redacted recorded JSONL capture plus a small request-id-aware replay executable, then add one focused E2E test file that composes the existing Grok adapter and ProviderService layers. Reusing production layers makes the fixture exercise the lifetime boundary while avoiding any production edit or broad orchestration fixture refactor.
