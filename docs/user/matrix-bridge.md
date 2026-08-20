# Matrix Bridge

> The bridge is still rolling out. **Matrix bridge** appears in Settings → Connections only on a server that has it; if you do not see the section, that server does not support the bridge yet and nothing on this page applies to it.

Use this when you want one T3 Code thread to appear as a private encrypted chat on Matrix, including Beeper.

The bridge is one room and at most one owner thread per T3 environment. Mid-turn tool output, streaming fragments, and activity notices stay in T3. Matrix receives each completed assistant reply as a single plain-text message.

## What You Need

- A Matrix bot account on a homeserver that can invite you, reachable over HTTPS. Plain HTTP is accepted only for a homeserver on this machine (`localhost` or `127.0.0.1`). You do not log T3 into Beeper; the bot creates the room and invites your Matrix user ID.
- A T3 client with permission to manage access (`access:write`). If Connections will not let you configure the bridge or mint a pairing code, re-pair this T3 client from an administrative link, then try again.
- End-to-end encryption on the bot. If the encrypted Matrix library cannot load, the bridge stays unavailable and T3 otherwise keeps running.

## Connect the Bot

1. Open **Settings** → **Connections**.
2. In **Matrix bridge**, enter the bot homeserver URL, the bot access token, and your Matrix user ID (for Beeper, that is your Beeper MXID).
3. Connect. T3 creates a private invite-only encrypted room and invites you.
4. Join the room from Beeper or another Matrix client.
5. Create a Matrix pairing code from the same Connections subsection. The code is a one-time T3 credential with `orchestration:read` and `orchestration:operate` access because the paired room can start and steer turns; the bridge consumes it as proof and does not create an authorized T3 client.
6. Paste the raw code into the room. You should see a pairing-complete message. Invalid, expired, revoked, or already-used codes all get the same rejection, and the room stays locked.

The bot token is write-only. T3 will not show it again after you save it.

## Choose the Owner Thread

From a thread's context menu:

- **Bridge to Matrix** when nothing is bridged
- **Move Matrix bridge here** when another thread is the owner
- **Stop Matrix bridge** on the current owner

Moving ownership drops any in-progress reply on the previous thread. Unbridging is immediate for work that has not been sent. Archiving or deleting the owner thread also clears ownership; restoring the thread does not restore the bridge.

With no owner selected, Matrix messages do not start T3 turns, and T3 does not post to the room.

## Using the Room

After pairing, with an owner thread selected:

- Each completed T3 reply on that thread arrives once as plain text.
- A Matrix message you send starts a T3 turn. If a turn is already running, the same message steers it.
- Replies from the bot are ignored and do not create extra T3 turns.

Latency on a local machine is typically under five seconds each way, excluding the time the model spends working.

## Disconnect

**Disconnect** in Connections is the reverse of connect. It clears the bot configuration and owner, and stops all bridge activity. The Matrix room can remain; remove the bot or the invite yourself if you want the room gone.

Remote T3 clients can use the same settings and thread menu. They never receive the bot token.
