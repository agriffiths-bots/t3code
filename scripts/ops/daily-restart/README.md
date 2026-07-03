# Daily Restart Operations

## Verified Client Pointer

Client update agents poll branch `client-verified-latest` / file
`client-verified-latest.json`.

```bash
node scripts/ops/daily-restart/update-verified-pointer.ts --sha "$SHA"
```

Omit `--sha` for `origin/main`; pass `--dry-run` or `--out FILE`. Final stdout:
`VERIFIED <sha>` or `NOT-VERIFIED <reason>`. Failures exit non-zero and never
overwrite the previous pointer.

Gate: `Windows Launch Smoke` must conclude `success`, and a product release for
the SHA must contain Windows installer/blockmap/manifest assets. The pointer
uses a dedicated branch instead of release asset replacement so failed writes
leave the previous file commit in place.

Schema: `{"version":1,"sha","verified_at","desktop":{"ready":true,"artifacts":{"release","windows":{"installer","blockmap","manifest"}},"launch_smoke":"success","checks":{...},"linux":{"ready":"unknown","launch_smoke":"unknown","artifact":"unknown","reason"}},"mobile":{"ready":"unknown","ota":"unknown","apk"?:url,"reason"}}`.

Linux and mobile OTA are `unknown`: current workflows publish Windows/APK assets
but no main-queryable Linux release artifact or Expo OTA update.

Poll:

```bash
gh api 'repos/agriffiths-bots/t3code/contents/client-verified-latest.json?ref=client-verified-latest' --jq .content | base64 -d
```
