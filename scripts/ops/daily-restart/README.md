# Daily Restart Operations

## Verified Client Pointer

Client update agents poll release tag `client-verified-latest` / asset
`client-verified-latest.json`.

```bash
node scripts/ops/daily-restart/update-verified-pointer.ts --sha "$SHA"
```

Omit `--sha` for `origin/main`; pass `--dry-run` or `--out FILE`. Final stdout:
`VERIFIED <sha>` or `NOT-VERIFIED <reason>`. Failures exit non-zero and never
overwrite the previous pointer.

Gate: `Windows Launch Smoke` must conclude `success`, and a release for the SHA
must contain Windows installer/blockmap/manifest assets.

Schema: `{"version":1,"sha","verified_at","desktop":{"ready":true,"artifacts":{"release","windows":{"installer","blockmap","manifest"}},"launch_smoke":"success","checks":{...},"linux":{"ready":"unknown","launch_smoke":"unknown","artifact":"unknown","reason"}},"mobile":{"ready":"unknown","ota":"unknown","apk"?:url,"reason"}}`.

Linux and mobile OTA are `unknown`: current workflows publish Windows/APK assets
but no main-queryable Linux release artifact or Expo OTA update.

Poll:

```bash
gh release download client-verified-latest --repo agriffiths-bots/t3code --pattern client-verified-latest.json --clobber --output -
```
