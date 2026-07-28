# Verified nightly alert bridge

GitHub Actions cannot call the host-local `wizzo-alert` delivery stack. The
`t3-verified-nightly-alert` one-shot poller reads completed `Verified Nightly
Promotion` runs and sends exactly one alert for each terminal run. Successful
alerts include the verified stable version; refusals include the failed job and
step. Its bounded delivered-run set advances only after delivery succeeds, so
an older run that completes late is not lost behind a newer run ID.

Install the user timer on Adam's instance:

```bash
install -m 0644 scripts/ops/verified-nightly/systemd/t3-verified-nightly-alert.service \
  ~/.config/systemd/user/t3-verified-nightly-alert.service
install -m 0644 scripts/ops/verified-nightly/systemd/t3-verified-nightly-alert.timer \
  ~/.config/systemd/user/t3-verified-nightly-alert.timer
systemctl --user daemon-reload
systemctl --user enable --now t3-verified-nightly-alert.timer
```

The service executes the script from `~/t3code`, so the instance checkout must
be deployed to the commit that introduced the workflow before enabling it.
