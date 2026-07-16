# CI quality gates

- `.github/workflows/ci.yml` runs `vp check` (lint + typecheck), `vpr typecheck`, and `vp run test`, plus desktop build verification, desktop launch smoke, and release smoke on pull requests and pushes to `main`.
- `.github/workflows/release.yml` builds the Windows (`x64`) desktop artifact from a single `v*.*.*` tag and publishes one GitHub release.
- Mobile app builds are deprecated in this fork; CI and release workflows do not run Expo prebuild, EAS APK builds, or mobile native static analysis.
- The release workflow auto-enables Windows signing only when Azure Trusted Signing credentials are present. Without the signing credentials, it still releases unsigned artifacts.
- See [Release Checklist](./release.md) for the full release/signing setup checklist.
