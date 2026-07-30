Intent: bind asset and filesystem/VCS access to the owning project's `dataAudience`, while preserving unrestricted private-session behavior. The smallest useful slice is a path-to-project guard around asset issuance/resolution, project file/list/search/browse RPCs, and the VCS RPCs already exposed from `ws.ts`.

Blast radius: `apps/server/src/assets/AssetAccess.ts` and tests; `apps/server/src/ws.ts` project file, filesystem browse, asset URL, and VCS handlers; `apps/server/src/auth/audienceScopePolicy.ts` only for read methods that become guarded here; `apps/server/src/server.test.ts` focused WebSocket/HTTP coverage; contract schemas only if an existing structured error cannot express existence-masked denial.

Edge-case matrix and red-first tests:

- `asset-token-private-presented-in-factory`: a token minted for a private attachment/workspace/favicon must resolve to null under a factory ceiling, matching an absent/tampered asset URL.
- `asset-token-old-or-missing-audience`: a legacy v1 token without audience must fail closed for factory and continue for private only when ownership can still be classified.
- `file-rpc-cross-audience-read-write-list-search-browse`: factory-ceiling file/list/search/browse/read/write requests for a private project path return the same structured failure shape as a missing root/path; the same operations succeed for a factory project.
- `vcs-cross-audience-read-write`: factory-ceiling VCS status/listRefs/pull/worktree/ref/init requests against a private project cwd are denied before the VCS service is called, while factory-project calls reach the service.
- `path-traversal-and-absolute-escape`: `..`, absolute paths, and cwd/partial-path combinations cannot classify a private/outside target as factory.
- `symlink-escape`: a symlink inside a factory project to a private/outside file is rejected after realpath resolution for project file reads and asset serving.
- `unknown-owner-fail-closed`: a cwd/attachment/workspace root that cannot be mapped to one projected project is denied in a factory context and treated like nonexistent.
- `same-timestamp-duplicates-replay-version-skew`: no new persistence ordering is introduced; duplicate/replayed URLs rely on signed claims plus live project classification, and unknown/old claim fields fail closed for factory.

Smallest-change argument: add local classification helpers that resolve real paths and compare them against projected project roots/worktree paths, then wrap only the asset/filesystem/VCS call sites. Do not change read-query/event/command dispatch surfaces, capability RPC policy beyond the guarded read methods, or provider/terminal sandbox behavior.
