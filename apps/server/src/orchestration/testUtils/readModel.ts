import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";

export function getDetailedReadModel(snapshotQuery: ProjectionSnapshotQuery["Service"]) {
  return Effect.gen(function* () {
    const snapshot = yield* snapshotQuery.getSnapshot();
    const threads = yield* Effect.forEach(snapshot.threads, (thread) =>
      snapshotQuery
        .getThreadDetailById(thread.id)
        .pipe(Effect.map((detail) => Option.getOrElse(detail, () => thread))),
    );
    return { ...snapshot, threads };
  });
}

export function readDetailedReadModel(snapshotQuery: ProjectionSnapshotQuery["Service"]) {
  return Effect.runPromise(getDetailedReadModel(snapshotQuery));
}
