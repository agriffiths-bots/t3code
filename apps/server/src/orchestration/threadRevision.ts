/**
 * Return the per-thread revision that an authoritative detail snapshot is
 * known to cover. A newer persisted event may be waiting for the projection,
 * so reporting that marker as verified would let the client skip recovery.
 */
import type { EventId } from "@t3tools/contracts";

export interface ThreadRevisionMarker {
  readonly latestSequence: number;
  readonly latestEventId: EventId | null;
}

export function coveredThreadRevision(
  snapshotSequence: number,
  latest: ThreadRevisionMarker,
): ThreadRevisionMarker {
  return latest.latestSequence <= snapshotSequence
    ? latest
    : { latestSequence: 0, latestEventId: null };
}
