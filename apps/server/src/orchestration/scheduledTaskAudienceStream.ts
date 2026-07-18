import { NonNegativeInt, type ScheduledTasksStreamItem } from "@t3tools/contracts";
import * as Equal from "effect/Equal";
import * as Stream from "effect/Stream";

export function scopeScheduledTaskStreamForAudience<E, R>(
  stream: Stream.Stream<ScheduledTasksStreamItem, E, R>,
): Stream.Stream<ScheduledTasksStreamItem, E, R> {
  let sequence = 0;
  return stream.pipe(
    Stream.changesWith(
      (previous, current) =>
        previous.kind === "snapshot" &&
        current.kind === "snapshot" &&
        Equal.equals(previous.snapshot.tasks, current.snapshot.tasks),
    ),
    Stream.map((item) => {
      const audienceSequence = NonNegativeInt.make(sequence++);
      return item.kind === "snapshot"
        ? { ...item, snapshot: { ...item.snapshot, sequence: audienceSequence } }
        : { ...item, sequence: audienceSequence };
    }),
  );
}
