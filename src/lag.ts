import type { EventLog } from "./log.ts";
import type { Checkpoints } from "./checkpoints.ts";
import { name } from "./model.ts";

export interface PartitionLag {
  readonly partition: string;
  readonly committed: number;
  readonly end: number;
  readonly pending: number;
  readonly ahead: number;
}

export interface LagSample {
  readonly partitions: readonly PartitionLag[];
  readonly pending: number;
}

/** Collect a synchronous sample for the requested partitions in caller order. */
export function sampleLag(
  log: EventLog, book: Checkpoints, group: string, partitions: Iterable<string>,
): LagSample {
  name(group);
  const ids = new Set(partitions);
  const rows: PartitionLag[] = [];
  let total = 0;
  for (const partition of ids) {
    const committed = book.next(group, partition);
    const end = log.end(partition);
    const pending = Math.max(0, end - committed);
    total += pending;
    if (!Number.isSafeInteger(total)) throw new RangeError("lag exceeds safe integer range");
    rows.push({partition, committed, end, pending, ahead: Math.max(0, committed - end)});
  }
  return {partitions: rows, pending: total};
}
