import { Checkpoints } from "./checkpoints.ts";
import type { Position } from "./model.ts";

/** Build a fresh checkpoint book from independently captured snapshots. */
export function mergeSnapshots(snapshots: Iterable<string>): Checkpoints {
  const newest = new Map<string, Position>();
  for (const snapshot of snapshots) {
    const source = Checkpoints.restore(snapshot);
    for (const position of source.entries()) {
      const key = [position.group, position.partition].join("/");
      const prior = newest.get(key);
      if (!prior || prior.next < position.next) newest.set(key, position);
    }
  }
  const merged = new Checkpoints();
  for (const row of newest.values()) {
    merged.advance(row.group, row.partition, 0, row.next);
  }
  return merged;
}
