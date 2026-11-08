import type { EventLog } from "./log.ts";
import type { Checkpoints } from "./checkpoints.ts";
import type { Handler } from "./consumer.ts";
import { limit } from "./model.ts";

/** Dispatch a bounded window, settle it, and checkpoint successful work. */
export async function drain(
  log: EventLog, book: Checkpoints, group: string, partition: string,
  count: number, concurrency: number, handle: Handler,
): Promise<number> {
  limit(concurrency);
  let cursor = book.next(group, partition);
  const batch = log.read(partition, cursor, count);
  for (let start = 0; start < batch.length; start += concurrency) {
    const window = batch.slice(start, start + concurrency);
    const results = await Promise.allSettled(window.map(async record => handle(record)));
    let next = cursor;
    for (const [index, result] of results.entries()) {
      if (result.status === "fulfilled") next = window[index].offset + 1;
    }
    if (next > cursor) book.advance(group, partition, cursor, next);
    const failure = results.find(result => result.status === "rejected");
    if (failure) throw failure.reason;
    cursor = next;
  }
  return batch.length;
}
