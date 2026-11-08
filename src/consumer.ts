import type { Checkpoints } from "./checkpoints.ts";
import type { EventLog } from "./log.ts";
import type { EventRecord } from "./model.ts";

export type Handler = (record: EventRecord) => void | Promise<void>;

export async function consume(
  log: EventLog,
  book: Checkpoints,
  group: string,
  partition: string,
  count: number,
  handler: Handler,
): Promise<number> {
  const start = book.next(group, partition);
  const batch = log.read(partition, start, count);
  for (const record of batch) {
    await handler(record);
    book.advance(group, partition, record.offset, record.offset + 1);
  }
  return batch.length;
}
