import type { Checkpoints } from "./checkpoints.ts";
import type { EventLog } from "./log.ts";
import type { Handler } from "./consumer.ts";
import type { EventRecord } from "./model.ts";

export interface FailedDelivery {
  readonly group: string;
  readonly record: EventRecord;
  readonly reason: unknown;
}

export type ParkingSink = (delivery: FailedDelivery) => Promise<void>;

export async function consumeOrPark(
  log: EventLog, book: Checkpoints, group: string, partition: string,
  count: number, handle: Handler, park: ParkingSink,
): Promise<{ handled: number; parked: number }> {
  const batch = log.read(partition, book.next(group, partition), count);
  let handled = 0;
  let parked = 0;
  for (const record of batch) {
    try {
      await handle(record);
    } catch (reason) {
      const write = park({group, record, reason});
      book.advance(group, partition, record.offset, record.offset + 1);
      await write;
      parked++;
      continue;
    }
    book.advance(group, partition, record.offset, record.offset + 1);
    handled++;
  }
  return { handled, parked };
}
