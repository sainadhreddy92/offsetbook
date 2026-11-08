import { CheckpointConflict } from "./checkpoints.ts";
import type { Checkpoints } from "./checkpoints.ts";
import type { EventLog } from "./log.ts";
import type { Handler } from "./consumer.ts";
import type { EventRecord } from "./model.ts";

/**
 * A delivery the handler could not process. `record` carries the original,
 * unredacted payload and `reason` is exactly what the handler threw (not
 * necessarily an `Error`). Sinks that write to shared storage should redact.
 */
export interface FailedDelivery {
  readonly group: string;
  readonly record: EventRecord;
  readonly reason: unknown;
}

/**
 * Durably stores a failed delivery. Resolve only once it is stored. Delivery is
 * at least once, so sinks should ignore duplicates keyed on
 * (group, record.partition, record.offset).
 */
export type ParkingSink = (delivery: FailedDelivery) => Promise<void>;

/**
 * Like `consume`, but a handler failure is sent to `park` instead of stopping.
 * Each record is acknowledged only after the handler succeeds or the sink
 * resolves. A sink failure propagates, stops the batch, and leaves the record
 * eligible for redelivery. Resolves with how many records were handled and parked.
 */
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
      const current = book.next(group, partition);
      if (current !== record.offset) throw new CheckpointConflict(record.offset, current);
      await park({group, record, reason});
      book.advance(group, partition, record.offset, record.offset + 1);
      parked++;
      continue;
    }
    book.advance(group, partition, record.offset, record.offset + 1);
    handled++;
  }
  return { handled, parked };
}
