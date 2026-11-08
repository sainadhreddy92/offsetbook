# Offsetbook

Offsetbook tracks which events a consumer group has processed. Each partition is an independent ordered stream. A checkpoint records the **next** offset to read. Two groups can consume the same partition at different speeds.

## Quick start

Use Node.js 24 or newer. From this checkout, run `npm test`. The library uses the standard library and runs TypeScript directly.

```ts
import { EventLog, Checkpoints, consume } from "./src/index.ts";

const log = new EventLog();
log.append("orders/0", ["order-71", "order-72"]);

const book = new Checkpoints();
await consume(log, book, "billing", "orders/0", 1, async event => {
  console.log(event.value); // order-71
});
console.log(book.next("billing", "orders/0")); // 1
```

## Delivery and recovery

Consume calls the handler in offset order and advances the checkpoint after each successful callback. If a callback fails, the successful prefix stays acknowledged and the failing event is eligible for redelivery. Handlers that write externally should tolerate duplicates: the callback and checkpoint are separate operations.

The event log and checkpoint book are in memory. To resume in a new process, persist the JSON from `book.serialize()`, reconstruct the same source stream, and initialize the consumer with `Checkpoints.restore(json)`. Group names and partition names are independent, opaque strings.

## Parking failed events

`consumeOrPark` works like `consume`, but when the handler throws it passes the failure to a parking sink you supply (for example, one that writes to a dead-letter table) and continues with the next event. It resolves with `{ handled, parked }`, whereas `consume` resolves with the number of events read.

- A record is acknowledged only after the handler succeeds or the sink's promise resolves. If the sink rejects, the error propagates, processing stops, and the record stays eligible for redelivery.
- Once parked, a record is never delivered again. Replaying parked records is up to the sink's owner.
- The sink receives `{ group, record, reason }`. `record.value` is the original payload and `reason` is exactly what the handler threw, which may not be an `Error`. Nothing is redacted, so sinks that write to shared storage should redact secrets themselves.
- Delivery is at least once, so a sink may see the same record more than once. Ignore duplicates keyed on group, partition and offset.

## Modules

- `src/log.ts` assigns zero-based offsets and reads bounded slices.
- `src/checkpoints.ts` owns checkpoint comparisons and snapshot persistence.
- `src/consumer.ts` connects the log and book to an asynchronous handler.
- `src/parking.ts` routes handler failures to a parking sink.
