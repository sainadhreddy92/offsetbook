import test from "node:test";
import assert from "node:assert/strict";
import { EventLog, Checkpoints, CheckpointConflict, consume, consumeOrPark } from "../src/index.ts";
import type { FailedDelivery } from "../src/index.ts";

const GROUP = "billing";
const PART = "orders/0";

function setup() {
  const log = new EventLog();
  const book = new Checkpoints();
  log.append(PART, ["order-71", "order-72", "order-73"]);
  return { log, book };
}

const failOn = (value: string) => (record: { value: string }) => {
  if (record.value === value) throw new Error(`cannot bill ${value}`);
};

/** Collects unhandled rejections raised while `run` executes and shortly after. */
async function unhandledDuring(run: () => Promise<unknown>): Promise<unknown[]> {
  const seen: unknown[] = [];
  const listener = (reason: unknown) => { seen.push(reason); };
  process.on("unhandledRejection", listener);
  try {
    await run().catch(() => {});
    await new Promise(resolve => setTimeout(resolve, 20));
  } finally {
    process.off("unhandledRejection", listener);
  }
  return seen;
}

test("routes failed events to the supplied sink and continues", async () => {
  const log = new EventLog();
  const book = new Checkpoints();
  log.append("p", ["bad","good"]);
  const parked: string[] = [];
  const counts = await consumeOrPark(log, book, "g", "p", 5, record => {
    if (record.value === "bad") throw new Error("unsupported");
  }, async delivery => { parked.push(delivery.record.value); });
  assert.deepEqual(parked, ["bad"]);
  assert.deepEqual(counts, {handled:1, parked:1});
  assert.equal(book.next("g","p"), 2);
});

test("a synchronous sink error stops further work", async () => {
  const log = new EventLog();
  const book = new Checkpoints();
  log.append("p", ["bad"]);
  await assert.rejects(consumeOrPark(log, book, "g", "p", 1,
    () => { throw new Error("bad"); },
    () => { throw new Error("unavailable"); },
  ), /unavailable/);
  assert.equal(book.next("g","p"), 0);
});

test("mixed batch parks the failed event and handles the rest", async () => {
  const { log, book } = setup();
  const deliveries: FailedDelivery[] = [];
  const counts = await consumeOrPark(log, book, GROUP, PART, 3, failOn("order-72"),
    async delivery => { deliveries.push(delivery); });
  assert.deepEqual(counts, { handled: 2, parked: 1 });
  assert.equal(book.next(GROUP, PART), 3);
  assert.equal(deliveries.length, 1);
  assert.equal(deliveries[0].group, GROUP);
  assert.deepEqual(deliveries[0].record, { partition: PART, offset: 1, value: "order-72" });
  assert.match((deliveries[0].reason as Error).message, /cannot bill order-72/);
});

test("an asynchronous sink rejection leaves the event for redelivery", async () => {
  const { log, book } = setup();
  await assert.rejects(consumeOrPark(log, book, GROUP, PART, 3, failOn("order-72"),
    async () => { await Promise.resolve(); throw new Error("sink unavailable"); },
  ), /sink unavailable/);
  assert.equal(book.next(GROUP, PART), 1);
  const retried: string[] = [];
  await consume(log, book, GROUP, PART, 3, record => { retried.push(record.value); });
  assert.deepEqual(retried, ["order-72", "order-73"]);
});

test("the checkpoint does not move while the sink is still storing", async () => {
  const { log, book } = setup();
  let duringWrite = -1;
  await consumeOrPark(log, book, GROUP, PART, 3, failOn("order-72"), async () => {
    await Promise.resolve();
    duringWrite = book.next(GROUP, PART);
  });
  assert.equal(duringWrite, 1);
  assert.equal(book.next(GROUP, PART), 3);
});

test("a sink failure mid-batch keeps the prefix and stops further work", async () => {
  const { log, book } = setup();
  const handled: string[] = [];
  await assert.rejects(consumeOrPark(log, book, GROUP, PART, 3, record => {
    handled.push(record.value);
    if (record.value === "order-72") throw new Error("cannot bill");
  }, async () => { throw new Error("sink unavailable"); }), /sink unavailable/);
  assert.deepEqual(handled, ["order-71", "order-72"]);
  assert.equal(book.next(GROUP, PART), 1);
});

test("an event is not parked after another consumer moved the checkpoint", async () => {
  const { log, book } = setup();
  let sinkCalls = 0;
  await assert.rejects(consumeOrPark(log, book, GROUP, PART, 3, record => {
    if (record.value === "order-72") {
      book.advance(GROUP, PART, 1, 2); // a concurrent consumer acknowledges order-72
      throw new Error("cannot bill");
    }
  }, async () => { sinkCalls++; }), CheckpointConflict);
  assert.equal(sinkCalls, 0);
  assert.equal(book.next(GROUP, PART), 2);
});

test("a conflict during a failing sink write surfaces the sink error without an unhandled rejection", async () => {
  const { log, book } = setup();
  let caught: unknown;
  const unhandled = await unhandledDuring(async () => {
    try {
      await consumeOrPark(log, book, GROUP, PART, 3, failOn("order-72"), async () => {
        book.advance(GROUP, PART, 1, 2); // a concurrent consumer moves on while we store
        await Promise.resolve();
        throw new Error("sink down");
      });
    } catch (error) { caught = error; }
  });
  assert.deepEqual(unhandled, []);
  assert.match((caught as Error).message, /sink down/);
});

test("a conflict after a successful sink write is reported", async () => {
  const { log, book } = setup();
  await assert.rejects(consumeOrPark(log, book, GROUP, PART, 3, failOn("order-72"), async () => {
    book.advance(GROUP, PART, 1, 2);
  }), CheckpointConflict);
  assert.equal(book.next(GROUP, PART), 2);
});

test("a non-Error thrown value reaches the sink unchanged", async () => {
  const { log, book } = setup();
  const reasons: unknown[] = [];
  const counts = await consumeOrPark(log, book, GROUP, PART, 3, record => {
    if (record.value === "order-72") throw undefined;
  }, async delivery => { reasons.push(delivery.reason); });
  assert.deepEqual(reasons, [undefined]);
  assert.deepEqual(counts, { handled: 2, parked: 1 });
});

test("when every event succeeds the sink is never called", async () => {
  const { log, book } = setup();
  let sinkCalls = 0;
  const counts = await consumeOrPark(log, book, GROUP, PART, 3, () => {}, async () => { sinkCalls++; });
  assert.equal(sinkCalls, 0);
  assert.deepEqual(counts, { handled: 3, parked: 0 });
  assert.equal(book.next(GROUP, PART), 3);
});
