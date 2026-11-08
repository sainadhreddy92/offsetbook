import test from "node:test";
import assert from "node:assert/strict";
import { EventLog, Checkpoints, consumeOrPark } from "../src/index.ts";

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
