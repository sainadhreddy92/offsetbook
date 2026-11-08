import test from "node:test";
import assert from "node:assert/strict";
import { EventLog, Checkpoints, drain } from "../src/index.ts";

test("at most two callbacks run together", async () => {
  const log = new EventLog();
  const book = new Checkpoints();
  log.append("p", ["a", "b", "c"]);
  let active = 0, peak = 0;
  const count = await drain(log, book, "g", "p", 3, 2, async () => {
    peak = Math.max(peak, ++active);
    await Promise.resolve();
    active--;
  });
  assert.equal(peak, 2);
  assert.equal(count, 3);
  assert.equal(book.next("g", "p"), 3);
});

test("failure on the final event leaves it pending", async () => {
  const log = new EventLog();
  const book = new Checkpoints();
  log.append("p", ["a", "b"]);
  await assert.rejects(drain(log, book, "g", "p", 5, 2, record => {
    if (record.offset === 1) throw new Error("busy");
  }), /busy/);
  assert.equal(book.next("g", "p"), 1);
});
