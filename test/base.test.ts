import test from "node:test";
import assert from "node:assert/strict";
import { EventLog, Checkpoints, CheckpointConflict, consume } from "../src/index.ts";

test("bounded reads assign offsets independently by partition", () => {
  const log = new EventLog();
  log.append("p", ["a", "b", "c"]);
  log.append("q", ["x"]);
  assert.deepEqual(log.read("p", 1, 1), [{partition:"p", offset:1, value:"b"}]);
  assert.equal(log.end("q"), 1);
});

test("a checkpoint belongs to a group and a partition", () => {
  const book = new Checkpoints();
  book.advance("g", "p", 0, 2);
  assert.throws(() => book.advance("g", "p", 0, 3), CheckpointConflict);
  assert.equal(book.next("other", "p"), 0);
  assert.equal(book.next("g", "q"), 0);
});

test("restored consumers resume after the successful prefix", async () => {
  const log = new EventLog();
  const book = new Checkpoints();
  log.append("p", ["a", "b", "c"]);
  await assert.rejects(consume(log, book, "g", "p", 3, record => {
    if (record.offset === 1) throw new Error("busy");
  }), /busy/);
  const restored = Checkpoints.restore(book.serialize());
  const seen: string[] = [];
  await consume(log, restored, "g", "p", 3, record => { seen.push(record.value); });
  assert.deepEqual(seen, ["b", "c"]);
});

test("snapshots reject duplicate entries", () => {
  const row = {group: "g", partition: "p", next: 1};
  assert.throws(() => Checkpoints.restore(JSON.stringify({version:1, positions:[row,row]})));
});

test("a source error does not partially append", () => {
  function* input() { yield "ok"; throw new Error("upstream"); }
  const log = new EventLog();
  assert.throws(() => log.append("p", input()), /upstream/);
  assert.equal(log.end("p"), 0);
});
