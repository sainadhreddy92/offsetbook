import test from "node:test";
import assert from "node:assert/strict";
import { EventLog, Checkpoints, sampleLag } from "../src/index.ts";

test("counts each requested partition once and preserves order", () => {
  const log = new EventLog();
  const book = new Checkpoints();
  log.append("p", ["x","y","z"]);
  book.advance("g","p",0,1);
  book.advance("other","p",0,3);
  const result = sampleLag(log, book, "g", ["q","p","q"]);
  assert.deepEqual(result.partitions.map(row => row.partition), ["q","p"]);
  assert.equal(result.pending, 2);
});

test("a checkpoint beyond the current log is reported separately", () => {
  const book = new Checkpoints();
  book.advance("g","q",0,3);
  const sample = sampleLag(new EventLog(), book, "g", ["q"]);
  assert.deepEqual(sample.partitions[0], {partition:"q", committed:3, end:0, pending:0, ahead:3});
});
