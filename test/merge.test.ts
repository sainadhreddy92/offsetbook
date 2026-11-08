import test from "node:test";
import assert from "node:assert/strict";
import { Checkpoints, mergeSnapshots } from "../src/index.ts";

test("takes the highest stored position independently for each group", () => {
  const a = new Checkpoints();
  const b = new Checkpoints();
  a.advance("billing","orders/0",0,12);
  b.advance("billing","orders/0",0,8);
  b.advance("metrics","orders/0",0,18);
  const merged = mergeSnapshots([b.serialize(), a.serialize(), b.serialize()]);
  assert.equal(merged.next("billing","orders/0"), 12);
  assert.equal(merged.next("metrics","orders/0"), 18);
});

test("empty input yields a usable new book", () => {
  const book = mergeSnapshots([]);
  book.advance("g","p",0,2);
  assert.equal(book.next("g","p"), 2);
});

test("an invalid snapshot is rejected", () => {
  assert.throws(() => mergeSnapshots([new Checkpoints().serialize(), "{}"]));
});
