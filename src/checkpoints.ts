import { name, offset } from "./model.ts";
import type { Position } from "./model.ts";

export class CheckpointConflict extends Error {
  constructor(expected: number, actual: number) {
    super(`expected checkpoint ${expected}, found ${actual}`);
    this.name = "CheckpointConflict";
  }
}

export class Checkpoints {
  readonly #groups = new Map<string, Map<string, number>>();

  next(group: string, partition: string): number {
    name(group);
    name(partition);
    return this.#groups.get(group)?.get(partition) ?? 0;
  }

  advance(group: string, partition: string, expected: number, next: number): void {
    const current = this.next(group, partition);
    offset(expected);
    offset(next);
    if (current !== expected) throw new CheckpointConflict(expected, current);
    if (next < current) throw new RangeError("cannot rewind a checkpoint");
    if (next === current) return;
    const partitions = this.#groups.get(group) ?? new Map<string, number>();
    partitions.set(partition, next);
    this.#groups.set(group, partitions);
  }

  entries(): Position[] {
    const result: Position[] = [];
    for (const [group, partitions] of this.#groups) {
      for (const [partition, next] of partitions) {
        result.push({group, partition, next});
      }
    }
    return result;
  }

  serialize(): string {
    return JSON.stringify({ version: 1, positions: this.entries() });
  }

  static restore(serialized: string): Checkpoints {
    const input = JSON.parse(serialized);
    if (input?.version !== 1 || !Array.isArray(input.positions)) {
      throw new TypeError("invalid checkpoint snapshot");
    }
    const book = new Checkpoints();
    for (const row of input.positions) {
      if (!row || typeof row !== "object" || row.next === 0) {
        throw new TypeError("expected a stored checkpoint");
      }
      book.advance(row.group, row.partition, 0, row.next);
    }
    return book;
  }
}
