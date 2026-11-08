import { name, offset, limit } from "./model.ts";
import type { EventRecord } from "./model.ts";

export class EventLog {
  readonly #streams = new Map<string, string[]>();

  append(partition: string, values: Iterable<string>): number {
    name(partition);
    const batch = Array.from(values);
    if (batch.some(value => typeof value !== "string")) {
      throw new TypeError("event payloads must be strings");
    }
    const stream = this.#streams.get(partition) ?? [];
    for (const value of batch) stream.push(value);
    this.#streams.set(partition, stream);
    return stream.length;
  }

  end(partition: string): number {
    return this.#streams.get(name(partition))?.length ?? 0;
  }

  read(partition: string, from: number, count: number): EventRecord[] {
    const stream = this.#streams.get(name(partition)) ?? [];
    offset(from);
    limit(count);
    if (from > stream.length) throw new RangeError("offset ahead of source");
    return stream.slice(from, from + count).map((value, index) => ({
      partition, offset: from + index, value,
    }));
  }
}
