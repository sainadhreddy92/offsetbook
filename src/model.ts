export interface EventRecord {
  readonly partition: string;
  readonly offset: number;
  readonly value: string;
}

export interface Position {
  readonly group: string;
  readonly partition: string;
  readonly next: number;
}

export function name(value: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new TypeError("expected a nonempty stream identifier");
  }
  return value;
}

export function offset(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError("offset must be a nonnegative safe integer");
  }
  return value;
}

export function limit(value: number): number {
  if (offset(value) === 0) throw new RangeError("limit must be positive");
  return value;
}
