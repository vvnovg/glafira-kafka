import type { BrowseParams, DataFormat, DecodedData, FilterSpec, StartPosition, TextMatch } from "../../api/types";

export interface BrowseFormState {
  startType: StartPosition["type"];
  count: number;
  offset: number;
  timestamp: string; // значение <input type="datetime-local">, локальное время
  minutes: number;
  partitions: string;
  limit: number;
  live: boolean;
  keyFormat: DataFormat;
  valueFormat: DataFormat;
  keyPattern: string;
  valuePattern: string;
  regex: boolean;
  caseSensitive: boolean;
  headerName: string;
  headerValue: string;
  jsonPath: string;
  fromTime: string;
  toTime: string;
}

export const defaultBrowseForm: BrowseFormState = {
  startType: "latest",
  count: 100,
  offset: 0,
  timestamp: "",
  minutes: 15,
  partitions: "",
  limit: 500,
  live: false,
  keyFormat: "auto",
  valueFormat: "auto",
  keyPattern: "",
  valuePattern: "",
  regex: false,
  caseSensitive: false,
  headerName: "",
  headerValue: "",
  jsonPath: "",
  fromTime: "",
  toTime: "",
};

const MAX_LIMIT = 100_000;

function parseLocalTime(value: string, what: string): number {
  const ms = new Date(value).getTime();
  if (!value || Number.isNaN(ms)) throw new Error(`Укажите корректное время: ${what}`);
  return ms;
}

function parsePartitions(text: string): number[] | null {
  const t = text.trim();
  if (!t) return null;
  return t.split(",").map((part) => {
    const n = Number(part.trim());
    if (!Number.isInteger(n) || n < 0) throw new Error(`Партиции: «${part.trim()}» — не номер партиции`);
    return n;
  });
}

export function buildBrowseParams(f: BrowseFormState, connectionId: string, topic: string): BrowseParams {
  if (!Number.isInteger(f.limit) || f.limit < 1 || f.limit > MAX_LIMIT) {
    throw new Error(`Лимит должен быть от 1 до ${MAX_LIMIT}`);
  }
  let start: StartPosition;
  switch (f.startType) {
    case "earliest":
      start = { type: "earliest" };
      break;
    case "latest":
      start = { type: "latest", count: f.count };
      break;
    case "offset":
      start = { type: "offset", offset: f.offset };
      break;
    case "timestamp":
      start = { type: "timestamp", timestampMs: parseLocalTime(f.timestamp, "время начала") };
      break;
    case "lastMinutes":
      start = { type: "lastMinutes", minutes: f.minutes };
      break;
    default:
      throw new Error(`Неизвестная начальная позиция: ${String(f.startType)}`);
  }

  const text = (pattern: string): TextMatch | undefined =>
    pattern ? { pattern, regex: f.regex, caseSensitive: f.caseSensitive } : undefined;
  const filter: FilterSpec = {};
  const key = text(f.keyPattern);
  if (key) filter.key = key;
  const value = text(f.valuePattern);
  if (value) filter.value = value;
  if (f.headerName.trim()) {
    const hv = text(f.headerValue);
    filter.header = hv ? { name: f.headerName.trim(), value: hv } : { name: f.headerName.trim() };
  }
  if (f.jsonPath.trim()) filter.jsonPath = f.jsonPath.trim();
  if (f.fromTime) filter.fromTimestamp = parseLocalTime(f.fromTime, "фильтр «с»");
  if (f.toTime) filter.toTimestamp = parseLocalTime(f.toTime, "фильтр «по»");

  return {
    connectionId,
    topic,
    partitions: parsePartitions(f.partitions),
    start,
    limit: f.limit,
    live: f.live,
    keyFormat: f.keyFormat,
    valueFormat: f.valueFormat,
    filter,
  };
}

export function prettyText(d: DecodedData | null): string {
  if (!d) return "";
  if (d.format === "json" && !d.error && !d.truncated) {
    try {
      return JSON.stringify(JSON.parse(d.text), null, 2);
    } catch {
      return d.text;
    }
  }
  return d.text;
}

export function previewText(d: DecodedData | null, max = 200): string {
  if (!d) return "null";
  const flat = d.text.replace(/\s+/g, " ");
  return flat.length > max ? `${flat.slice(0, max)}…` : flat;
}
