import { i18n } from "../../i18n";
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
const MAX_MINUTES = 5_256_000;

function parseLocalTime(value: string, what: string): number {
  const ms = new Date(value).getTime();
  if (!value || Number.isNaN(ms)) throw new Error(i18n.t("browse.errors.invalidTime", { what }));
  return ms;
}

function parsePartitions(text: string): number[] | null {
  const t = text.trim();
  if (!t) return null;
  const out: number[] = [];
  for (const raw of t.split(",")) {
    const part = raw.trim();
    if (!/^\d+$/.test(part)) throw new Error(i18n.t("browse.errors.badPartition", { part }));
    const n = Number(part);
    if (!Number.isSafeInteger(n)) throw new Error(i18n.t("browse.errors.badPartition", { part }));
    if (!out.includes(n)) out.push(n);
  }
  return out;
}

export type FilterFields = Pick<
  BrowseFormState,
  "keyPattern" | "valuePattern" | "regex" | "caseSensitive" | "headerName" | "headerValue" | "jsonPath" | "fromTime" | "toTime"
>;

export function buildFilterSpec(f: FilterFields): FilterSpec {
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
  if (f.fromTime) filter.fromTimestamp = parseLocalTime(f.fromTime, i18n.t("browse.errors.filterFrom"));
  if (f.toTime) filter.toTimestamp = parseLocalTime(f.toTime, i18n.t("browse.errors.filterTo"));
  if (filter.fromTimestamp !== undefined && filter.toTimestamp !== undefined && filter.fromTimestamp > filter.toTimestamp) {
    throw new Error(i18n.t("browse.errors.timeOrder"));
  }
  return filter;
}

export function buildBrowseParams(f: BrowseFormState, connectionId: string, topic: string): BrowseParams {
  if (!Number.isInteger(f.limit) || f.limit < 1 || f.limit > MAX_LIMIT) {
    throw new Error(i18n.t("browse.errors.limit", { max: MAX_LIMIT }));
  }
  let start: StartPosition;
  switch (f.startType) {
    case "earliest":
      start = { type: "earliest" };
      break;
    case "latest":
      if (!Number.isInteger(f.count) || f.count < 1 || f.count > MAX_LIMIT) {
        throw new Error(i18n.t("browse.errors.count", { max: MAX_LIMIT }));
      }
      start = { type: "latest", count: f.count };
      break;
    case "offset":
      if (!Number.isInteger(f.offset) || f.offset < 0) throw new Error(i18n.t("browse.errors.offset"));
      start = { type: "offset", offset: f.offset };
      break;
    case "timestamp":
      start = { type: "timestamp", timestampMs: parseLocalTime(f.timestamp, i18n.t("browse.errors.startTime")) };
      break;
    case "lastMinutes":
      if (!Number.isInteger(f.minutes) || f.minutes < 1 || f.minutes > MAX_MINUTES) {
        throw new Error(i18n.t("browse.errors.minutes", { max: MAX_MINUTES }));
      }
      start = { type: "lastMinutes", minutes: f.minutes };
      break;
    default:
      throw new Error(i18n.t("browse.errors.unknownStart", { value: String(f.startType) }));
  }

  const filter = buildFilterSpec(f);

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
  const head = d.text.slice(0, max * 4);
  const flat = head.replace(/\s+/g, " ");
  return flat.length > max || d.text.length > head.length ? `${flat.slice(0, max)}…` : flat;
}
