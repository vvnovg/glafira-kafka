import type { DataFormat, McpAuditEntry } from "../../api/types";
import type { BrowsePrefill } from "../../state/workspace";
import type { BrowseFormState } from "../browse/params";
import { toLocalInput } from "../groups/reset";

/** Инструменты чтения сообщений, чей вызов можно повторить на экране «Сообщения» (G4). */
const BROWSABLE = new Set(["read_messages", "summarize_messages"]);
const FORMATS = new Set(["auto", "string", "json", "hex", "base64"]);
const MAX_LIMIT = 100_000;

type Args = Record<string, unknown>;
type TextMatchArg = { pattern?: unknown; regex?: unknown; caseSensitive?: unknown };

function timeMs(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const ms = Date.parse(v);
    return Number.isNaN(ms) ? null : ms;
  }
  return null;
}

const str = (v: unknown): string => (typeof v === "string" ? v : "");
const format = (v: unknown): DataFormat => (typeof v === "string" && FORMATS.has(v) ? (v as DataFormat) : "auto");
const match = (v: unknown): TextMatchArg | null => (v && typeof v === "object" ? (v as TextMatchArg) : null);

/** Вкладка топика и заполнение формы «Сообщения» из записи журнала MCP; null — повторить нельзя. */
export function browsePrefillFromAudit(
  e: McpAuditEntry,
): { connectionId: string; topic: string; prefill: Omit<BrowsePrefill, "nonce"> } | null {
  // Аргументы длиннее 1 КБ журнал хранит усечённой строкой — по ним форму не собрать.
  if (e.event !== "call" || !e.tool || !BROWSABLE.has(e.tool) || e.outcome !== "ok") return null;
  if (!e.args || typeof e.args !== "object" || Array.isArray(e.args) || !e.connection?.id) return null;
  const a = e.args as Args;
  const topic = str(a.topic);
  if (!topic) return null;
  const key = match(a.key);
  const value = match(a.value);
  const header = match(a.header) as { name?: unknown; value?: unknown } | null;
  const headerValue = match(header?.value);
  // В форме флаги общие для ключа и значения: при расхождении берутся флаги фильтра значения.
  const flags = value ?? key;
  const inexact = !!key && !!value && (!!key.regex !== !!value.regex || !!key.caseSensitive !== !!value.caseSensitive);
  const form: Partial<BrowseFormState> = {
    keyFormat: format(a.keyFormat),
    valueFormat: format(a.valueFormat),
    keyPattern: str(key?.pattern),
    valuePattern: str(value?.pattern),
    regex: !!flags?.regex,
    caseSensitive: !!flags?.caseSensitive,
    headerName: str(header?.name),
    headerValue: str(headerValue?.pattern),
    jsonPath: str(a.jsonPath),
  };
  const partition = a.partition;
  const start = a.startOffset;
  if (typeof partition === "number" && typeof start === "number") {
    form.startType = "offset";
    form.offset = start;
    form.partitions = String(partition);
    if (typeof a.endOffset === "number" && a.endOffset > start) form.limit = Math.min(a.endOffset - start, MAX_LIMIT);
  } else {
    const from = timeMs(a.from);
    const to = timeMs(a.to);
    if (from === null || to === null) return null;
    // В форме — секунды: границы округляются наружу.
    const fromSec = Math.floor(from / 1000) * 1000;
    form.startType = "timestamp";
    form.timestamp = toLocalInput(fromSec);
    form.fromTime = toLocalInput(fromSec);
    form.toTime = toLocalInput(Math.ceil(to / 1000) * 1000);
    form.partitions = Array.isArray(a.partitions) ? a.partitions.filter((p) => typeof p === "number").join(", ") : "";
  }
  return { connectionId: e.connection.id, topic, prefill: { form, inexact } };
}

/** Что прочитал вызов: топик или группа из аргументов. */
export function auditTarget(e: McpAuditEntry): string {
  if (!e.args || typeof e.args !== "object" || Array.isArray(e.args)) return "";
  const a = e.args as Args;
  return str(a.topic) || str(a.groupId);
}
