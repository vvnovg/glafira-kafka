import type { DataFormat, McpAuditEntry } from "../../api/types";
import type { BrowsePrefill, PrefillInexact } from "../../state/workspace";
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
/** Текстовый фильтр, который сервер применил: с пустым шаблоном он отбрасывается. */
const textMatch = (v: unknown): TextMatchArg | null => {
  const m = match(v);
  return m && str(m.pattern) ? m : null;
};
const sameFlags = (a: TextMatchArg, b: TextMatchArg) => !!a.regex === !!b.regex && !!a.caseSensitive === !!b.caseSensitive;

/**
 * Значение datetime-local, которое форма прочитает не позже `ms` (dir −1, начало окна) или не раньше (dir 1, конец).
 * В повторяющемся часе перевода часов местное время двузначно, и `new Date(s)` берёт один из проходов: тогда граница
 * отодвигается наружу на величину сдвига, чтобы окно формы по-прежнему содержало окно вызова.
 */
function outwardLocal(ms: number, dir: 1 | -1): string {
  let at = ms;
  let s = toLocalInput(at);
  for (let i = 0; i < 3; i++) {
    const back = new Date(s).getTime();
    if (!(dir * (ms - back) > 0)) break;
    at += at - back;
    s = toLocalInput(at);
  }
  return s;
}

/** Успешный вызов чтения сообщений: у записи журнала есть кнопка «Открыть в „Сообщениях“» (неактивна, если аргументы усечены). */
export function isBrowsable(e: McpAuditEntry): boolean {
  return e.event === "call" && !!e.tool && BROWSABLE.has(e.tool) && e.outcome === "ok";
}

/** Вкладка топика и заполнение формы «Сообщения» из записи журнала MCP; null — повторить нельзя. */
export function browsePrefillFromAudit(
  e: McpAuditEntry,
): { connectionId: string; topic: string; prefill: Omit<BrowsePrefill, "nonce"> } | null {
  if (!isBrowsable(e)) return null;
  // Аргументы длиннее 1 КБ журнал хранит усечённой строкой — по ним форму не собрать.
  if (!e.args || typeof e.args !== "object" || Array.isArray(e.args) || !e.connection?.id) return null;
  const a = e.args as Args;
  const topic = str(a.topic);
  if (!topic) return null;
  const key = textMatch(a.key);
  const value = textMatch(a.value);
  const header = match(a.header) as { name?: unknown; value?: unknown } | null;
  const headerName = str(header?.name);
  // Заголовок без имени сервер отбрасывает вместе с фильтром его значения.
  const headerValue = headerName.trim() ? textMatch(header?.value) : null;
  const jsonPath = str(a.jsonPath);
  // В форме флаги regex и регистра общие для ключа, значения и значения заголовка: берутся флаги первого непустого
  // из значения, ключа, значения заголовка; если у остальных они другие — перенос неточный.
  const texts = [value, key, headerValue].filter((m): m is TextMatchArg => m !== null);
  const flags = texts[0];
  const inexact: PrefillInexact[] = texts.some((m) => !sameFlags(m, flags)) ? ["flags"] : [];
  const form: Partial<BrowseFormState> = {
    keyFormat: format(a.keyFormat),
    valueFormat: format(a.valueFormat),
    keyPattern: str(key?.pattern),
    valuePattern: str(value?.pattern),
    regex: !!flags?.regex,
    caseSensitive: !!flags?.caseSensitive,
    headerName,
    headerValue: str(headerValue?.pattern),
    jsonPath,
  };
  const partition = a.partition;
  const start = a.startOffset;
  // Режим «офсеты» есть только у read_messages: лишние поля summarize_messages сервер не читает.
  if (e.tool === "read_messages" && typeof partition === "number" && typeof start === "number") {
    form.startType = "offset";
    form.offset = start;
    form.partitions = String(partition);
    if (typeof a.endOffset === "number" && a.endOffset > start) form.limit = Math.min(a.endOffset - start, MAX_LIMIT);
    // В вызове диапазон офсетов ограничивает просмотр, в форме лимит считает найденные сообщения.
    if (texts.length > 0 || headerName.trim() || jsonPath.trim()) inexact.push("limit");
  } else {
    const from = timeMs(a.from);
    const to = timeMs(a.to);
    if (from === null || to === null) return null;
    // В форме — секунды: границы округляются наружу (и в повторяющемся часе остаются снаружи).
    const fromTime = outwardLocal(Math.floor(from / 1000) * 1000, -1);
    form.startType = "timestamp";
    form.timestamp = fromTime;
    form.fromTime = fromTime;
    form.toTime = outwardLocal(Math.ceil(to / 1000) * 1000, 1);
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
