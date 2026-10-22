import type { OffsetTarget, ResetPreviewRow, ResetRequest, ResetScope, ResetStrategy } from "../../api/types";
import { i18n } from "../../i18n";
import type { ResetPrefill } from "../../state/workspace";

export interface ResetForm {
  strategy: ResetStrategy["type"];
  /** offset / shift — число; timestamp — значение input datetime-local (локальное время). */
  value: string;
  scope: ResetScope["type"];
  /** topics: имена через запятую/пробел/перевод строки; partitions: «топик:партиция» через запятую. */
  scopeText: string;
}

/** Значение `datetime-local` (местное время, с секундами) из Unix-миллисекунд. */
export function toLocalInput(ms: number): string {
  const d = new Date(ms);
  const two = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())}T${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}`;
}

export function defaultResetForm(prefill?: ResetPrefill): ResetForm {
  if (!prefill) return { strategy: "earliest", value: "", scope: "all", scopeText: "" };
  if ("strategy" in prefill) {
    // Предложение LLM: стратегия и область — в поля формы в том виде, в каком их вводит человек.
    const s = prefill.strategy;
    const value = s.type === "offset" ? String(s.offset) : s.type === "shift" ? String(s.delta) : s.type === "timestamp" ? toLocalInput(s.timestampMs) : "";
    const sc = prefill.scope;
    const scopeText = sc.type === "topics" ? sc.topics.join(", ") : sc.type === "partitions" ? sc.partitions.map((p) => `${p.topic}:${p.partition}`).join(", ") : "";
    return { strategy: s.type, value, scope: sc.type, scopeText };
  }
  return { strategy: "offset", value: String(prefill.offset), scope: "partitions", scopeText: `${prefill.topic}:${prefill.partition}` };
}

const tokens = (text: string) => text.split(/[\s,]+/).filter(Boolean);

function parseStrategy(form: ResetForm): ResetStrategy {
  const n = Number(form.value);
  switch (form.strategy) {
    case "earliest":
    case "latest":
      return { type: form.strategy };
    case "offset":
      if (form.value.trim() === "" || !Number.isSafeInteger(n) || n < 0) throw new Error(i18n.t("groups.reset.errOffset"));
      return { type: "offset", offset: n };
    case "shift":
      if (!Number.isSafeInteger(n) || n === 0) throw new Error(i18n.t("groups.reset.errShift"));
      return { type: "shift", delta: n };
    case "timestamp": {
      const ms = new Date(form.value).getTime();
      if (!form.value || Number.isNaN(ms)) throw new Error(i18n.t("groups.reset.errTimestamp"));
      return { type: "timestamp", timestampMs: ms };
    }
  }
}

function parseScope(form: ResetForm): ResetScope {
  switch (form.scope) {
    case "all":
      return { type: "all" };
    case "topics": {
      const topics = tokens(form.scopeText);
      if (topics.length === 0) throw new Error(i18n.t("groups.reset.errNoTopic"));
      return { type: "topics", topics };
    }
    case "partitions": {
      const partitions = tokens(form.scopeText).map((t) => {
        const i = t.lastIndexOf(":");
        const p = Number(t.slice(i + 1));
        if (i <= 0 || !Number.isInteger(p) || p < 0 || t.slice(i + 1) === "") throw new Error(i18n.t("groups.reset.errPartitionFormat", { token: t }));
        return { topic: t.slice(0, i), partition: p };
      });
      if (partitions.length === 0) throw new Error(i18n.t("groups.reset.errNoPartition"));
      return { type: "partitions", partitions };
    }
  }
}

export function buildResetRequest(groupId: string, form: ResetForm): ResetRequest {
  return { groupId, strategy: parseStrategy(form), scope: parseScope(form) };
}

export function targetsFromPreview(rows: ResetPreviewRow[]): OffsetTarget[] {
  return rows
    .filter((r) => r.target !== null && !r.error)
    .map((r) => ({ topic: r.topic, partition: r.partition, offset: r.target as number }));
}
