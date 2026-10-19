import type { TFunction } from "i18next";
import type { Annotation, Finding, HealthTarget, HealthView, Severity } from "../../api/types";

export const SEVERITY_RANK: Record<Severity, number> = { info: 0, warning: 1, critical: 2 };
export const SEVERITIES: Severity[] = ["critical", "warning", "info"];

const num = (v: unknown) => (typeof v === "number" ? v : 0);

/**
 * Параметры текста находки (`health.rules.<ruleId>.*`): имя объекта, списки через запятую,
 * источник значения словами, отклонение — процентом со знаком в формате языка.
 */
export function findingParams(f: Finding, t: TFunction, lang: string): Record<string, string | number> {
  const p: Record<string, string | number> = { name: f.target.name ?? "" };
  for (const [k, v] of Object.entries(f.params)) {
    if (k === "values" && Array.isArray(v)) {
      p[k] = v
        .map((x) => {
          const e = x as { brokerId?: unknown; value?: unknown };
          return `${String(e.brokerId)}: ${String(e.value ?? "—")}`;
        })
        .join(", ");
    } else if (Array.isArray(v)) {
      p[k] = v.map(String).join(", ");
    } else if (typeof v === "number" || typeof v === "string") {
      p[k] = v;
    } else if (typeof v === "boolean") {
      p[k] = String(v);
    }
  }
  if (typeof f.params.source === "string") {
    p.source = t(f.params.source === "DynamicTopic" ? "health.source.topic" : "health.source.inherited");
  }
  if (typeof f.params.deviationPercent === "number") {
    p.deviation = new Intl.NumberFormat(lang, { style: "percent", signDisplay: "always" }).format(f.params.deviationPercent / 100);
  }
  if (f.ruleId === "check.partial") {
    p.errors = num(f.params.topicConfigErrors) + num(f.params.brokerConfigErrors) + num(f.params.groupErrors) + num(f.params.lagErrors);
  }
  return p;
}

export function findingTexts(f: Finding, t: TFunction, lang: string) {
  const p = findingParams(f, t, lang);
  const base = `health.rules.${f.ruleId}`;
  return { title: t(`${base}.title`, p), details: t(`${base}.details`, p), fix: t(`${base}.fix`, p) };
}

export function sameTarget(a: HealthTarget, b: HealthTarget): boolean {
  return a.kind === b.kind && (a.kind === "cluster" || a.name === b.name);
}

/** Находки последней проверки: скрытые — только по запросу. */
export function findingsOf(view: HealthView | undefined, showHidden: boolean): Finding[] {
  if (!view?.lastCheck) return [];
  const hidden = new Set(view.ignored);
  return view.lastCheck.findings.filter((f) => showHidden || !hidden.has(f.key));
}

export function openNotes(view: HealthView | undefined): Annotation[] {
  return (view?.annotations ?? []).filter((a) => a.status === "open");
}

/** Что относится к объекту: видимые находки и открытые замечания. */
export function itemsFor(view: HealthView | undefined, match: (t: HealthTarget) => boolean) {
  return {
    findings: findingsOf(view, false).filter((f) => match(f.target)),
    notes: openNotes(view).filter((a) => match(a.target)),
  };
}

export interface Badge {
  count: number;
  severity: Severity;
}

/** Значок: число видимых находок и открытых замечаний уровня «критично» и «предупреждение». */
export function badgeFor(view: HealthView | undefined, match: (t: HealthTarget) => boolean): Badge | null {
  const { findings, notes } = itemsFor(view, match);
  const severe = [...findings.map((f) => f.severity), ...notes.map((a) => a.severity)].filter((s) => s !== "info");
  if (severe.length === 0) return null;
  const top = severe.reduce((a, b) => (SEVERITY_RANK[b] > SEVERITY_RANK[a] ? b : a));
  return { count: severe.length, severity: top };
}
