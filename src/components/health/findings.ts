import type { TFunction } from "i18next";
import type { Annotation, Finding, HealthTarget, HealthView, Proposal, Severity } from "../../api/types";

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

/** Закрытые замечания и предложения — недавно закрытые сверху (по времени закрытия, иначе обновления). */
export function newestClosedFirst<T extends { closedAtMs?: number; updatedAtMs: number }>(items: T[]): T[] {
  return [...items].sort((a, b) => (b.closedAtMs ?? b.updatedAtMs) - (a.closedAtMs ?? a.updatedAtMs));
}

export function openProposals(view: HealthView | undefined): Proposal[] {
  return (view?.proposals ?? []).filter((p) => p.status === "open");
}

/**
 * Что относится к объекту: видимые находки, открытые замечания и открытые предложения. Без `match` —
 * всё подключение (значок вкладки «Замечания»). Предложения `createTopic` — только там: топика ещё
 * нет, и блок или значок объекта, даже одноимённого топика, их не показывает (спец. G3 §7).
 */
export function itemsFor(view: HealthView | undefined, match?: (t: HealthTarget) => boolean) {
  const m = match ?? (() => true);
  return {
    findings: findingsOf(view, false).filter((f) => m(f.target)),
    notes: openNotes(view).filter((a) => m(a.target)),
    proposals: openProposals(view).filter((p) => !match || (p.change.kind !== "createTopic" && match(p.target))),
  };
}

/** Что посчитал значок: находка, открытое замечание или открытое предложение уровня «критично» или «предупреждение». */
export type BadgeItem =
  | { kind: "finding"; severity: Severity; finding: Finding }
  | { kind: "note"; severity: Severity; note: Annotation }
  | { kind: "proposal"; severity: Severity; proposal: Proposal };

export interface Badge {
  count: number;
  severity: Severity;
  /** Самые строгие первыми; внутри строгости — сначала находки, затем замечания, затем предложения; порядок данных сохраняется. */
  items: BadgeItem[];
}

/** Сколько заголовков показывает подсказка значка; остальное сворачивается в «…». */
export const BADGE_TITLES_MAX = 10;

/** Значок: число видимых находок, открытых замечаний и предложений уровня «критично» и «предупреждение»; без `match` — всего подключения. */
export function badgeFor(view: HealthView | undefined, match?: (t: HealthTarget) => boolean): Badge | null {
  const { findings, notes, proposals } = itemsFor(view, match);
  const items: BadgeItem[] = [
    ...findings.map((finding): BadgeItem => ({ kind: "finding", severity: finding.severity, finding })),
    ...notes.map((note): BadgeItem => ({ kind: "note", severity: note.severity, note })),
    ...proposals.map((proposal): BadgeItem => ({ kind: "proposal", severity: proposal.severity, proposal })),
  ]
    .filter((i) => i.severity !== "info")
    // Сортировка стабильна: порядок внутри одной строгости остаётся прежним.
    .sort((a, b) => SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity]);
  if (items.length === 0) return null;
  return { count: items.length, severity: items[0].severity, items };
}

const oneLine = (s: string) => s.replace(/\s+/g, " ").trim();

/**
 * Подсказка значка: первая строка — «Требуют внимания: N», далее заголовки учтённого (не больше 10, затем «…»).
 * Строки через перевод строки: нативная подсказка `title` его показывает. Пробелы и переводы строк
 * в заголовке замечания или предложения LLM схлопываются: одна строка подсказки — один заголовок.
 */
export function badgeTooltip(badge: Badge, t: TFunction, lang: string): string {
  const titles = badge.items
    .slice(0, BADGE_TITLES_MAX)
    .map((i) => (i.kind === "finding" ? findingTexts(i.finding, t, lang).title : i.kind === "note" ? oneLine(i.note.title) : oneLine(i.proposal.title)));
  if (badge.items.length > BADGE_TITLES_MAX) titles.push("…");
  return [t("health.badgeTitle", { count: badge.count }), ...titles].join("\n");
}
