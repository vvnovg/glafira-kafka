import { describe, expect, it } from "vitest";
import type { Annotation, Finding, HealthView } from "../../api/types";
import { i18n } from "../../i18n";
import { BADGE_TITLES_MAX, badgeFor, badgeTooltip, findingsOf, findingTexts, itemsFor, sameTarget } from "./findings";

const t = i18n.getFixedT("ru");

function finding(ruleId: string, severity: Finding["severity"], target: Finding["target"], params: Record<string, unknown> = {}): Finding {
  return { key: `${ruleId}:${target.kind}:${target.name ?? ""}`, ruleId, severity, target, params };
}

function note(severity: Annotation["severity"], name: string, status: Annotation["status"] = "open", title = "t"): Annotation {
  return {
    id: name + severity + title,
    target: { kind: "topic", name },
    severity,
    title,
    body: "b",
    client: "claude-ai",
    createdAtMs: 1,
    updatedAtMs: 1,
    status,
  };
}

function view(findings: Finding[], annotations: Annotation[] = [], ignored: string[] = []): HealthView {
  return {
    lastCheck: {
      runId: "r",
      startedAtMs: 1,
      durationMs: 1,
      trigger: { kind: "ui" },
      environment: "prod",
      incomplete: false,
      scope: { brokers: 1, topicsChecked: 1, topicsTotal: 1, groupsChecked: 0, groupsTotal: 0 },
      findings,
    },
    ignored,
    annotations,
  };
}

describe("findingTexts", () => {
  it("joins lists and names the target", () => {
    const f = finding("partition.offline", "critical", { kind: "topic", name: "orders" }, { count: 2, partitions: [0, 3] });
    const x = findingTexts(f, t, "ru");
    expect(x.title).toBe("Недоступные партиции в orders");
    expect(x.details).toBe("Партиции без лидера нельзя ни читать, ни писать: 0, 3 (всего 2).");
  });

  it("describes the value source and broker values", () => {
    const own = finding("topic.minIsrUnreachable", "critical", { kind: "topic", name: "a" }, { minIsr: 3, replicationFactor: 2, source: "DynamicTopic" });
    expect(findingTexts(own, t, "ru").details).toContain("(задано у топика)");
    const inherited = { ...own, params: { ...own.params, source: "Default" } };
    expect(findingTexts(inherited, t, "ru").details).toContain("(унаследовано)");
    const drift = finding("broker.configDrift", "warning", { kind: "cluster" }, {
      config: "num.partitions",
      values: [
        { brokerId: 1, value: "3" },
        { brokerId: 2, value: "6" },
      ],
    });
    const d = findingTexts(drift, t, "ru");
    expect(d.title).toBe("num.partitions различается между брокерами");
    expect(d.details).toContain("1: 3, 2: 6");
  });

  it("formats the deviation with a sign and sums read errors", () => {
    const skew = finding("broker.leaderSkew", "warning", { kind: "broker", name: "2" }, { leaders: 13, average: 10, deviationPercent: 30 });
    expect(findingTexts(skew, t, "en").details).toContain("(+30%)");
    const partial = finding("check.partial", "info", { kind: "cluster" }, {
      topicsChecked: 2000,
      topicsTotal: 2500,
      groupsChecked: 3,
      groupsTotal: 3,
      topicConfigErrors: 1,
      brokerConfigErrors: 2,
      groupErrors: 0,
      lagErrors: 1,
      timedOut: false,
    });
    expect(findingTexts(partial, t, "ru").details).toContain("ошибок чтения: 4");
  });
});

describe("selection and badges", () => {
  const a = finding("topic.replicationFactorOne", "critical", { kind: "topic", name: "a" });
  const b = finding("topic.minIsrOne", "warning", { kind: "topic", name: "b" });
  const info = finding("partition.notPreferredLeader", "info", { kind: "cluster" });

  it("hides ignored findings unless asked", () => {
    const v = view([a, b, info], [], [a.key]);
    expect(findingsOf(v, false).map((f) => f.key)).toEqual([b.key, info.key]);
    expect(findingsOf(v, true)).toHaveLength(3);
    expect(findingsOf(undefined, true)).toEqual([]);
  });

  it("counts severe findings and open notes of an object", () => {
    const v = view([a, b, info], [note("warning", "a"), note("critical", "a", "resolved"), note("info", "a")]);
    const isA = (x: Finding["target"]) => sameTarget(x, { kind: "topic", name: "a" });
    expect(badgeFor(v, isA)).toMatchObject({ count: 2, severity: "critical" });
    expect(itemsFor(v, isA).notes).toHaveLength(2);
    expect(badgeFor(v, () => true)).toMatchObject({ count: 3, severity: "critical" });
    expect(badgeFor(view([info]), () => true)).toBeNull();
    expect(sameTarget({ kind: "cluster" }, { kind: "cluster", name: "x" })).toBe(true);
  });
});

describe("badge tooltip", () => {
  const rf = (name: string, severity: Finding["severity"] = "critical") => finding("topic.replicationFactorOne", severity, { kind: "topic", name });
  const isTopic = (x: Finding["target"]) => x.kind === "topic";

  it("lists what the badge counted, most severe first, without info and hidden", () => {
    const v = view(
      [rf("w1", "warning"), rf("c1"), rf("i1", "info"), rf("hidden")],
      [note("critical", "n1", "open", "Critical note"), note("warning", "n2", "open", "Warning note"), note("critical", "n3", "resolved", "Closed note"), note("info", "n4", "open", "Info note")],
      [rf("hidden").key],
    );
    const b = badgeFor(v, isTopic);
    expect(b).toMatchObject({ count: 4, severity: "critical" });
    // Внутри строгости: сначала находки, затем замечания; порядок как в данных.
    expect(b?.items.map((i) => (i.kind === "finding" ? i.finding.target.name : i.note.title))).toEqual(["c1", "Critical note", "w1", "Warning note"]);
    expect(badgeTooltip(b!, t, "ru")).toBe(
      ["Требуют внимания: 4", "Фактор репликации 1 у c1", "Critical note", "Фактор репликации 1 у w1", "Warning note"].join("\n"),
    );
  });

  it("caps the titles and marks the rest with an ellipsis", () => {
    const names = Array.from({ length: BADGE_TITLES_MAX + 2 }, (_, i) => `t${String(i).padStart(2, "0")}`);
    const b = badgeFor(view(names.map((n) => rf(n))), isTopic)!;
    expect(b.count).toBe(BADGE_TITLES_MAX + 2);
    const lines = badgeTooltip(b, t, "ru").split("\n");
    expect(lines).toHaveLength(1 + BADGE_TITLES_MAX + 1);
    expect(lines[0]).toBe(`Требуют внимания: ${BADGE_TITLES_MAX + 2}`);
    expect(lines[1]).toBe("Фактор репликации 1 у t00");
    expect(lines[BADGE_TITLES_MAX]).toBe("Фактор репликации 1 у t09");
    expect(lines[lines.length - 1]).toBe("…");
  });

  it("has no ellipsis at exactly the cap", () => {
    const b = badgeFor(view(Array.from({ length: BADGE_TITLES_MAX }, (_, i) => rf(`t${i}`))), isTopic)!;
    const lines = badgeTooltip(b, t, "ru").split("\n");
    expect(lines).toHaveLength(1 + BADGE_TITLES_MAX);
    expect(lines).not.toContain("…");
  });
});
