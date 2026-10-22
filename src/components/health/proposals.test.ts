import { describe, expect, it } from "vitest";
import type { Proposal, ProposalChange } from "../../api/types";
import { i18n } from "../../i18n";
import { changeSummary, proposalStatusText, scopeText, strategyText } from "./proposals";

const t = i18n.getFixedT("ru");

function proposal(p: Partial<Proposal> = {}): Proposal {
  return {
    id: "p1",
    severity: "warning",
    title: "Raise min.insync.replicas",
    rationale: "r",
    target: { kind: "topic", name: "orders" },
    change: { kind: "addPartitions", topic: "orders", newTotal: 6 },
    client: "claude-ai",
    createdAtMs: 1,
    updatedAtMs: 1,
    status: "open",
    ...p,
  };
}

describe("changeSummary", () => {
  it("lists config changes with the default marker", () => {
    const c: ProposalChange = {
      kind: "alterTopicConfigs",
      topic: "orders",
      changes: [
        { name: "min.insync.replicas", value: "2" },
        { name: "retention.ms", value: null },
      ],
    };
    expect(changeSummary(c, t, "ru")).toBe("Конфиги топика: min.insync.replicas → 2; retention.ms → по умолчанию");
  });

  it("describes a new topic with the broker default and configs", () => {
    const c: ProposalChange = { kind: "createTopic", name: "dlq", partitions: 6, replicationFactor: -1, configs: { "cleanup.policy": "compact" } };
    expect(changeSummary(c, t, "ru")).toBe("Новый топик — партиций: 6, фактор репликации: по умолчанию брокера, конфиги: cleanup.policy=compact");
    expect(changeSummary({ ...c, replicationFactor: 3, configs: {} }, t, "ru")).toBe("Новый топик — партиций: 6, фактор репликации: 3");
  });

  it("describes partitions and offset resets with the words of the reset form", () => {
    expect(changeSummary({ kind: "addPartitions", topic: "orders", newTotal: 6 }, t, "ru")).toBe("Партиций: → 6");
    expect(changeSummary({ kind: "resetOffsets", groupId: "billing", strategy: { type: "earliest" }, scope: { type: "all" } }, t, "ru")).toBe(
      "Оффсеты группы: С начала (earliest), Вся группа",
    );
    expect(
      changeSummary(
        {
          kind: "resetOffsets",
          groupId: "billing",
          strategy: { type: "offset", offset: 10 },
          scope: { type: "partitions", partitions: [{ topic: "orders", partition: 0 }, { topic: "orders", partition: 1 }] },
        },
        t,
        "ru",
      ),
    ).toBe("Оффсеты группы: Конкретный оффсет 10, Партиции: orders:0, orders:1");
    expect(strategyText({ type: "shift", delta: -5 }, t, "ru")).toBe("Сдвиг на ±N -5");
    expect(strategyText({ type: "shift", delta: 5 }, t, "ru")).toBe("Сдвиг на ±N +5");
    expect(strategyText({ type: "timestamp", timestampMs: Date.UTC(2026, 9, 22, 7, 5, 9) }, t, "ru")).toMatch(/^По времени \d/);
    expect(scopeText({ type: "topics", topics: ["a", "b"] }, t)).toBe("Топики: a, b");
  });
});

describe("proposalStatusText", () => {
  it("is empty for open and tells how a closed one ended", () => {
    const now = 10 * 60_000;
    expect(proposalStatusText(proposal(), t, now, "ru")).toBeNull();
    expect(proposalStatusText(proposal({ status: "applied", closedAtMs: now - 5 * 60_000, appliedAsProposed: true }), t, now, "ru")).toBe("применено 5 минут назад");
    expect(proposalStatusText(proposal({ status: "applied", closedAtMs: now - 5 * 60_000, appliedAsProposed: false }), t, now, "ru")).toBe("применено с правками 5 минут назад");
    expect(proposalStatusText(proposal({ status: "rejected" }), t, now, "ru")).toBe("отклонено");
    expect(proposalStatusText(proposal({ status: "rejected", rejectReason: "позже" }), t, now, "ru")).toBe("отклонено: позже");
    expect(proposalStatusText(proposal({ status: "withdrawn" }), t, now, "ru")).toBe("отозвано");
  });
});
