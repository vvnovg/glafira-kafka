import type { TFunction } from "i18next";
import type { Proposal, ProposalChange, ResetScope, ResetStrategy } from "../../api/types";
import { formatAgo } from "../../lib/formatAgo";
import type { TabSpec } from "../../state/workspace";

/** Вкладка, в которой предложение открывается заполненной формой (спец. G3 §8). */
export function proposalTab(connectionId: string, proposal: Proposal, nonce = Date.now()): TabSpec {
  const prefill = { proposal, nonce };
  switch (proposal.change.kind) {
    case "alterTopicConfigs":
    case "addPartitions":
      return { kind: "topic", connectionId, topic: proposal.change.topic, proposal: prefill };
    case "createTopic":
      return { kind: "topics", connectionId, proposal: prefill };
    case "resetOffsets":
      return { kind: "group", connectionId, groupId: proposal.change.groupId, proposal: prefill };
  }
}

/** Стратегия сброса словами формы сброса: «С начала (earliest)», «По времени 22.10.2026, 10:00:00», «Сдвиг на ±N −5». */
export function strategyText(s: ResetStrategy, t: TFunction, lang: string): string {
  switch (s.type) {
    case "earliest":
      return t("groups.reset.strategyEarliest");
    case "latest":
      return t("groups.reset.strategyLatest");
    case "timestamp":
      return `${t("groups.reset.strategyTimestamp")} ${new Intl.DateTimeFormat(lang, { dateStyle: "short", timeStyle: "medium" }).format(new Date(s.timestampMs))}`;
    case "offset":
      return `${t("groups.reset.strategyOffset")} ${s.offset}`;
    case "shift":
      return `${t("groups.reset.strategyShift")} ${s.delta > 0 ? "+" : ""}${s.delta}`;
  }
}

/** Область сброса словами формы: «Вся группа», «Топики: a, b», «Партиции: t:0, t:1». */
export function scopeText(s: ResetScope, t: TFunction): string {
  switch (s.type) {
    case "all":
      return t("groups.reset.scopeAll");
    case "topics":
      return `${t("groups.reset.scopeTopics")}: ${s.topics.join(", ")}`;
    case "partitions":
      return `${t("groups.reset.scopePartitions")}: ${s.partitions.map((p) => `${p.topic}:${p.partition}`).join(", ")}`;
  }
}

/** Описание изменения для карточки предложения (спец. G3 §7): что именно меняется, на языке интерфейса. */
export function changeSummary(change: ProposalChange, t: TFunction, lang: string): string {
  switch (change.kind) {
    case "alterTopicConfigs":
      return `${t("health.proposals.change.alterTopicConfigs")} ${change.changes
        .map((c) => `${c.name} → ${c.value ?? t("common.config.defaultValue")}`)
        .join("; ")}`;
    case "createTopic": {
      const rf = change.replicationFactor > 0 ? String(change.replicationFactor) : t("health.proposals.change.brokerDefault");
      const head = t("health.proposals.change.createTopic", { partitions: change.partitions, rf });
      const configs = Object.entries(change.configs).map(([k, v]) => `${k}=${v}`);
      return configs.length === 0 ? head : `${head}, ${t("health.proposals.change.configs", { configs: configs.join(", ") })}`;
    }
    case "addPartitions":
      return t("health.proposals.change.addPartitions", { newTotal: change.newTotal });
    case "resetOffsets":
      return t("health.proposals.change.resetOffsets", { strategy: strategyText(change.strategy, t, lang), scope: scopeText(change.scope, t) });
  }
}

/** Статус закрытого предложения: «применено 5 минут назад», «применено с правками …», «отклонено: …», «отозвано». */
export function proposalStatusText(p: Proposal, t: TFunction, nowMs: number, lang: string): string | null {
  switch (p.status) {
    case "open":
      return null;
    case "applied": {
      const when = formatAgo(p.closedAtMs ?? p.updatedAtMs, nowMs, lang);
      return p.appliedAsProposed === false ? t("health.proposals.status.appliedEdited", { when }) : t("health.proposals.status.applied", { when });
    }
    case "rejected":
      return p.rejectReason ? t("health.proposals.status.rejectedWithReason", { reason: p.rejectReason }) : t("health.proposals.status.rejected");
    case "withdrawn":
      return t("health.proposals.status.withdrawn");
  }
}
