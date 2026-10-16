import type { NewTopicSpec } from "../../api/types";
import { i18n } from "../../i18n";

const MAX_NAME = 249;

/** Те же правила, что в Rust (`kafka::admin::validate_topic_name`): ошибка — сразу у поля. */
export function validateTopicName(name: string): string | null {
  if (!name) return i18n.t("topics.name.empty");
  if (name === "." || name === "..") return i18n.t("topics.name.dots");
  if (name.length > MAX_NAME) return i18n.t("topics.name.tooLong", { max: MAX_NAME });
  const bad = [...name].find((c) => !/[A-Za-z0-9._-]/.test(c));
  if (bad !== undefined) return i18n.t("topics.name.badChar", { char: bad });
  return null;
}

export function topicNameWarning(name: string): string | null {
  return name.includes(".") && name.includes("_")
    ? i18n.t("topics.name.dotUnderscore")
    : null;
}

export const TOPIC_PRESETS: { id: string; labelKey: string | null; label: string; configs: Record<string, string> }[] = [
  { id: "delete", labelKey: "topics.create.presetDelete", label: "delete", configs: {} },
  { id: "compact", labelKey: null, label: "compact", configs: { "cleanup.policy": "compact" } },
  { id: "compact-delete", labelKey: null, label: "compact + delete", configs: { "cleanup.policy": "compact,delete" } },
  { id: "test", labelKey: "topics.create.presetTest", label: "test", configs: { "retention.ms": "3600000" } },
];

/** Эквивалентная команда — для справки и копирования. */
export function buildCreateCommand(bootstrap: string, spec: NewTopicSpec): string {
  const parts = [
    "kafka-topics.sh",
    `--bootstrap-server ${bootstrap}`,
    "--create",
    `--topic ${spec.name}`,
    `--partitions ${spec.partitions}`,
  ];
  if (spec.replicationFactor > 0) parts.push(`--replication-factor ${spec.replicationFactor}`);
  for (const [k, v] of Object.entries(spec.configs)) parts.push(`--config ${k}=${v}`);
  return parts.join(" ");
}
