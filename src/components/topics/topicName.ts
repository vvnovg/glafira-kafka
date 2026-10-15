import type { NewTopicSpec } from "../../api/types";

const MAX_NAME = 249;

/** Те же правила, что в Rust (`kafka::admin::validate_topic_name`): ошибка — сразу у поля. */
export function validateTopicName(name: string): string | null {
  if (!name) return "Введите имя топика";
  if (name === "." || name === "..") return "Имя топика не может быть «.» или «..»";
  if (name.length > MAX_NAME) return `Имя топика длиннее ${MAX_NAME} символов`;
  const bad = [...name].find((c) => !/[A-Za-z0-9._-]/.test(c));
  if (bad !== undefined) return `Недопустимый символ «${bad}»: разрешены латинские буквы, цифры, «.», «_» и «-»`;
  return null;
}

export function topicNameWarning(name: string): string | null {
  return name.includes(".") && name.includes("_")
    ? "В имени есть и «.», и «_»: имена метрик таких топиков могут совпасть с другими"
    : null;
}

export const TOPIC_PRESETS: { id: string; label: string; configs: Record<string, string> }[] = [
  { id: "delete", label: "delete (по умолчанию)", configs: {} },
  { id: "compact", label: "compact", configs: { "cleanup.policy": "compact" } },
  { id: "compact-delete", label: "compact + delete", configs: { "cleanup.policy": "compact,delete" } },
  { id: "test", label: "тестовый (retention 1 ч)", configs: { "retention.ms": "3600000" } },
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
