import { i18n } from "../../i18n";
import type { HeaderInput, PayloadInput, ProduceMessage, ProducerSettings } from "../../api/types";

export interface ProducerForm {
  topic: string;
  /** "" — авто (партиционер) */
  partition: string;
  key: PayloadInput;
  value: PayloadInput;
  headers: HeaderInput[];
  settings: ProducerSettings;
}

export const DEFAULT_SETTINGS: ProducerSettings = { acks: "all", compression: "none", idempotence: false };

export function emptyProducerForm(topic = ""): ProducerForm {
  return {
    topic,
    partition: "",
    key: { format: "string", text: "" },
    value: { format: "json", text: "" },
    headers: [],
    settings: DEFAULT_SETTINGS,
  };
}

/** Проверки формы до отправки; содержимое форматов и плейсхолдеры проверяет Rust. */
export function formToMessage(f: ProducerForm): ProduceMessage {
  const topic = f.topic.trim();
  if (!topic) throw new Error(i18n.t("producer.form.errTopic"));
  let partition: number | null = null;
  const p = f.partition.trim();
  if (p) {
    if (!/^\d+$/.test(p)) throw new Error(i18n.t("producer.form.errPartition"));
    partition = Number(p);
  }
  return {
    topic,
    partition,
    key: f.key,
    value: f.value,
    headers: f.headers.filter((h) => h.name.trim()).map((h) => ({ ...h, name: h.name.trim() })),
  };
}

export function messageToForm(m: ProduceMessage, settings: ProducerSettings = DEFAULT_SETTINGS): ProducerForm {
  return {
    topic: m.topic,
    partition: m.partition === null ? "" : String(m.partition),
    key: m.key,
    value: m.value,
    headers: m.headers,
    settings,
  };
}
