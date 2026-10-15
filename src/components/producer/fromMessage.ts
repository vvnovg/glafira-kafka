import type { BrowsedMessage, DecodedData, PayloadInput, ProduceMessage } from "../../api/types";

function payload(d: DecodedData | null): PayloadInput {
  if (!d) return { format: "null", text: "" };
  if (d.truncated) throw new Error("Значение больше 1 МБ показано не полностью — отправить его повторно нельзя");
  // DataFormat = auto | string | json | hex | base64: форматов реестра схем нет,
  // поэтому все значения воспроизводятся побайтно. "auto" в ответе не встречается.
  const format = d.format === "auto" ? "string" : d.format;
  return { format, text: d.text };
}

/** Сообщение из Browse → форма Producer (партицию выбирает партиционер). */
export function browsedToMessage(m: BrowsedMessage): ProduceMessage {
  return {
    topic: m.topic,
    partition: null,
    key: payload(m.key),
    value: payload(m.value),
    headers: m.headers.map((h) => {
      const v = h.value ?? "";
      // header_text в Rust помечает не-UTF-8 значения префиксом 0x.
      return /^0x[0-9a-f]*$/i.test(v) && v.length > 2
        ? { name: h.name, value: v.slice(2), format: "hex" as const }
        : { name: h.name, value: v, format: "string" as const };
    }),
  };
}
