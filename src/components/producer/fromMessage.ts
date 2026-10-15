import type { BrowsedMessage, DecodedData, PayloadInput, ProduceMessage } from "../../api/types";

/**
 * Текст, который Rust-шаблонизатор (`render`) вернёт без изменений: каждое `{{` экранируется
 * как `\{{`. Грамматика обратима — буквальный `\{{` превращается в `\\{{` и отрисовывается
 * обратно в `\{{`. Так «Отправить повторно» и «Повторить» отправляют точную копию: чужие
 * `{{uuid}}`/`{{env NAME}}` из прочитанных сообщений и отрисованной истории не подставляются.
 */
export function literalText(text: string): string {
  return text.replaceAll("{{", "\\{{");
}

/** Сообщение, все тексты которого отправятся буквально (см. literalText). */
export function literalMessage(m: ProduceMessage): ProduceMessage {
  return {
    ...m,
    key: { ...m.key, text: literalText(m.key.text) },
    value: { ...m.value, text: literalText(m.value.text) },
    headers: m.headers.map((h) => ({ ...h, value: literalText(h.value) })),
  };
}

function payload(d: DecodedData | null): PayloadInput {
  if (!d) return { format: "null", text: "" };
  if (d.truncated) throw new Error("Значение больше 1 МБ показано не полностью — отправить его повторно нельзя");
  // DataFormat = auto | string | json | hex | base64: форматов реестра схем нет,
  // поэтому все значения воспроизводятся побайтно. "auto" в ответе не встречается.
  let format = d.format === "auto" ? "string" : d.format;
  if (format === "string" || format === "json") {
    // Rust декодирует через from_utf8_lossy: U+FFFD означает потерю исходных байт.
    if (d.text.includes("\uFFFD")) {
      throw new Error("Значение декодировано с потерями (не UTF-8) — переключите формат на hex/base64, чтобы отправить точную копию");
    }
    // Невалидный JSON (d.error): текст — те же байты, но Producer отклонит формат json,
    // поэтому отправляем как string — байты сохраняются точно.
    if (format === "json" && d.error) format = "string";
  }
  return { format, text: d.text };
}

/**
 * Сообщение из Browse → форма Producer (партицию выбирает партиционер). Тексты экранированы
 * (literalText): байты прочитанного сообщения не проходят через шаблонизатор.
 */
export function browsedToMessage(m: BrowsedMessage): ProduceMessage {
  return literalMessage({
    topic: m.topic,
    partition: null,
    key: payload(m.key),
    value: payload(m.value),
    headers: m.headers.map((h) => {
      // HeaderInput не умеет null: null-значение превращается в пустую строку.
      const v = h.value ?? "";
      // header_text в Rust помечает не-UTF-8 значения префиксом 0x + hex. Неоднозначность остаётся:
      // UTF-8 заголовок с буквальным текстом «0xdeadbeef» неотличим от бинарного и уйдёт как hex.
      return /^0x([0-9a-f]{2})+$/i.test(v)
        ? { name: h.name, value: v.slice(2), format: "hex" as const }
        : { name: h.name, value: v, format: "string" as const };
    }),
  });
}
