import { i18n } from "./index";

type Localizable = { key?: string; params?: Record<string, string>; text?: string; message?: string };

/** Перевод текста из Rust: по ключу, а если ключа нет в словаре — английский текст как есть. */
export function tr(t: Localizable): string {
  if (t.key && i18n.exists(t.key)) return i18n.t(t.key, t.params ?? {});
  return t.text ?? t.message ?? "";
}
