export type LanguageCode = "en" | "ru" | "de" | "fr" | "es" | "it" | "pl" | "pt-BR" | "zh-CN" | "zh-TW";

export const LANGUAGES: { code: LanguageCode; nativeName: string }[] = [
  { code: "en", nativeName: "English" },
  { code: "ru", nativeName: "Русский" },
  { code: "de", nativeName: "Deutsch" },
  { code: "fr", nativeName: "Français" },
  { code: "es", nativeName: "Español" },
  { code: "it", nativeName: "Italiano" },
  { code: "pl", nativeName: "Polski" },
  { code: "pt-BR", nativeName: "Português (Brasil)" },
  { code: "zh-CN", nativeName: "简体中文" },
  { code: "zh-TW", nativeName: "繁體中文" },
];
