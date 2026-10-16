export type LanguageCode = "en" | "ru" | "de" | "fr" | "es" | "it" | "pl" | "pt-BR";

export const LANGUAGES: { code: LanguageCode; nativeName: string; pro: boolean }[] = [
  { code: "en", nativeName: "English", pro: false },
  { code: "ru", nativeName: "Русский", pro: false },
  { code: "de", nativeName: "Deutsch", pro: true },
  { code: "fr", nativeName: "Français", pro: true },
  { code: "es", nativeName: "Español", pro: true },
  { code: "it", nativeName: "Italiano", pro: true },
  { code: "pl", nativeName: "Polski", pro: true },
  { code: "pt-BR", nativeName: "Português (Brasil)", pro: true },
];
