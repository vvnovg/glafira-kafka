import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LanguageSettings, LicenseStatus } from "../../api/types";
import { i18n } from "../../i18n";
import { renderWithClient } from "../../test/render";

const base: LanguageSettings = {
  system: "ru-RU", systemMatch: "ru", chosen: null, policy: null, effective: "ru", reason: "system",
  languages: ["en", "ru", "de", "fr", "es", "it", "pl", "pt-BR"],
};
const FREE = { tier: "free", state: "none", source: null, licensee: null, kind: null, expires: null, daysLeft: null, error: null } as LicenseStatus;

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getLanguageSettings: vi.fn(), setLanguage: vi.fn(), getLicenseStatus: vi.fn() } };
});

import { api } from "../../api/client";
import { SettingsPanel } from "./SettingsPanel";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
});

afterEach(() => i18n.changeLanguage("ru")); // выбор «en» в тесте переключает глобальный язык

describe("SettingsPanel", () => {
  it("выбор языка; без лицензии доступны все языки", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    vi.mocked(api.setLanguage).mockResolvedValue({ ...base, chosen: "en", effective: "en", reason: "user" });
    const user = userEvent.setup();
    renderWithClient(<SettingsPanel />);
    const select = await screen.findByLabelText("Язык интерфейса");
    for (const name of ["English", "Русский", "Deutsch", "Français", "Español", "Italiano", "Polski", "Português (Brasil)"]) {
      const option = screen.getByRole("option", { name });
      expect(option).toBeEnabled();
      expect(option.textContent).not.toMatch(/Pro/);
    }
    expect(screen.getByRole("option", { name: /Как в системе/ })).toBeInTheDocument();
    await user.selectOptions(select, "en");
    expect(api.setLanguage).toHaveBeenCalledWith("en");
  });

  it("язык задан организацией", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue({ ...base, policy: "ru", reason: "policy" });
    renderWithClient(<SettingsPanel />);
    expect(await screen.findByLabelText("Язык интерфейса")).toBeDisabled();
    expect(screen.getByText(/задан вашей организацией/)).toBeInTheDocument();
  });

  it("Free выбирает Deutsch — язык применяется, подсказки о Pro нет", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    vi.mocked(api.setLanguage).mockResolvedValue({ ...base, chosen: "de", effective: "de", reason: "user" });
    const user = userEvent.setup();
    renderWithClient(<SettingsPanel />);
    await user.selectOptions(await screen.findByLabelText("Язык интерфейса"), "de");
    expect(api.setLanguage).toHaveBeenCalledWith("de");
    await vi.waitFor(() => expect(i18n.language).toBe("de"));
    expect(screen.queryByText(/Pro/)).not.toBeInTheDocument();
  });
});
