import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LanguageSettings, LicenseStatus } from "../../api/types";
import { i18n } from "../../i18n";
import { renderWithClient } from "../../test/render";

const base: LanguageSettings = {
  system: "ru-RU", systemMatch: "ru", chosen: null, policy: null, effective: "ru", reason: "system",
  proLanguages: ["de", "fr", "es", "it", "pl", "pt-BR"], languages: ["en", "ru", "de", "fr", "es", "it", "pl", "pt-BR"],
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
  it("выбор языка; Pro-языки без лицензии недоступны", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    vi.mocked(api.setLanguage).mockResolvedValue({ ...base, chosen: "en", effective: "en", reason: "user" });
    const user = userEvent.setup();
    renderWithClient(<SettingsPanel />);
    const select = await screen.findByLabelText("Язык интерфейса");
    expect(screen.getByRole("option", { name: /Deutsch/ })).toBeDisabled();
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

  it("английский вместо Pro-языка — причина видна", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue({ ...base, chosen: "de", effective: "en", reason: "proRequired" });
    renderWithClient(<SettingsPanel />);
    expect(await screen.findByText(/доступен в Pro/)).toBeInTheDocument();
  });
});
