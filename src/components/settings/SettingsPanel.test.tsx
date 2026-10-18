import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LanguageSettings } from "../../api/types";
import { i18n } from "../../i18n";
import { mcpStatus } from "../../test/mcp";
import { renderWithClient } from "../../test/render";
import { useHelpContext } from "../../state/helpContext";

const base: LanguageSettings = {
  system: "ru-RU", systemMatch: "ru", chosen: null, policy: null, effective: "ru", reason: "system",
  languages: ["en", "ru", "de", "fr", "es", "it", "pl", "pt-BR", "zh-CN", "zh-TW", "ja", "ko"],
};

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getLanguageSettings: vi.fn(),
      setLanguage: vi.fn(),
      getMcpStatus: vi.fn().mockResolvedValue(mcpStatus({ state: "off", enabled: false })),
      getMcpClientConfigs: vi.fn().mockResolvedValue({ command: "/x", args: ["mcp"], claudeDesktop: "{}", claudeCode: "claude", otherClient: "/x mcp" }),
      getMcpAudit: vi.fn().mockResolvedValue([]),
    },
  };
});

import { api } from "../../api/client";
import { SettingsPanel } from "./SettingsPanel";

beforeEach(() => {
  vi.clearAllMocks();
});

// Выбор «en» в тесте переключает глобальный язык; сброс идёт в act, пока компоненты ещё
// смонтированы (useTranslation обновляет их по languageChanged).
afterEach(() => act(() => i18n.changeLanguage("ru")));

// SettingsPanel монтирует асинхронный раздел MCP: дожидаемся, пока его запросы
// (статус, фрагменты конфига, журнал) отработают, чтобы тест не кончился раньше.
async function mcpSettled() {
  await screen.findByLabelText("Разрешить MCP-сервер");
  await screen.findByText("claude");
  await screen.findByText("Журнал пуст.");
}

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
    await mcpSettled();
    await user.selectOptions(select, "en");
    expect(api.setLanguage).toHaveBeenCalledWith("en");
  });

  it("язык задан организацией", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue({ ...base, policy: "ru", reason: "policy" });
    renderWithClient(<SettingsPanel />);
    expect(await screen.findByLabelText("Язык интерфейса")).toBeDisabled();
    expect(screen.getByText(/задан вашей организацией/)).toBeInTheDocument();
    await mcpSettled();
  });

  it("Free выбирает Deutsch — язык применяется, подсказки о Pro нет", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    vi.mocked(api.setLanguage).mockResolvedValue({ ...base, chosen: "de", effective: "de", reason: "user" });
    const user = userEvent.setup();
    renderWithClient(<SettingsPanel />);
    const select = await screen.findByLabelText("Язык интерфейса");
    await mcpSettled();
    await user.selectOptions(select, "de");
    expect(api.setLanguage).toHaveBeenCalledWith("de");
    await vi.waitFor(() => expect(i18n.language).toBe("de"));
    expect(screen.queryByText(/\bPro\b/)).not.toBeInTheDocument();
  });

  it("F1 в разделе MCP открывает главу «LLM-ассистенты»", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    renderWithClient(<SettingsPanel />);
    expect(useHelpContext.getState().byTab.settings).toBe("getting-started");
    const mcp = await screen.findByLabelText("Разрешить MCP-сервер");
    act(() => mcp.focus());
    expect(useHelpContext.getState().byTab.settings).toBe("llm-mcp");
    act(() => screen.getByLabelText("Язык интерфейса").focus());
    expect(useHelpContext.getState().byTab.settings).toBe("getting-started");
    await mcpSettled();
  });
});
