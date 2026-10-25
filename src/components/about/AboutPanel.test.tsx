import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-dialog", () => ({ save: vi.fn().mockResolvedValue("/tmp/diag.zip") }));
vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));
vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getAppInfo: vi.fn().mockResolvedValue({ name: "X", version: "0.2.0", commit: "abc123", os: "macos", arch: "aarch64", dataDir: "/d", logDir: "/l" }),
      getPolicyStatus: vi.fn().mockResolvedValue({ managed: false, source: null, updatesEnabled: true, updateUrl: null, language: null, errors: [] }),
      exportDiagnostics: vi.fn().mockResolvedValue(undefined),
      checkUpdate: vi.fn().mockResolvedValue({ status: "upToDate" }),
      installUpdate: vi.fn(),
    },
  };
});

import { save } from "@tauri-apps/plugin-dialog";
import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import { i18n } from "../../i18n";
import { LICENSE_URL, SUPPORT_URL } from "../../lib/links";
import { useDiagnosticsRequest } from "../../state/diagnosticsRequest";
import { AboutPanel } from "./AboutPanel";

describe("AboutPanel", () => {
  it("версия, сборка, папка логов и диагностика", async () => {
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    expect(await screen.findByText("0.2.0")).toBeInTheDocument();
    expect(screen.getByText(/настольный клиент Apache Kafka/)).toBeInTheDocument();
    expect(screen.getByText("abc123")).toBeInTheDocument();
    expect(screen.getByText("/l")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Собрать диагностику" }));
    expect(api.exportDiagnostics).toHaveBeenCalledWith("/tmp/diag.zip");
    expect(await screen.findByText(/Архив сохранён/)).toBeInTheDocument();
  });

  it("строка лицензии и кнопка поддержки открывают страницы в браузере", async () => {
    vi.mocked(openUrl).mockClear();
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Apache 2.0" }));
    expect(openUrl).toHaveBeenCalledWith(LICENSE_URL);
    await user.click(screen.getByRole("button", { name: "Поддержать проект" }));
    expect(openUrl).toHaveBeenCalledWith(SUPPORT_URL);
    expect(screen.getByText(/пожертвования помогают/)).toBeInTheDocument();
    expect(screen.queryByText("Тариф")).not.toBeInTheDocument();
  });

  it("отмена диалога: экспорта и ошибки нет", async () => {
    vi.mocked(save).mockResolvedValueOnce(null);
    vi.mocked(api.exportDiagnostics).mockClear();
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Собрать диагностику" }));
    expect(api.exportDiagnostics).not.toHaveBeenCalled();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.queryByText(/Архив сохранён/)).not.toBeInTheDocument();
  });

  it("запрос из меню Help запускает сбор диагностики один раз", async () => {
    vi.mocked(save).mockClear();
    vi.mocked(api.exportDiagnostics).mockClear();
    useDiagnosticsRequest.getState().request();
    renderWithClient(<AboutPanel />);
    expect(await screen.findByText(/Архив сохранён/)).toBeInTheDocument();
    expect(save).toHaveBeenCalledTimes(1);
    expect(api.exportDiagnostics).toHaveBeenCalledWith("/tmp/diag.zip");
    expect(useDiagnosticsRequest.getState().pending).toBe(false);
  });

  it("запрос из меню во время сбора не запускает второй сбор", async () => {
    vi.mocked(save).mockClear();
    let resolveSave: (p: string) => void = () => {};
    vi.mocked(save).mockImplementationOnce(() => new Promise((r) => (resolveSave = r)));
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Собрать диагностику" }));
    expect(save).toHaveBeenCalledTimes(1);
    act(() => useDiagnosticsRequest.getState().request());
    expect(useDiagnosticsRequest.getState().pending).toBe(false);
    resolveSave("/tmp/diag.zip");
    expect(await screen.findByText(/Архив сохранён/)).toBeInTheDocument();
    expect(save).toHaveBeenCalledTimes(1);
  });

  it("обновление доступно; при активных сессиях — подтверждение", async () => {
    // Релиз без notes_i18n (0.5.0 и раньше): показывается обычный notes.
    vi.mocked(api.checkUpdate).mockResolvedValue({ status: "available", version: "0.3.0", notes: "Исправления", notesI18n: {}, date: null });
    vi.mocked(api.installUpdate)
      .mockRejectedValueOnce({ code: "CONFIRMATION_REQUIRED", message: "Идёт сессий: 1 …", retryable: false })
      .mockResolvedValueOnce(undefined);
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Проверить обновления" }));
    expect(await screen.findByText("Доступна версия 0.3.0")).toBeInTheDocument();
    expect(await screen.findByText("Исправления")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Установить и перезапустить" }));
    expect(api.installUpdate).toHaveBeenLastCalledWith(false);
    expect(await screen.findByText(/Идёт сессий: 1/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Остановить сессии и обновить" }));
    expect(api.installUpdate).toHaveBeenLastCalledWith(true);
  });

  it("заметки к обновлению — Markdown на языке интерфейса, ссылки — в системный браузер", async () => {
    vi.mocked(openUrl).mockClear();
    vi.mocked(api.checkUpdate).mockResolvedValue({
      status: "available",
      version: "0.3.0",
      notes: "- **Fixes**",
      notesI18n: { en: "- **Fixes**", ru: "- **Исправления** для `acks`\n- [Подробнее](https://example.com/r)" },
      date: null,
    });
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Проверить обновления" }));
    const notes = await screen.findByRole("region", { name: "Что нового в версии 0.3.0" });
    expect((await screen.findByText("Исправления")).tagName).toBe("STRONG");
    expect(screen.getByText("acks").tagName).toBe("CODE");
    expect(notes.querySelectorAll("li")).toHaveLength(2);
    expect(notes.textContent).not.toContain("**");
    expect(screen.queryByText(/Fixes/)).not.toBeInTheDocument();
    await user.click(screen.getByRole("link", { name: "Подробнее" }));
    expect(openUrl).toHaveBeenCalledWith("https://example.com/r");
  });

  it("смена языка меняет заметки без новой проверки; нет перевода — английские", async () => {
    vi.mocked(api.checkUpdate).mockClear();
    vi.mocked(api.checkUpdate).mockResolvedValue({
      status: "available",
      version: "0.3.0",
      notes: "- Fixes",
      notesI18n: { en: "- Fixes", ru: "- Исправления", de: "- Korrekturen" },
      date: null,
    });
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Проверить обновления" }));
    expect(await screen.findByText("Исправления")).toBeInTheDocument();
    try {
      await act(() => i18n.changeLanguage("de"));
      expect(await screen.findByText("Korrekturen")).toBeInTheDocument();
      await act(() => i18n.changeLanguage("ja"));
      expect(await screen.findByText("Fixes")).toBeInTheDocument();
      expect(api.checkUpdate).toHaveBeenCalledTimes(1);
    } finally {
      await act(() => i18n.changeLanguage("ru"));
    }
  });

  it("политика выключила обновления", async () => {
    vi.mocked(api.checkUpdate).mockResolvedValue({
      status: "disabled",
      reason: { key: "errors.updates.managed", params: {}, text: "Updates are managed by your organization" },
    });
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Проверить обновления" }));
    expect(await screen.findByText("Обновления управляются вашей организацией")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Установить и перезапустить" })).not.toBeInTheDocument();
  });

  it("последняя версия; ошибка сети — баннер с повтором", async () => {
    vi.mocked(api.checkUpdate)
      .mockRejectedValueOnce({ code: "CONNECTION_FAILED", message: "Не удалось проверить обновления", retryable: true })
      .mockResolvedValueOnce({ status: "upToDate" });
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Проверить обновления" }));
    expect(await screen.findByText("Не удалось проверить обновления")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("Установлена последняя версия")).toBeInTheDocument();
  });

  it("ошибки политики видны в «О программе»", async () => {
    vi.mocked(api.getPolicyStatus).mockResolvedValueOnce({
      managed: true,
      source: "/etc/x/policy.json",
      updatesEnabled: false,
      updateUrl: null,
      language: null,
      errors: [
        {
          key: "errors.policy.httpsRequired",
          params: { key: "updateUrl" },
          text: "updateUrl: an https:// address is required — updates are not downloaded over http",
        },
      ],
    });
    renderWithClient(<AboutPanel />);
    expect(await screen.findByText(/updateUrl: нужен адрес https/)).toBeInTheDocument();
    expect(screen.getByText("задана")).toBeInTheDocument();
  });
});
