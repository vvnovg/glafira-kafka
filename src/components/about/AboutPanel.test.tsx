import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-dialog", () => ({ save: vi.fn().mockResolvedValue("/tmp/diag.zip") }));
vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getAppInfo: vi.fn().mockResolvedValue({ name: "X", version: "0.2.0", commit: "abc123", os: "macos", arch: "aarch64", dataDir: "/d", logDir: "/l" }),
      getLicenseStatus: vi.fn().mockResolvedValue({ tier: "free", state: "none", source: null, licensee: null, kind: null, expires: null, daysLeft: null, error: null }),
      getPolicyStatus: vi.fn().mockResolvedValue({ managed: false, source: null, updatesEnabled: true, updateUrl: null, hasLicenseKey: false, errors: [] }),
      exportDiagnostics: vi.fn().mockResolvedValue(undefined),
      checkUpdate: vi.fn().mockResolvedValue({ status: "upToDate" }),
      installUpdate: vi.fn(),
    },
  };
});

import { save } from "@tauri-apps/plugin-dialog";
import { api } from "../../api/client";
import { AboutPanel } from "./AboutPanel";

describe("AboutPanel", () => {
  it("версия, сборка, папка логов и диагностика", async () => {
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    expect(await screen.findByText("0.2.0")).toBeInTheDocument();
    expect(screen.getByText("abc123")).toBeInTheDocument();
    expect(screen.getByText("/l")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Собрать диагностику" }));
    expect(api.exportDiagnostics).toHaveBeenCalledWith("/tmp/diag.zip");
    expect(await screen.findByText(/Архив сохранён/)).toBeInTheDocument();
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

  it("обновление доступно; при активных сессиях — подтверждение", async () => {
    vi.mocked(api.checkUpdate).mockResolvedValue({ status: "available", version: "0.3.0", notes: "Исправления", date: null });
    vi.mocked(api.installUpdate)
      .mockRejectedValueOnce({ code: "CONFIRMATION_REQUIRED", message: "Идёт сессий: 1 …", retryable: false })
      .mockResolvedValueOnce(undefined);
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Проверить обновления" }));
    expect(await screen.findByText("Доступна версия 0.3.0")).toBeInTheDocument();
    expect(screen.getByText("Исправления")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Установить и перезапустить" }));
    expect(api.installUpdate).toHaveBeenLastCalledWith(false);
    expect(await screen.findByText(/Идёт сессий: 1/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Остановить сессии и обновить" }));
    expect(api.installUpdate).toHaveBeenLastCalledWith(true);
  });

  it("политика выключила обновления", async () => {
    vi.mocked(api.checkUpdate).mockResolvedValue({ status: "disabled", reason: "Обновления управляются вашей организацией" });
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
      hasLicenseKey: false,
      errors: ["updateUrl: нужен адрес https:// — обновления по http не загружаются"],
    });
    renderWithClient(<AboutPanel />);
    expect(await screen.findByText(/нужен адрес https/)).toBeInTheDocument();
    expect(screen.getByText("задана")).toBeInTheDocument();
  });
});
