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
});
