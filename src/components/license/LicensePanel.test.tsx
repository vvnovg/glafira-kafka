import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { LicenseStatus, PolicyStatus } from "../../api/types";
import { renderWithClient } from "../../test/render";

const FREE: LicenseStatus = { tier: "free", state: "none", source: null, licensee: null, kind: null, expires: null, daysLeft: null, error: null };
const PRO: LicenseStatus = { ...FREE, tier: "pro", state: "valid", source: "user", licensee: "a@b", kind: "personal", expires: "2027-03-31", daysLeft: 166 };
const UNMANAGED: PolicyStatus = { managed: false, source: null, updatesEnabled: true, updateUrl: null, hasLicenseKey: false, errors: [] };

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getLicenseStatus: vi.fn(),
      getPolicyStatus: vi.fn(),
      setLicenseKey: vi.fn(),
      removeLicenseKey: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { LicensePanel } from "./LicensePanel";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getPolicyStatus).mockResolvedValue(UNMANAGED);
});

describe("LicensePanel", () => {
  it("активация ключа", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    vi.mocked(api.setLicenseKey).mockResolvedValue(PRO);
    const user = userEvent.setup();
    renderWithClient(<LicensePanel />);
    expect(await screen.findByText("Free")).toBeInTheDocument();
    await user.type(screen.getByLabelText("Лицензионный ключ"), " a.b ");
    await user.click(screen.getByRole("button", { name: "Активировать" }));
    expect(api.setLicenseKey).toHaveBeenCalledWith("a.b");
    expect(await screen.findByText("Pro")).toBeInTheDocument();
    expect(screen.getByText("a@b")).toBeInTheDocument();
    expect(screen.getByText("2027-03-31")).toBeInTheDocument();
  });

  it("ошибка ключа показывается", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    vi.mocked(api.setLicenseKey).mockRejectedValue({ code: "INVALID_INPUT", message: "Ключ недействителен: подпись", retryable: false });
    const user = userEvent.setup();
    renderWithClient(<LicensePanel />);
    await user.type(await screen.findByLabelText("Лицензионный ключ"), "x.y");
    await user.click(screen.getByRole("button", { name: "Активировать" }));
    expect(await screen.findByText("Ключ недействителен: подпись")).toBeInTheDocument();
  });

  it("удаление ключа — с подтверждением на странице", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(PRO);
    vi.mocked(api.removeLicenseKey).mockResolvedValue(FREE);
    const user = userEvent.setup();
    renderWithClient(<LicensePanel />);
    await user.click(await screen.findByRole("button", { name: "Удалить ключ" }));
    expect(api.removeLicenseKey).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Да, удалить" }));
    expect(api.removeLicenseKey).toHaveBeenCalled();
  });

  it("лицензия организации: ввод недоступен", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue({ ...PRO, source: "policy", licensee: "ACME", kind: "organization" });
    vi.mocked(api.getPolicyStatus).mockResolvedValue({ ...UNMANAGED, managed: true, source: "/etc/x/policy.json", hasLicenseKey: true });
    renderWithClient(<LicensePanel />);
    expect(await screen.findByText("Лицензия управляется вашей организацией.")).toBeInTheDocument();
    expect(screen.queryByLabelText("Лицензионный ключ")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Удалить ключ" })).not.toBeInTheDocument();
  });

  it("истекает и истекла", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue({ ...PRO, state: "expiring", daysLeft: 3 });
    const { unmount } = renderWithClient(<LicensePanel />);
    expect(await screen.findByText(/истекает через 3 дн/)).toBeInTheDocument();
    unmount();
    vi.mocked(api.getLicenseStatus).mockResolvedValue({ ...PRO, tier: "free", state: "expired", expires: "2026-10-01", daysLeft: -15 });
    renderWithClient(<LicensePanel />);
    expect(await screen.findByText(/истекла 2026-10-01/)).toBeInTheDocument();
  });
});
