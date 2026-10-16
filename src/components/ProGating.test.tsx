import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { LicenseStatus } from "../api/types";
import { renderWithClient } from "../test/render";
import { newProfile } from "./connections/profile";

const FREE: LicenseStatus = { tier: "free", state: "none", source: null, licensee: null, kind: null, expires: null, daysLeft: null, error: null };
const PRO: LicenseStatus = { ...FREE, tier: "pro", state: "valid", source: "user", licensee: "a@b", kind: "personal" };

vi.mock("../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../api/client")>();
  return {
    ...orig,
    api: {
      getLicenseStatus: vi.fn(),
      listConnections: vi.fn().mockResolvedValue([{ ...newProfile(), id: "c", name: "local" }]),
    },
  };
});

import { api } from "../api/client";
import { ConnectionForm } from "./connections/ConnectionForm";
import { Sidebar } from "./Sidebar";

beforeEach(() => vi.clearAllMocks());

describe("Free / Pro", () => {
  it("Free: consumer-сессия в дереве недоступна, рядом замок", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    renderWithClient(<Sidebar />);
    const link = await screen.findByRole("button", { name: /Consumer-сессия/ });
    await vi.waitFor(() => expect(link).toBeDisabled());
    expect(link).toHaveAttribute("title", expect.stringContaining("Pro"));
    expect(screen.getByText("Лицензия: Free")).toBeInTheDocument();
  });

  it("Pro: consumer-сессия доступна", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(PRO);
    renderWithClient(<Sidebar />);
    expect(await screen.findByText("Лицензия: Pro")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Consumer-сессия/ })).toBeEnabled();
  });

  it("Free: окружение prod нельзя выбрать для нового подключения", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    const option = await screen.findByRole("option", { name: "prod (Pro)" });
    await vi.waitFor(() => expect(option).toBeDisabled());
  });

  it("Free: существующий prod-профиль остаётся prod", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={{ ...newProfile(), id: "p", name: "p", environment: "prod" }} tabId="t" />);
    expect(screen.getByLabelText("Окружение")).toHaveValue("prod");
    await user.selectOptions(screen.getByLabelText("Окружение"), "dev");
    await user.selectOptions(screen.getByLabelText("Окружение"), "prod");
    expect(screen.getByLabelText("Окружение")).toHaveValue("prod");
  });
});
