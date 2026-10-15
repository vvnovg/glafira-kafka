import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { BrowsedMessage } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn().mockResolvedValue([
        { ...newProfile(), id: "c", name: "dev", environment: "dev" },
        { ...newProfile(), id: "ro", name: "prod", environment: "prod", readOnly: true },
      ]),
      listGroups: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { MessageDetails } from "./MessageDetails";

const okGroups = { groups: [{ groupId: "svc", state: "Empty", isSimple: false }], errors: [] as string[] };

const msg: BrowsedMessage = { partition: 2, offset: 41, timestamp: 0, key: null, value: null, headers: [], size: 0 };

describe("MessageDetails: сбросить группу сюда", () => {
  beforeEach(() => {
    vi.mocked(api.listGroups).mockReset();
    vi.mocked(api.listGroups).mockResolvedValue(okGroups);
  });

  it("открывает вкладку группы с предзаполненным сбросом", async () => {
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    renderWithClient(<MessageDetails message={msg} utc={false} connectionId="c" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Сбросить группу сюда…" }));
    const go = screen.getByRole("button", { name: "Перейти к сбросу" });
    expect(go).toBeDisabled();
    await user.selectOptions(await screen.findByLabelText("Группа"), "svc");
    expect(go).toBeEnabled();
    await user.click(go);
    const tab = useWorkspace.getState().tabs[0];
    expect(tab).toMatchObject({ kind: "group", connectionId: "c", groupId: "svc", resetPrefill: { topic: "orders", partition: 2, offset: 41 } });
  });

  it("нельзя выбрать неизвестную группу", async () => {
    const user = userEvent.setup();
    renderWithClient(<MessageDetails message={msg} utc={false} connectionId="c" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Сбросить группу сюда…" }));
    const select = await screen.findByLabelText("Группа");
    expect(screen.queryByRole("option", { name: "typo" })).toBeNull();
    expect(select).toHaveValue("");
    expect(screen.getByRole("button", { name: "Перейти к сбросу" })).toBeDisabled();
  });

  it("при ошибке listGroups показывает ошибку с повтором", async () => {
    vi.mocked(api.listGroups).mockRejectedValueOnce({ code: "TIMEOUT", message: "нет ответа", retryable: true });
    const user = userEvent.setup();
    renderWithClient(<MessageDetails message={msg} utc={false} connectionId="c" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Сбросить группу сюда…" }));
    expect(await screen.findByText("нет ответа")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByLabelText("Группа")).toBeInTheDocument();
  });

  it("показывает частичные ошибки списка групп", async () => {
    vi.mocked(api.listGroups).mockResolvedValue({ ...okGroups, errors: ["broker 2 недоступен"] });
    const user = userEvent.setup();
    renderWithClient(<MessageDetails message={msg} utc={false} connectionId="c" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Сбросить группу сюда…" }));
    expect(await screen.findByText(/broker 2 недоступен/)).toBeInTheDocument();
  });

  it("скрыто для read-only подключения", async () => {
    renderWithClient(<MessageDetails message={msg} utc={false} connectionId="ro" topic="orders" />);
    await screen.findByText(/Партиция 2/);
    await waitFor(() => expect(api.listConnections).toHaveBeenCalled());
    await waitFor(() => expect(screen.queryByRole("button", { name: "Сбросить группу сюда…" })).toBeNull());
  });
});
