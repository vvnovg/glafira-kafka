import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
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
      listGroups: vi.fn().mockResolvedValue({ groups: [{ groupId: "svc", state: "Empty", isSimple: false }], errors: [] }),
    },
  };
});

import { MessageDetails } from "./MessageDetails";

const msg: BrowsedMessage = { partition: 2, offset: 41, timestamp: 0, key: null, value: null, headers: [], size: 0 };

describe("MessageDetails: сбросить группу сюда", () => {
  it("открывает вкладку группы с предзаполненным сбросом", async () => {
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    renderWithClient(<MessageDetails message={msg} utc={false} connectionId="c" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Сбросить группу сюда…" }));
    await user.type(screen.getByLabelText("Группа"), "svc");
    await user.click(screen.getByRole("button", { name: "Перейти к сбросу" }));
    const tab = useWorkspace.getState().tabs[0];
    expect(tab).toMatchObject({ kind: "group", connectionId: "c", groupId: "svc", resetPrefill: { topic: "orders", partition: 2, offset: 41 } });
  });

  it("скрыто для read-only подключения", async () => {
    renderWithClient(<MessageDetails message={msg} utc={false} connectionId="ro" topic="orders" />);
    await screen.findByText(/Партиция 2/);
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByRole("button", { name: "Сбросить группу сюда…" })).toBeNull();
  });
});
