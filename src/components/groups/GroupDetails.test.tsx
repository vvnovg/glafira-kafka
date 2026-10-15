import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile, GroupDescription, GroupPartitionOffset } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";
import { groupTotalLag } from "./groupLag";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const offsets: GroupPartitionOffset[] = [
  { topic: "orders", partition: 0, committed: 2, logEnd: 5, lag: 3, memberClientId: "svc-1" },
  { topic: "orders", partition: 1, committed: null, logEnd: 4, lag: null, memberClientId: "svc-1" },
];
const stable: GroupDescription = {
  groupId: "g",
  state: "Stable",
  assignor: "range",
  members: [{ memberId: "m1", clientId: "svc-1", host: "/10.0.0.1", assignment: [{ topic: "orders", partition: 0 }, { topic: "orders", partition: 1 }] }],
};

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      describeGroups: vi.fn(),
      getGroupOffsets: vi.fn(),
      deleteGroup: vi.fn().mockResolvedValue(undefined),
    },
  };
});

import { api } from "../../api/client";
import { GroupDetails } from "./GroupDetails";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listConnections).mockResolvedValue([dev]);
  vi.mocked(api.getGroupOffsets).mockResolvedValue(offsets);
});

describe("groupTotalLag", () => {
  it("сумма по партициям с коммитом, ошибка — неизвестно", () => {
    expect(groupTotalLag(offsets)).toBe(3);
    expect(groupTotalLag([...offsets, { ...offsets[0], partition: 2, lag: null, logEnd: null, error: "нет лидера" }])).toBeNull();
  });
});

describe("GroupDetails", () => {
  it("показывает партиции, участников и запрещает удаление активной группы", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([stable]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    expect(await screen.findByText("нет коммита")).toBeInTheDocument();
    expect(screen.getByText("/10.0.0.1")).toBeInTheDocument();
    expect(screen.getByText("Суммарный лаг: 3")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Удалить группу" })).toBeDisabled();
  });

  it("GROUP_NOT_EMPTY показывает детали и не закрывает вкладку", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
    vi.mocked(api.deleteGroup).mockRejectedValueOnce({ code: "GROUP_NOT_EMPTY", message: "Группа не пуста", details: "member m9" });
    useWorkspace.setState({ tabs: [], activeId: null });
    useWorkspace.getState().open({ kind: "group", connectionId: "c", groupId: "g" });
    const user = userEvent.setup();
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    await user.click(await screen.findByRole("button", { name: "Удалить группу" }));
    await user.type(screen.getByLabelText("Подтверждение: id группы"), "g");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(await screen.findByText("member m9")).toBeInTheDocument();
    expect(useWorkspace.getState().tabs).toHaveLength(1);
  });

  it("read-only скрывает удаление", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([{ ...dev, readOnly: true }]);
    vi.mocked(api.describeGroups).mockResolvedValue([stable]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    await screen.findByText("нет коммита");
    expect(screen.queryByRole("button", { name: "Удалить группу" })).toBeNull();
  });

  it("удаляет пустую группу после ввода id и закрывает вкладку", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
    useWorkspace.setState({ tabs: [], activeId: null });
    useWorkspace.getState().open({ kind: "group", connectionId: "c", groupId: "g" });
    const user = userEvent.setup();
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    await user.click(await screen.findByRole("button", { name: "Удалить группу" }));
    await user.type(screen.getByLabelText("Подтверждение: id группы"), "g");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(api.deleteGroup).toHaveBeenCalledWith("c", "g", { confirm: null, typedName: "g" });
    await vi.waitFor(() => expect(useWorkspace.getState().tabs).toHaveLength(0));
  });

  it("пустой ответ describeGroups даёт понятную ошибку", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    expect(await screen.findByText("Брокер не вернул описание группы")).toBeInTheDocument();
  });

  it("отмена удаления сбрасывает устаревшую ошибку GROUP_NOT_EMPTY", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
    vi.mocked(api.deleteGroup).mockRejectedValueOnce({ code: "GROUP_NOT_EMPTY", message: "Группа не пуста", details: "member m9" });
    const user = userEvent.setup();
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    await user.click(await screen.findByRole("button", { name: "Удалить группу" }));
    await user.type(screen.getByLabelText("Подтверждение: id группы"), "g");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(await screen.findByText("member m9")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отмена" }));
    await user.click(screen.getByRole("button", { name: "Удалить группу" }));
    expect(screen.queryByText("member m9")).toBeNull();
  });
});
