import { screen, waitFor } from "@testing-library/react";
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

const resetProposal = {
  proposal: {
    id: "p1",
    severity: "warning" as const,
    title: "Replay orders",
    rationale: "r",
    target: { kind: "group" as const, name: "g" },
    change: { kind: "resetOffsets" as const, groupId: "g", strategy: { type: "latest" as const }, scope: { type: "all" as const } },
    client: "claude-code",
    createdAtMs: 1,
    updatedAtMs: 1,
    status: "open" as const,
  },
  nonce: 1,
};

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      describeGroups: vi.fn(),
      getGroupOffsets: vi.fn(),
      previewOffsetReset: vi.fn(),
      resetOffsets: vi.fn(),
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
    expect(groupTotalLag([...offsets, { ...offsets[0], partition: 2, lag: null, logEnd: null, error: { key: "errors.generic", params: { message: "нет лидера" }, text: "нет лидера" } }])).toBeNull();
    expect(groupTotalLag([offsets[1]])).toBeNull();
    expect(groupTotalLag([])).toBeNull();
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

  it("координатор, протокол и group.instance.id участников", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([
      {
        ...stable,
        groupType: "classic",
        coordinator: { id: 2, host: "kafka-2", port: 9092 },
        members: [{ ...stable.members[0], groupInstanceId: "pod-0" }],
      },
    ]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    expect(await screen.findByText("Координатор: брокер 2 (kafka-2:9092) · протокол: classic")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "group.instance.id" })).toBeInTheDocument();
    expect(screen.getByText("pod-0")).toBeInTheDocument();
  });

  it("без static membership колонки group.instance.id нет", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([stable]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    expect(await screen.findByText("/10.0.0.1")).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "group.instance.id" })).not.toBeInTheDocument();
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
    const join = screen.getByRole("button", { name: "Читать как участник" });
    await waitFor(() => expect(join).toBeDisabled());
    expect(join).toHaveAttribute("title", expect.stringContaining("только для чтения"));
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

  it("сброс оффсетов доступен только для Empty/Dead", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([stable]);
    const { unmount } = renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    await screen.findByText("нет коммита");
    const btn = screen.getByRole("button", { name: "Сбросить оффсеты" });
    expect(btn).toBeDisabled();
    await vi.waitFor(() =>
      expect(btn).toHaveAttribute("title", "Сброс возможен только для группы без участников (сейчас: Stable)"),
    );
    unmount();
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Dead", members: [] }]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    await vi.waitFor(() => expect(screen.getByRole("button", { name: "Сбросить оффсеты" })).toBeEnabled());
  });

  it("открытая панель сброса остаётся, но «Применить» блокируется, когда группа перестала быть пустой", async () => {
    const user = userEvent.setup();
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
    vi.mocked(api.previewOffsetReset).mockResolvedValue([{ topic: "orders", partition: 0, current: 2, target: 0, low: 0, high: 5 }]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    await user.click(await screen.findByRole("button", { name: "Сбросить оффсеты" }));
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    expect(await screen.findByRole("button", { name: "Применить (1)" })).toBeEnabled();
    vi.mocked(api.describeGroups).mockResolvedValue([stable]);
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    const apply = await screen.findByRole("button", { name: "Применить (1)" });
    await vi.waitFor(() => expect(apply).toBeDisabled());
    expect(apply).toHaveAttribute("title", "Сброс возможен только для группы без участников (сейчас: Stable)");
    expect(screen.getByText("Сброс возможен только для группы без участников (сейчас: Stable)")).toBeInTheDocument();
  });

  it("отказ GROUP_NOT_EMPTY при применении предложения перечитывает группу: «Применить» блокируется без «Обновить»", async () => {
    const user = userEvent.setup();
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
    vi.mocked(api.previewOffsetReset).mockResolvedValue([{ topic: "orders", partition: 0, current: 2, target: 5, low: 0, high: 5 }]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" proposal={resetProposal} />);
    await user.click(await screen.findByRole("button", { name: "Предпросмотр" }));
    const apply = await screen.findByRole("button", { name: "Применить (1)" });
    await vi.waitFor(() => expect(apply).toBeEnabled());
    // Пока человек смотрел предпросмотр, в группу вошёл консьюмер: бэкенд перепроверил состав и отказал.
    vi.mocked(api.describeGroups).mockResolvedValue([stable]);
    vi.mocked(api.resetOffsets).mockRejectedValueOnce({ code: "GROUP_NOT_EMPTY", message: "Группа не пуста", details: "Active members: svc-1 (/10.0.0.1)" });
    await user.click(apply);
    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "orders", partition: 0, offset: 5 }], null, {
      id: "p1",
      request: { groupId: "g", strategy: { type: "latest" }, scope: { type: "all" } },
    });
    expect(await screen.findByText("Active members: svc-1 (/10.0.0.1)")).toBeInTheDocument();
    await vi.waitFor(() => expect(screen.getByRole("button", { name: "Применить (1)" })).toBeDisabled());
    expect(screen.getByText("Сброс возможен только для группы без участников (сейчас: Stable)")).toBeInTheDocument();
  });

  it("предложение LLM открывает панель сброса с его стратегией и строкой о предложении", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" proposal={resetProposal} />);
    expect(await screen.findByText("Применяется предложение LLM: Replay orders")).toBeInTheDocument();
    expect(screen.getByLabelText("Стратегия")).toHaveValue("latest");
    expect(screen.getByRole("button", { name: "Предпросмотр" })).toBeInTheDocument();
  });

  it("панель из предложения LLM закрывается, после неё открывается удаление группы", async () => {
    const user = userEvent.setup();
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" proposal={resetProposal} />);
    expect(await screen.findByText("Применяется предложение LLM: Replay orders")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Закрыть" }));
    expect(screen.queryByRole("button", { name: "Предпросмотр" })).toBeNull();
    expect(screen.queryByText("Применяется предложение LLM: Replay orders")).toBeNull();
    const del = screen.getByRole("button", { name: "Удалить группу" });
    await waitFor(() => expect(del).toBeEnabled());
    await user.click(del);
    expect(screen.getByText("Удалить группу «g»?")).toBeInTheDocument();
    expect(screen.getByLabelText("Подтверждение: id группы")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Предпросмотр" })).toBeNull();
  });

  it("read-only: панель сброса не открывается даже с prefill", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([{ ...dev, readOnly: true }]);
    vi.mocked(api.describeGroups).mockResolvedValue([stable]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" resetPrefill={{ topic: "orders", partition: 0, offset: 1, nonce: 1 }} />);
    await screen.findByText("нет коммита");
    expect(screen.queryByRole("button", { name: "Предпросмотр" })).toBeNull();
  });
});
