import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 30 })),
    getTotalSize: () => o.count * 30,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: { listGroups: vi.fn(), describeGroups: vi.fn(), getGroupsLag: vi.fn() },
  };
});

import { api } from "../../api/client";
import { GroupList } from "./GroupList";

beforeEach(() => {
  vi.mocked(api.listGroups).mockResolvedValue({
    groups: [{ groupId: "orders", state: "Stable", isSimple: false }],
    errors: ["broker 2: timeout"],
  });
  vi.mocked(api.describeGroups).mockResolvedValue([
    { groupId: "orders", state: "Stable", assignor: "range", members: [] },
  ]);
  vi.mocked(api.getGroupsLag).mockRejectedValue(new Error("lag boom"));
});

describe("GroupList", () => {
  it("показывает неполноту списка, а ошибку лага как «—», не как 0", async () => {
    renderWithClient(<GroupList connectionId="c" />);
    expect(await screen.findByText("orders")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("неполным");
    expect(await screen.findByText("range")).toBeInTheDocument();
    const lagCell = await screen.findByTitle(/lag boom/);
    expect(lagCell).toHaveTextContent("—");
  });

  it("id, отсутствующий в ответе брокера, показывается как «—», а не «…»", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([]);
    vi.mocked(api.getGroupsLag).mockResolvedValue([]);
    renderWithClient(<GroupList connectionId="c" />);
    expect(await screen.findByText("orders")).toBeInTheDocument();
    await vi.waitFor(() => expect(screen.getAllByTitle("нет данных в ответе брокера").length).toBeGreaterThanOrEqual(3));
    screen.getAllByTitle("нет данных в ответе брокера").forEach((c) => expect(c).toHaveTextContent("—"));
    expect(screen.queryByText("…")).toBeNull();
  });

  it("группа без коммитов: лаг «—» с подсказкой «нет коммитов»", async () => {
    vi.mocked(api.getGroupsLag).mockResolvedValue([{ groupId: "orders", totalLag: null, topics: [] }]);
    renderWithClient(<GroupList connectionId="c" />);
    expect(await screen.findByTitle("нет коммитов")).toHaveTextContent("—");
  });

  it("«Обновить» перезапрашивает описания и лаг", async () => {
    vi.mocked(api.getGroupsLag).mockResolvedValue([]);
    renderWithClient(<GroupList connectionId="c" />);
    await screen.findByText("orders");
    await vi.waitFor(() => expect(vi.mocked(api.getGroupsLag).mock.calls.length).toBeGreaterThan(0));
    const l0 = vi.mocked(api.getGroupsLag).mock.calls.length;
    const d0 = vi.mocked(api.describeGroups).mock.calls.length;
    await userEvent.setup().click(screen.getByRole("button", { name: "Обновить" }));
    await vi.waitFor(() => {
      expect(vi.mocked(api.getGroupsLag).mock.calls.length).toBe(l0 + 1);
      expect(vi.mocked(api.describeGroups).mock.calls.length).toBe(d0 + 1);
    });
  });
});
