import { screen } from "@testing-library/react";
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
});
