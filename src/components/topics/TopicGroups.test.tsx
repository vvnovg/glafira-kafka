import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getTopicGroups: vi.fn().mockResolvedValue({
        groups: [
          { groupId: "svc", state: "Stable", lag: 12, partitions: 3 },
          { groupId: "old", state: "Empty", lag: null, partitions: 1 },
        ],
        incomplete: true,
      }),
    },
  };
});

import { TopicGroups } from "./TopicGroups";

describe("TopicGroups", () => {
  it("показывает группы топика и открывает группу", async () => {
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    renderWithClient(<TopicGroups connectionId="c" topic="orders" />);
    expect(await screen.findByText("svc")).toBeInTheDocument();
    expect(screen.getByText("—")).toBeInTheDocument();
    expect(screen.getByText(/проверены не все группы/)).toBeInTheDocument();
    await user.click(screen.getByText("svc"));
    expect(useWorkspace.getState().activeId).toBe("group:c:svc");
  });
});
