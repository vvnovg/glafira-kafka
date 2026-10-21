import { fireEvent, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { useHelpContext } from "../../state/helpContext";
import { renderWithClient } from "../../test/render";
import { TopicDetails } from "./TopicDetails";

vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    api: {
      ...actual.api,
      getTopic: vi.fn().mockResolvedValue({ name: "orders", internal: false, replicationFactor: 1, messageCount: 0, partitions: [] }),
      getTopicConfigs: vi.fn(() => new Promise(() => {})),
      listConnections: vi.fn().mockResolvedValue([]),
    },
  };
});
vi.mock("../browse/BrowsePanel", () => ({ BrowsePanel: () => <div /> }));
vi.mock("./TopicGroups", () => ({ TopicGroups: () => <div /> }));

describe("TopicDetails help context", () => {
  beforeEach(() => useHelpContext.setState({ byTab: {} }));

  it("points F1 to browse on the messages sub-tab and to topics elsewhere", async () => {
    const { unmount } = renderWithClient(<TopicDetails connectionId="c1" topic="orders" />);
    expect(useHelpContext.getState().byTab["topic:c1:orders"]).toBe("browse");
    fireEvent.click(await screen.findByRole("button", { name: "Конфигурация" }));
    expect(useHelpContext.getState().byTab["topic:c1:orders"]).toBe("topics");
    unmount();
    expect(useHelpContext.getState().byTab["topic:c1:orders"]).toBeUndefined();
  });
});
