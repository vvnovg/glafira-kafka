import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getTopic: vi.fn().mockResolvedValue({
        name: "orders",
        internal: false,
        replicationFactor: 2,
        messageCount: 15,
        partitions: [
          { id: 0, leader: 1, replicas: [1, 2], isr: [1, 2], low: 0, high: 10, messageCount: 10, underReplicated: false },
          { id: 1, leader: 2, replicas: [1, 2], isr: [2], low: 5, high: 10, messageCount: 5, underReplicated: true },
        ],
      }),
      getTopicConfigs: vi.fn().mockResolvedValue([
        { name: "cleanup.policy", value: "compact", source: "DynamicTopic", isDefault: false, isReadOnly: false, isSensitive: false },
      ]),
    },
  };
});

import { TopicDetails } from "./TopicDetails";

describe("TopicDetails", () => {
  it("показывает партиции и конфиги", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicDetails connectionId="c" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Партиции" }));
    expect(screen.getAllByText("1, 2")).toHaveLength(3); // реплики обеих партиций + ISR партиции 0
    expect(screen.getByText("2", { selector: "td.warn" })).toBeInTheDocument(); // ISR под-реплицированной партиции
    await user.click(screen.getByRole("button", { name: "Конфигурация" }));
    expect(await screen.findByText("compact")).toBeInTheDocument();
  });
});

describe("TopicDetails: недоступные оффсеты", () => {
  it("показывает «—» и ошибку в title", async () => {
    const user = userEvent.setup();
    const { api } = await import("../../api/client");
    vi.mocked(api.getTopic).mockResolvedValueOnce({
      name: "broken",
      internal: false,
      replicationFactor: 1,
      messageCount: null,
      partitions: [
        { id: 0, leader: 1, replicas: [1], isr: [1], low: null, high: null, messageCount: null, underReplicated: false, offsetsError: "timeout fetching offsets" },
      ],
    });
    renderWithClient(<TopicDetails connectionId="c2" topic="broken" />);
    await user.click(await screen.findByRole("button", { name: "Партиции" }));
    const cell = document.querySelector('td[title="timeout fetching offsets"]');
    expect(cell).not.toBeNull();
    expect(cell).toHaveTextContent("—");
  });
});

describe("TopicDetails: вкладка сообщений", () => {
  it("сохраняет состояние формы при переключении вкладок", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicDetails connectionId="c3" topic="orders" />);
    const input = await screen.findByLabelText("Значение содержит");
    await user.type(input, "FAILED");
    await user.click(screen.getByRole("button", { name: "Партиции" }));
    await user.click(screen.getByRole("button", { name: "Сообщения" }));
    expect(screen.getByLabelText("Значение содержит")).toHaveValue("FAILED");
  });
});
