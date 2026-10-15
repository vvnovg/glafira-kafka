import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile, TopicDetails } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      addPartitions: vi.fn().mockResolvedValue(undefined),
      purgeTopic: vi.fn().mockResolvedValue([{ partition: 0, lowWatermark: 10 }, { partition: 1, lowWatermark: 4 }]),
      deleteTopics: vi.fn().mockResolvedValue([{ name: "orders" }]),
      getTopicPolicies: vi.fn().mockResolvedValue([{ name: "orders", cleanupPolicy: "compact,delete" }]),
    },
  };
});

import { api } from "../../api/client";
import { TopicActions } from "./TopicActions";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const topic: TopicDetails = {
  name: "orders",
  internal: false,
  replicationFactor: 1,
  messageCount: 14,
  partitions: [
    { id: 0, leader: 1, replicas: [1], isr: [1], low: 0, high: 10, messageCount: 10, underReplicated: false },
    { id: 1, leader: 1, replicas: [1], isr: [1], low: 0, high: 4, messageCount: 4, underReplicated: false },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
});

describe("TopicActions", () => {
  it("добавление партиций — только больше текущего", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Добавить партиции" }));
    const input = screen.getByLabelText("Новое число партиций");
    await user.clear(input);
    await user.type(input, "2");
    expect(screen.getByRole("button", { name: "Увеличить" })).toBeDisabled();
    await user.clear(input);
    await user.type(input, "4");
    await user.click(screen.getByRole("button", { name: "Увеличить" }));
    expect(api.addPartitions).toHaveBeenCalledWith("c", "orders", 4, null);
  });

  it("purge предупреждает о compact и требует имя топика", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Очистить" }));
    expect(await screen.findByText(/compact/)).toBeInTheDocument();
    await user.click(screen.getByLabelText("Партиция 1"));
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
    await user.click(screen.getByRole("button", { name: "Очистить партиции" }));
    expect(api.purgeTopic).toHaveBeenCalledWith("c", "orders", [0], { confirm: null, typedName: "orders" });
    expect(await screen.findByText("Партиция 0: новый low watermark 10")).toBeInTheDocument();
  });

  it("purge недоступен для топика с cleanup.policy=compact", async () => {
    vi.mocked(api.getTopicPolicies).mockResolvedValueOnce([{ name: "orders", cleanupPolicy: "compact" }]);
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} />);
    const msg = "Очистка невозможна: у топика cleanup.policy=compact (нужно compact,delete или delete)";
    await vi.waitFor(() => expect(screen.getByRole("button", { name: "Очистить" })).toBeDisabled());
    expect(screen.getByRole("button", { name: "Очистить" })).toHaveAttribute("title", msg);
    expect(screen.getByText(msg)).toBeInTheDocument();
  });

  it("purge при неизвестной политике доступен, но предупреждает", async () => {
    const user = userEvent.setup();
    vi.mocked(api.getTopicPolicies).mockRejectedValueOnce({ code: "INTERNAL", message: "boom" });
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Очистить" }));
    expect(await screen.findByText(/Не удалось определить cleanup.policy/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Очистить" })).toBeEnabled();
  });

  it("purge всех партиций отправляет явный список показанных партиций", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Очистить" }));
    expect(screen.getByText("Очистить все партиции топика «orders»?")).toBeInTheDocument();
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
    await user.click(screen.getByRole("button", { name: "Очистить партиции" }));
    expect(api.purgeTopic).toHaveBeenCalledWith("c", "orders", [0, 1], { confirm: null, typedName: "orders" });
  });

  it("смена выбора партиций сбрасывает введённое имя и обновляет заголовок", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Очистить" }));
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
    expect(screen.getByRole("button", { name: "Очистить партиции" })).toBeEnabled();
    await user.click(screen.getByLabelText("Партиция 1"));
    expect(screen.getByLabelText("Подтверждение: имя топика")).toHaveValue("");
    expect(screen.getByRole("button", { name: "Очистить партиции" })).toBeDisabled();
    expect(screen.getByText("Очистить партиции [0] топика «orders»?")).toBeInTheDocument();
  });

  it("удаление сообщает наверх", async () => {
    const user = userEvent.setup();
    const onDeleted = vi.fn();
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={onDeleted} />);
    await user.click(screen.getByRole("button", { name: "Удалить топик" }));
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    await vi.waitFor(() => expect(onDeleted).toHaveBeenCalled());
  });
});
