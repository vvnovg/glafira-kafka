import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      createTopic: vi.fn().mockResolvedValue(undefined),
      getClusterOverview: vi.fn().mockResolvedValue({ brokers: [{ id: 1, host: "h", port: 1 }] }),
    },
  };
});

import { api } from "../../api/client";
import { CreateTopicForm } from "./CreateTopicForm";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const prod: ConnectionProfile = { ...dev, environment: "prod", name: "prod-1" };

beforeEach(() => vi.clearAllMocks());

describe("CreateTopicForm", () => {
  it("валидирует имя, показывает команду и создаёт топик", async () => {
    const user = userEvent.setup();
    const onCreated = vi.fn();
    renderWithClient(<CreateTopicForm connection={dev} onCreated={onCreated} onCancel={() => {}} />);
    await user.type(screen.getByLabelText("Имя топика"), "bad name");
    expect(screen.getByText(/Недопустимый символ/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Создать" })).toBeDisabled();
    await user.clear(screen.getByLabelText("Имя топика"));
    await user.type(screen.getByLabelText("Имя топика"), "orders");
    await user.clear(screen.getByLabelText("Партиции"));
    await user.type(screen.getByLabelText("Партиции"), "3");
    await user.selectOptions(screen.getByLabelText("Пресет"), "compact");
    expect(screen.getByText(/--topic orders --partitions 3/)).toHaveTextContent("--config cleanup.policy=compact");
    await user.click(screen.getByRole("button", { name: "Создать" }));
    expect(api.createTopic).toHaveBeenCalledWith(
      "c",
      { name: "orders", partitions: 3, replicationFactor: 1, configs: { "cleanup.policy": "compact" } },
      null,
      null,
    );
    await vi.waitFor(() => expect(onCreated).toHaveBeenCalledWith("orders"));
  });

  it("поля из предложения LLM заполнены, создание передаёт proposalId", async () => {
    const user = userEvent.setup();
    renderWithClient(
      <CreateTopicForm
        connection={dev}
        onCreated={() => {}}
        onCancel={() => {}}
        initial={{ name: "orders-dlq", partitions: 6, replicationFactor: -1, configs: { "cleanup.policy": "compact", "retention.ms": "1000" } }}
        proposalId="p1"
      />,
    );
    expect(screen.getByLabelText("Имя топика")).toHaveValue("orders-dlq");
    expect(screen.getByLabelText("Партиции")).toHaveValue(6);
    expect(screen.getByLabelText("Фактор репликации")).toHaveValue(-1);
    expect(screen.getByLabelText("Пресет")).toHaveValue("delete");
    expect(screen.getByLabelText("Доп. конфиги (key=value)")).toHaveValue("cleanup.policy=compact\nretention.ms=1000");
    await user.click(screen.getByRole("button", { name: "Создать" }));
    expect(api.createTopic).toHaveBeenCalledWith(
      "c",
      { name: "orders-dlq", partitions: 6, replicationFactor: -1, configs: { "cleanup.policy": "compact", "retention.ms": "1000" } },
      null,
      "p1",
    );
  });

  it("в prod просит имя подключения перед созданием", async () => {
    const user = userEvent.setup();
    renderWithClient(<CreateTopicForm connection={prod} onCreated={() => {}} onCancel={() => {}} />);
    await user.type(screen.getByLabelText("Имя топика"), "t");
    await user.click(screen.getByRole("button", { name: "Создать" }));
    expect(api.createTopic).not.toHaveBeenCalled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Создать топик" }));
    expect(api.createTopic).toHaveBeenCalledWith("c", expect.objectContaining({ name: "t" }), "prod-1", null);
  });

  it("подсказки «?» у полей формы; открытая подсказка показывает текст", async () => {
    const user = userEvent.setup();
    renderWithClient(<CreateTopicForm connection={dev} onCreated={() => {}} onCancel={() => {}} />);
    for (const label of ["Партиции", "Фактор репликации", "Пресет", "Доп. конфиги (key=value)"]) {
      expect(screen.getByRole("button", { name: `Подсказка: ${label}` })).toBeInTheDocument();
    }
    // Значок рядом с подписью, а не внутри: доступное имя поля не меняется.
    expect(screen.getByLabelText("Фактор репликации")).toHaveAttribute("id", "ct-rf");
    await user.click(screen.getByRole("button", { name: "Подсказка: Фактор репликации" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Сколько копий каждой партиции хранят брокеры. Не больше числа брокеров; для prod обычно 3.",
    );
  });
});
