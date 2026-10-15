import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

const conns: ConnectionProfile[] = [
  { ...newProfile(), id: "dev", name: "dev-1", environment: "dev" },
  { ...newProfile(), id: "prod", name: "prod-1", environment: "prod" },
  { ...newProfile(), id: "ro", name: "ro-1", environment: "dev", readOnly: true },
];

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      listTopics: vi.fn().mockResolvedValue([{ name: "orders", internal: false, partitionCount: 1, replicationFactor: 1, underReplicated: 0 }]),
      produce: vi.fn().mockResolvedValue({ partition: 0, offset: 41, timestamp: 0 }),
      listTemplates: vi.fn().mockResolvedValue([]),
      listHistory: vi.fn().mockResolvedValue([]),
    },
  };
});

import { api } from "../../api/client";
import { ProducerPanel } from "./ProducerPanel";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listConnections).mockResolvedValue(conns);
});

describe("ProducerPanel", () => {
  it("«Очистить форму» сбрасывает поля, но сохраняет топик", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProducerPanel connectionId="dev" />);
    await user.type(await screen.findByLabelText("Топик"), "orders");
    await user.type(screen.getByLabelText("Значение"), "v");
    await user.click(screen.getByRole("button", { name: "Добавить заголовок" }));
    await user.click(screen.getByRole("button", { name: "Очистить форму" }));
    expect(screen.getByLabelText("Топик")).toHaveValue("orders");
    expect(screen.getByLabelText("Значение")).toHaveValue("");
    expect(screen.queryByLabelText("Имя заголовка 1")).not.toBeInTheDocument();
  });

  it("отправляет сообщение и показывает результат", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProducerPanel connectionId="dev" />);
    await user.type(await screen.findByLabelText("Топик"), "orders");
    await user.type(screen.getByLabelText("Значение"), '{{"a":1}');
    await user.click(screen.getByRole("button", { name: "Добавить заголовок" }));
    await user.type(screen.getByLabelText("Имя заголовка 1"), "trace");
    await user.type(screen.getByLabelText("Значение заголовка 1"), "t1");
    await user.click(screen.getByRole("button", { name: "Отправить" }));
    expect(api.produce).toHaveBeenCalledWith(
      "dev",
      {
        topic: "orders",
        partition: null,
        key: { format: "string", text: "" },
        value: { format: "json", text: '{"a":1}' },
        headers: [{ name: "trace", value: "t1", format: "string" }],
      },
      { acks: "all", compression: "none", idempotence: false },
      null,
    );
    expect(await screen.findByText(/партиция 0, offset 41/)).toBeInTheDocument();
  });

  it("prod требует имя подключения", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProducerPanel connectionId="prod" />);
    await user.type(await screen.findByLabelText("Топик"), "orders");
    await user.click(screen.getByRole("button", { name: "Отправить" }));
    expect(api.produce).not.toHaveBeenCalled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Отправить в prod" }));
    expect(vi.mocked(api.produce).mock.calls[0][3]).toBe("prod-1");
  });

  it("read-only: отправка недоступна", async () => {
    renderWithClient(<ProducerPanel connectionId="ro" />);
    expect(await screen.findByText(/только для чтения/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Отправить" })).not.toBeInTheDocument();
  });

  it("offset=null показывается как «—» с пояснением", async () => {
    vi.mocked(api.produce).mockResolvedValueOnce({ partition: 0, offset: null, timestamp: 0 });
    const user = userEvent.setup();
    renderWithClient(<ProducerPanel connectionId="dev" />);
    await user.type(await screen.findByLabelText("Топик"), "orders");
    await user.click(screen.getByRole("button", { name: "Отправить" }));
    expect(await screen.findByTitle(/не вернул offset/)).toHaveTextContent("—");
  });

  it("prod: ошибка валидации вместо диалога", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProducerPanel connectionId="prod" />);
    await screen.findByLabelText("Топик");
    await user.click(screen.getByRole("button", { name: "Отправить" }));
    expect(screen.getByText("Укажите топик")).toBeInTheDocument();
    expect(screen.queryByLabelText("Имя подключения")).not.toBeInTheDocument();
  });

  it("правка формы убирает старый результат; у формата есть видимая подпись", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProducerPanel connectionId="dev" />);
    expect(await screen.findByLabelText("Формат значения")).toBeVisible();
    await user.type(screen.getByLabelText("Топик"), "orders");
    await user.click(screen.getByRole("button", { name: "Отправить" }));
    await screen.findByText(/партиция 0, offset 41/);
    await user.type(screen.getByLabelText("Значение"), "x");
    expect(screen.queryByText(/партиция 0, offset 41/)).not.toBeInTheDocument();
  });
});
