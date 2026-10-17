import { act, fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile, ConsumeEvent } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

let emit: (e: ConsumeEvent) => void = () => {};
const conns: ConnectionProfile[] = [
  { ...newProfile(), id: "dev", name: "dev-1", environment: "dev" },
  { ...newProfile(), id: "ro", name: "ro-1", environment: "dev", readOnly: true },
  { ...newProfile(), id: "prod", name: "prod-1", environment: "prod" },
];

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      suggestGroupId: vi.fn().mockResolvedValue("glafira-kafka-qa-abc123"),
      startConsumer: vi.fn(async (_p: unknown, cb: (e: ConsumeEvent) => void) => {
        emit = cb;
        return "sid";
      }),
      stopSession: vi.fn().mockResolvedValue(true),
      commitOffsets: vi.fn().mockResolvedValue([{ topic: "orders", partition: 0, offset: 5 }]),
      pausePartitions: vi.fn().mockResolvedValue(undefined),
      resumePartitions: vi.fn().mockResolvedValue(undefined),
    },
  };
});

import { api } from "../../api/client";
import { ConsumerPanel } from "./ConsumerPanel";

const orders0 = { topic: "orders", partition: 0 };
const orders1 = { topic: "orders", partition: 1 };

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listConnections).mockResolvedValue(conns);
});

async function startDev() {
  const user = userEvent.setup();
  renderWithClient(<ConsumerPanel connectionId="dev" topic="orders" />);
  await screen.findByDisplayValue("glafira-kafka-qa-abc123");
  await user.click(screen.getByRole("button", { name: "Подключиться к группе" }));
  return user;
}

describe("ConsumerPanel", () => {
  it("запуск, назначение, пауза и коммит всего прочитанного", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConsumerPanel connectionId="dev" topic="orders" />);
    expect(await screen.findByDisplayValue("glafira-kafka-qa-abc123")).toBeInTheDocument();
    expect(screen.getByText(/влияет на кластер/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Подключиться к группе" }));
    expect(vi.mocked(api.startConsumer).mock.calls[0][0]).toMatchObject({ groupId: "glafira-kafka-qa-abc123", topics: ["orders"] });
    act(() => emit({ type: "rebalance", record: { kind: "assigned", partitions: [{ topic: "orders", partition: 0 }], at: 0 } }));
    await user.click(screen.getByRole("button", { name: "Пауза orders:0" }));
    expect(api.pausePartitions).toHaveBeenCalledWith("sid", [{ topic: "orders", partition: 0 }]);
    await user.click(screen.getByRole("button", { name: "Закоммитить всё прочитанное" }));
    expect(api.commitOffsets).toHaveBeenCalledWith("sid", { type: "allConsumed" }, null);
    expect(await screen.findByText(/Закоммичено: orders:0 → 5/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отключиться" }));
    expect(api.stopSession).toHaveBeenCalledWith("sid");
  });

  it("read-only: сессии недоступны", async () => {
    renderWithClient(<ConsumerPanel connectionId="ro" />);
    expect(await screen.findByText(/недоступны для подключения только для чтения/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Подключиться к группе" })).not.toBeInTheDocument();
  });

  it("подключение стало read-only во время сессии: сессия останавливается", async () => {
    const user = userEvent.setup();
    const { client } = renderWithClient(<ConsumerPanel connectionId="dev" topic="orders" />);
    await screen.findByDisplayValue("glafira-kafka-qa-abc123");
    // Сбой stopSession не оставляет UI в «running»: сессию остановит и бэкенд (cancel_for при сохранении).
    vi.mocked(api.stopSession).mockRejectedValueOnce(new Error("сбой"));
    await user.click(screen.getByRole("button", { name: "Подключиться к группе" }));
    act(() => client.setQueryData(["connections"], conns.map((c) => (c.id === "dev" ? { ...c, readOnly: true } : c))));
    expect(await screen.findByText(/недоступны для подключения только для чтения/)).toBeInTheDocument();
    expect(api.stopSession).toHaveBeenCalledWith("sid");
    expect(screen.queryByRole("button", { name: "Подключиться к группе" })).not.toBeInTheDocument();
  });

  it("read-only во время запуска: поздняя сессия останавливается", async () => {
    const user = userEvent.setup();
    let resolveStart: (id: string) => void = () => {};
    vi.mocked(api.startConsumer).mockImplementationOnce(
      (_p, cb) =>
        new Promise((r) => {
          emit = cb;
          resolveStart = r;
        }),
    );
    const { client } = renderWithClient(<ConsumerPanel connectionId="dev" topic="orders" />);
    await screen.findByDisplayValue("glafira-kafka-qa-abc123");
    await user.click(screen.getByRole("button", { name: "Подключиться к группе" }));
    act(() => client.setQueryData(["connections"], conns.map((c) => (c.id === "dev" ? { ...c, readOnly: true } : c))));
    await screen.findByText(/недоступны для подключения только для чтения/);
    // Поздний ответ запуска после автоостановки — сессия тоже останавливается.
    await act(async () => resolveStart("late"));
    expect(api.stopSession).toHaveBeenCalledWith("late");
  });

  it("у всех полей формы есть видимые подписи", async () => {
    renderWithClient(<ConsumerPanel connectionId="dev" topic="orders" />);
    await screen.findByDisplayValue("glafira-kafka-qa-abc123");
    for (const label of ["group.id", "Топики", "auto.offset.reset", "isolation.level", "Формат ключа", "Формат значения", "Буфер, сообщений"]) {
      expect(screen.getByText(label)).toBeVisible();
      expect(screen.getByLabelText(label)).toBeInTheDocument();
    }
  });

  it("пауза доступна только для назначенных партиций; после ребаланса состояние пересчитывается", async () => {
    const user = await startDev();
    act(() => emit({ type: "rebalance", record: { kind: "assigned", partitions: [orders0, orders1], at: 0 } }));
    act(() => emit({ type: "paused", partitions: [orders1] }));
    await user.click(screen.getByRole("button", { name: "Продолжить orders:1" }));
    expect(api.resumePartitions).toHaveBeenCalledWith("sid", [orders1]);
    act(() => emit({ type: "rebalance", record: { kind: "revoked", partitions: [orders1], at: 1 } }));
    expect(screen.queryByRole("button", { name: /orders:1/ })).not.toBeInTheDocument();
    act(() => emit({ type: "rebalance", record: { kind: "assigned", partitions: [orders1], at: 2 } }));
    expect(screen.getByRole("button", { name: "Пауза orders:1" })).toBeInTheDocument();
  });

  it("«Отключиться» сбрасывает UI без терминального события", async () => {
    const user = await startDev();
    act(() => emit({ type: "rebalance", record: { kind: "assigned", partitions: [orders0], at: 0 } }));
    await user.click(screen.getByRole("button", { name: "Отключиться" }));
    expect(screen.getByRole("button", { name: "Подключиться к группе" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Отключиться" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "Пауза orders:0" })).not.toBeInTheDocument();
    // События остановленной сессии игнорируются.
    act(() => emit({ type: "rebalance", record: { kind: "assigned", partitions: [orders1], at: 1 } }));
    expect(screen.queryByRole("button", { name: "Пауза orders:1" })).not.toBeInTheDocument();
  });

  it("«Отключиться» до ответа startConsumer останавливает позднюю сессию", async () => {
    let resolve: (id: string) => void = () => {};
    vi.mocked(api.startConsumer).mockImplementationOnce(() => new Promise<string>((r) => (resolve = r)));
    const user = await startDev();
    await user.click(screen.getByRole("button", { name: "Отключиться" }));
    expect(screen.getByRole("button", { name: "Подключиться к группе" })).toBeEnabled();
    await act(async () => resolve("late"));
    expect(api.stopSession).toHaveBeenCalledWith("late");
  });

  it("ошибка сессии (паника) показывается и разблокирует запуск", async () => {
    await startDev();
    act(() =>
      emit({ type: "error", error: { code: "INTERNAL", message: "Внутренняя ошибка consumer-сессии", retryable: false } }),
    );
    expect(screen.getByRole("alert")).toHaveTextContent("Внутренняя ошибка consumer-сессии");
    expect(screen.getByRole("button", { name: "Подключиться к группе" })).toBeEnabled();
  });

  it("несуществующий топик: понятная ошибка", async () => {
    await startDev();
    act(() =>
      emit({
        type: "error",
        error: { code: "NOT_FOUND", message: "Subscribed topic not available: orders: Broker: Unknown topic or partition", retryable: false },
      }),
    );
    expect(screen.getByText(/Топик не найден в кластере\. Проверьте имена: orders/)).toBeInTheDocument();
  });

  it("таймаут коммита: результат не утверждается", async () => {
    const user = await startDev();
    vi.mocked(api.commitOffsets).mockRejectedValueOnce({ code: "TIMEOUT", message: "Consumer-сессия не ответила вовремя", retryable: true });
    await user.click(screen.getByRole("button", { name: "Закоммитить всё прочитанное" }));
    expect(await screen.findByText(/Consumer-сессия не ответила вовремя/)).toBeInTheDocument();
    expect(screen.getByText(/Неизвестно, применён ли коммит/)).toBeInTheDocument();
    expect(screen.queryByText(/Закоммичено/)).not.toBeInTheDocument();
    // Сессия всё же закоммитила — приходит событие committed.
    act(() => emit({ type: "committed", offsets: [{ topic: "orders", partition: 0, offset: 7 }] }));
    expect(screen.getByText(/Закоммичено: orders:0 → 7/)).toBeInTheDocument();
  });

  it("prod: проверка формы до подтверждения, подтверждение запуска и коммита", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConsumerPanel connectionId="prod" />);
    await screen.findByDisplayValue("glafira-kafka-qa-abc123");
    await user.click(screen.getByRole("button", { name: "Подключиться к группе" }));
    expect(screen.getByText("Укажите топики")).toBeInTheDocument();
    expect(screen.queryByLabelText("Имя подключения")).not.toBeInTheDocument();

    await user.type(screen.getByLabelText("Топики"), "orders");
    await user.click(screen.getByRole("button", { name: "Подключиться к группе" }));
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Подтвердить" }));
    expect(vi.mocked(api.startConsumer).mock.calls[0][2]).toBe("prod-1");

    await user.click(screen.getByRole("button", { name: "Закоммитить всё прочитанное" }));
    expect(api.commitOffsets).not.toHaveBeenCalled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Подтвердить" }));
    expect(api.commitOffsets).toHaveBeenCalledWith("sid", { type: "allConsumed" }, "prod-1");
  });

  it("подсказки: значки в форме и в запущенной сессии, текст автокоммита", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConsumerPanel connectionId="dev" topic="orders" />);
    await screen.findByDisplayValue("glafira-kafka-qa-abc123");
    screen.getByRole("button", { name: "Подсказка: Вступление в группу" });
    await user.click(screen.getByRole("button", { name: "Подсказка: автокоммит" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Автоматически коммитит прочитанные оффсеты в фоне. Без него — вручную, кнопками коммита.",
    );
    await user.keyboard("{Escape}");
    await user.click(screen.getByRole("button", { name: "Подключиться к группе" }));
    act(() => emit({ type: "rebalance", record: { kind: "assigned", partitions: [orders0], at: 0 } }));
    screen.getByRole("button", { name: "Подсказка: Закоммитить всё прочитанное" });
    screen.getByRole("button", { name: "Подсказка: пауза" });
    // Форма в <fieldset disabled> на время сессии, но значок автокоммита открывается (span, а не button).
    const autoCommit = screen.getByRole("button", { name: "Подсказка: автокоммит" });
    act(() => autoCommit.focus());
    expect(autoCommit).toHaveFocus();
    fireEvent.keyDown(autoCommit, { key: "Enter" });
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Автоматически коммитит прочитанные оффсеты в фоне.");
  });
});
