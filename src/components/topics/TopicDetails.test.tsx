import { QueryClientProvider } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";
import { useWorkspace } from "../../state/workspace";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn().mockResolvedValue([]),
      deleteTopics: vi.fn().mockResolvedValue([{ name: "orders" }]),
      getTopicPolicies: vi.fn().mockResolvedValue([]),
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
      alterTopicConfigs: vi.fn().mockResolvedValue(undefined),
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

describe("TopicDetails: предложение LLM", () => {
  const proposal = {
    proposal: {
      id: "p1",
      severity: "warning" as const,
      title: "Compact orders",
      rationale: "r",
      target: { kind: "topic" as const, name: "orders" },
      change: { kind: "alterTopicConfigs" as const, topic: "orders", changes: [{ name: "cleanup.policy", value: "delete" }] },
      client: "claude-code",
      createdAtMs: 1,
      updatedAtMs: 1,
      status: "open" as const,
    },
    nonce: 1,
  };
  const banner = "Применяется предложение LLM: Compact orders";

  it("открывает «Конфигурацию» с заполненным редактором и строкой о предложении", async () => {
    const { api } = await import("../../api/client");
    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c", name: "dev-1", environment: "dev" }]);
    renderWithClient(<TopicDetails connectionId="c" topic="orders" proposal={proposal} />);
    expect(await screen.findByText(banner)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Конфигурация" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByLabelText("Значение cleanup.policy")).toHaveValue("delete");
    expect(screen.getByText("изменений: 1")).toBeInTheDocument();
  });

  it("после применения «Редактировать» открывает редактор без правок и без строки о предложении", async () => {
    const user = userEvent.setup();
    const { api } = await import("../../api/client");
    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c", name: "dev-1", environment: "dev" }]);
    renderWithClient(<TopicDetails connectionId="c" topic="orders" proposal={proposal} />);
    expect(await screen.findByText(banner)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
    await user.click(screen.getByRole("button", { name: "Применить" }));
    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "orders", [{ op: "set", name: "cleanup.policy", value: "delete" }], null, "p1");
    await user.click(await screen.findByRole("button", { name: "Редактировать" }));
    expect(screen.queryByText(banner)).toBeNull();
    expect(screen.getByLabelText("Значение cleanup.policy")).toHaveValue("compact");
    expect(screen.getByText("изменений: 0")).toBeInTheDocument();
  });

  it("закрытый редактор не возвращается с предложением: ни после смены подвкладки, ни по «Редактировать»", async () => {
    const user = userEvent.setup();
    const { api } = await import("../../api/client");
    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c", name: "dev-1", environment: "dev" }]);
    renderWithClient(<TopicDetails connectionId="c" topic="orders" proposal={proposal} />);
    expect(await screen.findByText(banner)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отменить правки" }));
    await user.click(screen.getByRole("button", { name: "Закрыть редактор" }));
    expect(screen.queryByText(banner)).toBeNull();
    await user.click(screen.getByRole("button", { name: "Партиции" }));
    await user.click(screen.getByRole("button", { name: "Конфигурация" }));
    expect(await screen.findByRole("button", { name: "Редактировать" })).toBeInTheDocument();
    expect(screen.queryByText(banner)).toBeNull();
    await user.click(screen.getByRole("button", { name: "Редактировать" }));
    expect(screen.queryByText(banner)).toBeNull();
    expect(screen.getByLabelText("Значение cleanup.policy")).toHaveValue("compact");
    expect(screen.getByText("изменений: 0")).toBeInTheDocument();
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
        { id: 0, leader: 1, replicas: [1], isr: [1], low: null, high: null, messageCount: null, underReplicated: false, offsetsError: { key: "errors.topics.noOffset", params: {}, text: "The broker did not return the partition offset" } },
      ],
    });
    renderWithClient(<TopicDetails connectionId="c2" topic="broken" />);
    await user.click(await screen.findByRole("button", { name: "Партиции" }));
    const cell = document.querySelector('td[title="Брокер не вернул оффсет партиции"]');
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

describe("TopicDetails: заполнение из журнала MCP", () => {
  it("открывает подвкладку «Сообщения», повторное «Открыть» заполняет форму заново", async () => {
    const user = userEvent.setup();
    const prefill = (nonce: number, valuePattern: string) => ({ form: { valuePattern }, inexact: [], nonce });
    const { rerender, client } = renderWithClient(<TopicDetails connectionId="c4" topic="orders" browsePrefill={prefill(1, "A")} />);
    expect(await screen.findByLabelText("Значение содержит")).toHaveValue("A");
    await user.click(screen.getByRole("button", { name: "Партиции" }));
    expect(screen.getByLabelText("Значение содержит")).not.toBeVisible();
    rerender(
      <QueryClientProvider client={client}>
        <TopicDetails connectionId="c4" topic="orders" browsePrefill={prefill(2, "B")} />
      </QueryClientProvider>,
    );
    expect(screen.getByRole("button", { name: "Сообщения" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByLabelText("Значение содержит")).toBeVisible();
    expect(screen.getByLabelText("Значение содержит")).toHaveValue("B");
  });
});

describe("TopicDetails: мутации", () => {
  const conn = (extra: object) => ({ ...newProfile(), id: "m", name: "dev-1", environment: "dev", ...extra });

  it("read-only подключение скрывает действия и редактирование", async () => {
    const user = userEvent.setup();
    const { api } = await import("../../api/client");
    vi.mocked(api.listConnections).mockResolvedValue([conn({ readOnly: true })] as never);
    renderWithClient(<TopicDetails connectionId="m" topic="orders" />);
    await screen.findByRole("button", { name: "Партиции" });
    await user.click(screen.getByRole("button", { name: "Конфигурация" }));
    await screen.findByText("compact");
    expect(screen.queryByRole("button", { name: "Удалить топик" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Редактировать" })).toBeNull();
    for (const name of ["Отправить сообщение", "Читать как группа"]) {
      const b = screen.getByRole("button", { name });
      await waitFor(() => expect(b).toBeDisabled());
      expect(b).toHaveAttribute("title", expect.stringContaining("только для чтения"));
    }
  });

  it("обычное подключение: отправка и чтение как группа доступны", async () => {
    const { api } = await import("../../api/client");
    vi.mocked(api.listConnections).mockResolvedValue([conn({ readOnly: false })] as never);
    renderWithClient(<TopicDetails connectionId="m" topic="orders" />);
    await screen.findByRole("button", { name: "Удалить топик" });
    expect(screen.getByRole("button", { name: "Отправить сообщение" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Читать как группа" })).toBeEnabled();
  });

  it("internal-топик скрывает действия", async () => {
    const { api } = await import("../../api/client");
    vi.mocked(api.listConnections).mockResolvedValue([conn({ readOnly: false })] as never);
    vi.mocked(api.getTopic).mockResolvedValueOnce({
      name: "__consumer_offsets",
      internal: true,
      replicationFactor: 1,
      messageCount: 0,
      partitions: [],
    });
    renderWithClient(<TopicDetails connectionId="m" topic="__consumer_offsets" />);
    await screen.findByRole("button", { name: "Партиции" });
    expect(screen.queryByRole("button", { name: "Удалить топик" })).toBeNull();
  });

  it("удаление закрывает вкладку топика", async () => {
    const user = userEvent.setup();
    const { api } = await import("../../api/client");
    vi.mocked(api.listConnections).mockResolvedValue([conn({ readOnly: false })] as never);
    useWorkspace.getState().open({ kind: "topic", connectionId: "m", topic: "orders" });
    renderWithClient(<TopicDetails connectionId="m" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Удалить топик" }));
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    await vi.waitFor(() => expect(useWorkspace.getState().tabs).toHaveLength(0));
  });

  it("редактор нельзя закрыть с несохранёнными правками", async () => {
    const user = userEvent.setup();
    const { api } = await import("../../api/client");
    vi.mocked(api.listConnections).mockResolvedValue([conn({ readOnly: false })] as never);
    renderWithClient(<TopicDetails connectionId="m" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Конфигурация" }));
    await user.click(await screen.findByRole("button", { name: "Редактировать" }));
    await user.type(screen.getByLabelText("Значение cleanup.policy"), "x");
    expect(screen.getByRole("button", { name: "Закрыть редактор" })).toBeDisabled();
  });
});
