import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { mcpStatus } from "../../test/mcp";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getMcpStatus: vi.fn(),
      setMcpEnabled: vi.fn(),
      revokeMcpClient: vi.fn(),
      disconnectMcpSessions: vi.fn(),
      requestMcpApproval: vi.fn(),
      getMcpAudit: vi.fn(),
      getMcpClientConfigs: vi.fn(),
      revealMcpAudit: vi.fn(),
      listConnections: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import type { McpAuditEntry } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { newProfile } from "../connections/profile";
import { McpSection } from "./McpSection";

const configs = {
  command: "/Applications/Glafira for Apache Kafka.app/Contents/MacOS/glafira-kafka",
  args: ["mcp"],
  claudeDesktop: '{"mcpServers":{"glafira-kafka":{}}}',
  claudeCode: "claude mcp add glafira-kafka -- /x mcp",
  otherClient: "'/Applications/Glafira for Apache Kafka.app/Contents/MacOS/glafira-kafka' mcp",
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getMcpClientConfigs).mockResolvedValue(configs);
  vi.mocked(api.getMcpAudit).mockResolvedValue([]);
  vi.mocked(api.listConnections).mockResolvedValue([]);
});

describe("McpSection", () => {
  it("включает сервер и показывает конфиги клиентов", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus({ state: "off", enabled: false }));
    vi.mocked(api.setMcpEnabled).mockResolvedValue(mcpStatus());
    const user = userEvent.setup();
    renderWithClient(<McpSection />);
    const toggle = await screen.findByLabelText("Разрешить MCP-сервер");
    expect(toggle).not.toBeChecked();
    expect(screen.getByRole("status")).toHaveTextContent("Сервер выключен");
    await user.click(toggle);
    expect(api.setMcpEnabled).toHaveBeenCalledWith(true);
    await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Сервер работает"));
    expect(screen.getByText("claude mcp add glafira-kafka -- /x mcp")).toBeInTheDocument();
    // Путь с пробелами — в кавычках, как у Claude Code.
    expect(screen.getByText(configs.otherClient)).toBeInTheDocument();
  });

  it("считает открытые подключения и показывает запрет чтения сообщений", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus({ allowedConnections: 3, messageConnections: 1 }));
    const { unmount } = renderWithClient(<McpSection />);
    expect(await screen.findByText("Открыто для LLM: 3, из них с сообщениями: 1")).toBeInTheDocument();
    expect(screen.queryByText("Чтение сообщений отключено вашей организацией")).not.toBeInTheDocument();
    unmount();
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus({ messagesAllowed: false }));
    renderWithClient(<McpSection />);
    expect(await screen.findByText("Чтение сообщений отключено вашей организацией")).toBeInTheDocument();
  });

  const read = (id: string, topic: string, patch: Partial<McpAuditEntry> = {}): McpAuditEntry => ({
    ts: 2,
    event: "call",
    tool: "read_messages",
    outcome: "ok",
    connection: { id, name: id },
    args: { connectionId: id, topic, from: 0, to: 1000 },
    scanned: 10,
    returned: 3,
    masked: 2,
    ms: 5,
    ...patch,
  });
  const openButtons = () => screen.findAllByRole("button", { name: "Открыть в «Сообщениях»" });
  const connections = [
    { ...newProfile(), id: "c1", name: "dev", llmAccess: "messages" as const },
    { ...newProfile(), id: "c2", name: "meta", llmAccess: "metadata" as const },
  ];

  it("журнал: подробности чтения сообщений и «Открыть в „Сообщениях“»", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    vi.mocked(api.listConnections).mockResolvedValue(connections);
    vi.mocked(api.getMcpAudit).mockResolvedValue([
      read("c1", "orders", { scanned: 12345 }),
      read("c2", "payments"),
      read("c1", "big", { args: '{"topic":"big"…' }),
    ]);
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    renderWithClient(<McpSection />);
    const buttons = await openButtons();
    expect(buttons).toHaveLength(3);
    // Числа — в формате языка интерфейса (textContent: нормализация testing-library заменила бы неразрывный пробел).
    expect(buttons[0].closest("td")?.textContent).toContain(`orders: 3 из ${(12345).toLocaleString("ru")}, замаскировано: 2`);
    await waitFor(() => expect(buttons[0]).toBeEnabled());
    expect(buttons[0]).not.toHaveAttribute("title");
    expect(buttons[1]).toBeDisabled();
    expect(buttons[1]).toHaveAttribute("title", "Недоступно: у подключения уровень не «Метаданные и сообщения»");
    // Аргументы усечены: кнопка есть, но неактивна; цели нет — и двоеточия перед числами тоже.
    expect(buttons[2]).toBeDisabled();
    expect(buttons[2]).toHaveAttribute("title", "Недоступно: аргументы вызова в журнале усечены");
    expect(buttons[2].closest("td")).toHaveTextContent(/^3 из 10, замаскировано: 2 Открыть в «Сообщениях»$/);
    await user.click(buttons[0]);
    const tab = useWorkspace.getState().tabs[0];
    expect(tab).toMatchObject({ kind: "topic", connectionId: "c1", topic: "orders" });
    expect(tab.kind === "topic" && tab.browsePrefill?.form.startType).toBe("timestamp");
  });

  // У каждого инструмента свой шаблон: у обзора отдаются корзины, у истории — серии коммитов, а не сообщения.
  it.each([
    ["read_messages", { topic: "orders" }, "orders: 3 из 10, замаскировано: 2", "3 из 10, замаскировано: 2"],
    [
      "summarize_messages",
      { topic: "orders" },
      "orders: корзин: 3, просмотрено: 10, замаскировано: 2",
      "корзин: 3, просмотрено: 10, замаскировано: 2",
    ],
    [
      "get_group_commit_history",
      { groupId: "billing" },
      "billing: серий коммитов: 3, просмотрено записей: 10, замаскировано: 2",
      "серий коммитов: 3, просмотрено записей: 10, замаскировано: 2",
    ],
  ])("журнал: подробности %s — с целью и без неё (аргументы усечены)", async (tool, args, withTarget, withoutTarget) => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    vi.mocked(api.listConnections).mockResolvedValue(connections);
    vi.mocked(api.getMcpAudit).mockResolvedValue([
      read("c1", "", { tool, args: { connectionId: "c1", from: 0, to: 1000, ...args } }),
      read("c1", "", { tool, args: '{"connectionId":"c1"…' }),
    ]);
    renderWithClient(<McpSection />);
    await screen.findAllByText(tool);
    // Колонка «Подробности» — последняя; у чтения и обзора за текстом идёт кнопка.
    const details = screen
      .getAllByRole("row")
      .map((r) => within(r).queryAllByRole("cell").slice(-1)[0]?.textContent ?? "")
      .filter((text) => text !== "");
    expect(details).toHaveLength(2);
    expect(details[0].slice(0, withTarget.length)).toBe(withTarget);
    expect(details[1].slice(0, withoutTarget.length)).toBe(withoutTarget);
  });

  it("журнал: чтение сообщений отключено организацией — своя подсказка у кнопки", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus({ messagesAllowed: false }));
    vi.mocked(api.listConnections).mockResolvedValue(connections);
    vi.mocked(api.getMcpAudit).mockResolvedValue([read("c1", "orders")]);
    renderWithClient(<McpSection />);
    const [button] = await openButtons();
    await waitFor(() => expect(api.listConnections).toHaveBeenCalled());
    expect(button).toBeDisabled();
    expect(button).toHaveAttribute("title", "Недоступно: чтение сообщений отключено вашей организацией");
  });

  it("журнал: подключения больше нет", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    vi.mocked(api.listConnections).mockResolvedValue(connections);
    vi.mocked(api.getMcpAudit).mockResolvedValue([read("gone", "orders")]);
    renderWithClient(<McpSection />);
    const [button] = await openButtons();
    await waitFor(() => expect(button).toHaveAttribute("title", "Недоступно: подключения больше нет"));
    expect(button).toBeDisabled();
  });

  it("журнал: список подключений не загрузился — своя подсказка у кнопки", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    vi.mocked(api.listConnections).mockRejectedValue(new Error("нет ответа"));
    vi.mocked(api.getMcpAudit).mockResolvedValue([read("c1", "orders")]);
    renderWithClient(<McpSection />);
    const [button] = await openButtons();
    await waitFor(() => expect(button).toHaveAttribute("title", "Недоступно: не удалось загрузить список подключений"));
    expect(button).toBeDisabled();
  });

  it("журнал: пока список подключений грузится, кнопка неактивна без подсказки", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    vi.mocked(api.listConnections).mockReturnValue(new Promise(() => {}));
    vi.mocked(api.getMcpAudit).mockResolvedValue([read("c1", "orders")]);
    renderWithClient(<McpSection />);
    const [button] = await openButtons();
    expect(button).toBeDisabled();
    expect(button).not.toHaveAttribute("title");
  });

  it("политика запрещает MCP", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(
      mcpStatus({ state: "disabledByPolicy", enabled: true, policyAllows: false }),
    );
    renderWithClient(<McpSection />);
    const toggle = await screen.findByLabelText("Разрешить MCP-сервер");
    expect(toggle).toBeDisabled();
    expect(toggle).not.toBeChecked();
    expect(screen.getByText("MCP отключён вашей организацией")).toBeInTheDocument();
    expect(screen.queryByText("Подключение клиента")).not.toBeInTheDocument();
  });

  it("ошибка получения фрагментов конфига показывается, а не скрывается молча", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    vi.mocked(api.getMcpClientConfigs).mockRejectedValue({
      code: "INTERNAL",
      key: "errors.mcp.noExe",
      params: { error: "x" },
      message: "Cannot find the Glafira executable: x",
      retryable: false,
    });
    renderWithClient(<McpSection />);
    expect(await screen.findByText("Не удалось найти исполняемый файл Glafira: x")).toBeInTheDocument();
    expect(screen.queryByText("Подключение клиента")).not.toBeInTheDocument();
  });

  it("сессии, отзыв клиента и журнал", async () => {
    const status = mcpStatus({
      sessions: [{ id: "s1", clientName: "claude-ai", clientVersion: "1.2", startedMs: 0, calls: 7, active: false, approved: true }],
      approvedClients: [{ name: "claude-ai", version: "1.2", firstSeenMs: 0, lastSeenMs: 0 }],
    });
    vi.mocked(api.getMcpStatus).mockResolvedValue(status);
    vi.mocked(api.revokeMcpClient).mockResolvedValue(status);
    vi.mocked(api.disconnectMcpSessions).mockResolvedValue(status);
    vi.mocked(api.getMcpAudit).mockResolvedValue([
      { ts: 0, event: "call", client: "claude-ai 1.2", tool: "describe_topic", connection: { id: "c1", name: "dev" }, outcome: "error", code: "not_allowed", ms: 3 },
      { ts: 0, event: "approved", session: "s1", client: "claude-ai" },
    ]);
    const user = userEvent.setup();
    renderWithClient(<McpSection />);
    const sessions = (await screen.findByText("Активные сессии")).nextElementSibling as HTMLElement;
    expect(within(sessions).getByText("7")).toBeInTheDocument();
    await user.click(within(sessions).getByRole("button", { name: "Отключить" }));
    expect(api.disconnectMcpSessions).toHaveBeenCalledWith("s1");
    expect(await screen.findByText("describe_topic")).toBeInTheDocument();
    expect(screen.getByText("not_allowed")).toBeInTheDocument();
    expect(screen.getByText("Клиент подтверждён")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отозвать" }));
    expect(api.revokeMcpClient).toHaveBeenCalledWith("claude-ai");
    // отзыв и отключение обновляют журнал: первая загрузка + отключение + отзыв
    await waitFor(() => expect(api.getMcpAudit).toHaveBeenCalledTimes(3));
  });

  it("«Подтвердить» у ожидающей сессии запрашивает диалог подтверждения", async () => {
    const status = mcpStatus({
      sessions: [
        { id: "s1", clientName: null, clientVersion: null, startedMs: 0, calls: 0, active: false, approved: false },
        { id: "s2", clientName: "claude-code", clientVersion: "2.2", startedMs: 0, calls: 3, active: false, approved: true },
      ],
    });
    vi.mocked(api.getMcpStatus).mockResolvedValue(status);
    vi.mocked(api.requestMcpApproval).mockResolvedValue(status);
    const user = userEvent.setup();
    renderWithClient(<McpSection />);
    const sessions = (await screen.findByText("Активные сессии")).nextElementSibling as HTMLElement;
    const [waiting, approved] = within(sessions).getAllByRole("row").slice(1);
    expect(within(approved).queryByRole("button", { name: "Подтвердить" })).not.toBeInTheDocument();
    await user.click(within(waiting).getByRole("button", { name: "Подтвердить" }));
    expect(api.requestMcpApproval).toHaveBeenCalledWith("s1");
  });
});
