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

  it("журнал: подробности чтения сообщений и «Открыть в „Сообщениях“»", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    vi.mocked(api.listConnections).mockResolvedValue([
      { ...newProfile(), id: "c1", name: "dev", llmAccess: "messages" },
      { ...newProfile(), id: "c2", name: "meta", llmAccess: "metadata" },
    ]);
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
    vi.mocked(api.getMcpAudit).mockResolvedValue([
      read("c1", "orders"),
      read("c2", "payments"),
      read("c1", "big", { args: '{"topic":"big"…' }),
    ]);
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    renderWithClient(<McpSection />);
    expect(await screen.findByText(/orders: 3 из 10, замаскировано: 2/)).toBeInTheDocument();
    const buttons = await screen.findAllByRole("button", { name: "Открыть в «Сообщениях»" });
    expect(buttons).toHaveLength(2);
    await waitFor(() => expect(buttons[0]).toBeEnabled());
    expect(buttons[1]).toBeDisabled();
    expect(buttons[1]).toHaveAttribute("title", expect.stringContaining("Недоступно"));
    await user.click(buttons[0]);
    const tab = useWorkspace.getState().tabs[0];
    expect(tab).toMatchObject({ kind: "topic", connectionId: "c1", topic: "orders" });
    expect(tab.kind === "topic" && tab.browsePrefill?.form.startType).toBe("timestamp");
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
});
