import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { mcpStatus } from "../../test/mcp";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getMcpStatus: vi.fn(), answerMcpApproval: vi.fn() } };
});

import { api } from "../../api/client";
import { ApprovalDialog } from "./ApprovalDialog";

beforeEach(() => vi.clearAllMocks());

describe("ApprovalDialog", () => {
  it("спрашивает о новом клиенте и отправляет ответ", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(
      mcpStatus({
        pending: [
          { sessionId: "s1", clientName: "claude-ai", clientVersion: "1.2", },
          { sessionId: "s2", clientName: "cursor", clientVersion: "3" },
        ],
      }),
    );
    vi.mocked(api.answerMcpApproval).mockResolvedValue(mcpStatus());
    const user = userEvent.setup();
    renderWithClient(<ApprovalDialog />);
    const dialog = await screen.findByRole("dialog", { name: "claude-ai 1.2 хочет подключиться к Glafira" });
    expect(dialog).toHaveTextContent("открытых для LLM-ассистентов: 2");
    expect(dialog).toHaveTextContent("Ждут ответа ещё: 1");
    await user.click(screen.getByRole("button", { name: "Разрешить" }));
    expect(api.answerMcpApproval).toHaveBeenCalledWith("s1", true);
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });

  it("клиент без имени: ответ только на сеанс", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(
      mcpStatus({ pending: [{ sessionId: "s9", clientName: null, clientVersion: null }] }),
    );
    vi.mocked(api.answerMcpApproval).mockResolvedValue(mcpStatus());
    const user = userEvent.setup();
    renderWithClient(<ApprovalDialog />);
    expect(await screen.findByRole("dialog", { name: "Неизвестный MCP-клиент хочет подключиться к Glafira" })).toHaveTextContent(
      "только на этот сеанс",
    );
    await user.click(screen.getByRole("button", { name: "Отклонить" }));
    expect(api.answerMcpApproval).toHaveBeenCalledWith("s9", false);
  });

  it("без ожидающих запросов ничего не показывает", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    renderWithClient(<ApprovalDialog />);
    await waitFor(() => expect(api.getMcpStatus).toHaveBeenCalled());
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });
});
