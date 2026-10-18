import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { MCP_STATUS_KEY, useMcpStatus } from "../../lib/useMcpStatus";
import { ErrorBanner } from "../common/ErrorBanner";

/**
 * Подтверждение нового MCP-клиента (спец. G1 §4) поверх любого экрана; несколько ожидающих —
 * по очереди. Системный window.confirm в Tauri 2 не показывается, поэтому — диалог в странице.
 */
export function ApprovalDialog() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const { data: s } = useMcpStatus();
  const answer = useMutation({
    mutationFn: ({ id, approve }: { id: string; approve: boolean }) => api.answerMcpApproval(id, approve),
    onSuccess: (st) => qc.setQueryData(MCP_STATUS_KEY, st),
  });
  const req = s?.pending[0];
  if (!s || !req) return null;
  const client = req.clientName ? `${req.clientName} ${req.clientVersion ?? ""}`.trim() : t("mcp.unknownClient");
  const decide = (approve: boolean) => answer.mutate({ id: req.sessionId, approve });

  return (
    <div className="modal-backdrop">
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby="mcp-approval-title">
        <h3 id="mcp-approval-title">{t("mcp.approval.title", { client })}</h3>
        <p>{t("mcp.approval.body", { n: s.allowedConnections })}</p>
        <p className="hint">{t("mcp.approval.readOnly")}</p>
        {!req.clientName && <p className="hint">{t("mcp.approval.unnamed")}</p>}
        {s.pending.length > 1 && <p className="muted">{t("mcp.approval.more", { n: s.pending.length - 1 })}</p>}
        {answer.error && <ErrorBanner error={answer.error} />}
        <div className="toolbar">
          <button type="button" className="primary" disabled={answer.isPending} onClick={() => decide(true)}>
            {t("mcp.approval.allow")}
          </button>
          <button type="button" disabled={answer.isPending} onClick={() => decide(false)}>
            {t("mcp.approval.reject")}
          </button>
        </div>
      </div>
    </div>
  );
}
