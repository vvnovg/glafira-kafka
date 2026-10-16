import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { BrowsedMessage, DecodedData } from "../../api/types";
import { tr } from "../../i18n/text";
import { formatBytes, formatTimestamp } from "../../lib/format";
import { ErrorBanner } from "../common/ErrorBanner";
import { useConnection } from "../../lib/useConnection";
import { useWorkspace } from "../../state/workspace";
import { browsedToMessage } from "../producer/fromMessage";
import { prettyText } from "./params";

function copy(text: string) {
  try {
    void navigator.clipboard.writeText(text).catch(() => {});
  } catch {
    /* буфер обмена недоступен */
  }
}

function DataBlock({ kind, data }: { kind: "key" | "value"; data: DecodedData | null }) {
  const { t } = useTranslation();
  const title = t(kind === "key" ? "browse.details.key" : "browse.details.value");
  return (
    <section>
      <h4>
        {title} {data && <span className="muted">({data.format}, {formatBytes(data.size)})</span>}
        {data && (
          <button
            type="button"
            className="link"
            aria-label={t(kind === "key" ? "browse.details.copyKeyAria" : "browse.details.copyValueAria")}
            onClick={() => copy(data.text)}
          >
            {t("browse.details.copy")}
          </button>
        )}
      </h4>
      {data?.error && <p className="warn">{tr(data.error)}</p>}
      {data?.truncated && <p className="warn">{t("browse.details.truncated")}</p>}
      <pre>{data ? prettyText(data) : <span className="muted">null</span>}</pre>
    </section>
  );
}

interface Props {
  message: BrowsedMessage | null;
  utc: boolean;
  connectionId: string;
}

export function MessageDetails({ message, utc, connectionId }: Props) {
  const { t } = useTranslation();
  const connection = useConnection(connectionId);
  const open = useWorkspace((s) => s.open);
  // Причина, по которой повторная отправка невозможна (например, усечённое значение), показывается в интерфейсе.
  const resend = useMemo(() => {
    if (!message) return null;
    try {
      return { prefill: browsedToMessage(message), error: null };
    } catch (e) {
      return { prefill: null, error: (e as Error).message };
    }
  }, [message]);
  if (!message) return <div className="details-pane muted">{t("browse.details.select")}</div>;
  return (
    <div className="details-pane">
      <p className="muted">
        {t("browse.details.meta", { partition: message.partition, offset: message.offset, time: formatTimestamp(message.timestamp, utc) })}
      </p>
      <div className="toolbar">
        <button
          type="button"
          className="link"
          aria-label={t("browse.details.copyJsonAria")}
          onClick={() => copy(JSON.stringify(message, null, 2))}
        >
          {t("browse.details.copyJson")}
        </button>
        {connection && !connection.readOnly && (
          <button
            type="button"
            className="link"
            disabled={!resend?.prefill}
            title={resend?.error ?? undefined}
            onClick={() => {
              if (resend?.prefill) open({ kind: "producer", connectionId, prefill: { message: resend.prefill, nonce: Date.now() } });
            }}
          >
            {t("browse.details.resend")}
          </button>
        )}
        {connection && !connection.readOnly && <ResetGroupHere connectionId={connectionId} topic={message.topic} message={message} />}
      </div>
      {resend?.error && <p className="warn">{resend.error}</p>}
      <DataBlock kind="value" data={message.value} />
      <DataBlock kind="key" data={message.key} />
      <section>
        <h4>{t("browse.details.headers", { count: message.headers.length })}</h4>
        {message.headers.length > 0 && (
          <table>
            <tbody>
              {message.headers.map((h, i) => (
                <tr key={`${h.name}-${i}`}>
                  <td>{h.name}</td>
                  <td>
                    <code>{h.value ?? "null"}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}

/** Выбор группы → вкладка группы с предзаполненным сбросом: группа начнёт чтение с этого сообщения. */
function ResetGroupHere({ connectionId, topic, message }: { connectionId: string; topic: string; message: BrowsedMessage }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [group, setGroup] = useState("");
  const openTab = useWorkspace((s) => s.open);
  const groups = useQuery({
    queryKey: ["groups", connectionId],
    queryFn: () => api.listGroups(connectionId),
    enabled: open,
  });
  if (!open) {
    return (
      <button type="button" className="link" onClick={() => setOpen(true)}>
        {t("browse.details.resetHere")}
      </button>
    );
  }
  const list = groups.data?.groups ?? [];
  // Группа выбирается только из списка: опечатка не создаст коммиты для несуществующей группы.
  const valid = list.some((g) => g.groupId === group);
  return (
    <div>
      {groups.error && <ErrorBanner error={groups.error} onRetry={() => groups.refetch()} />}
      {groups.data && groups.data.errors.length > 0 && (
        <p className="warn">{t("browse.details.incompleteGroups", { errors: groups.data.errors.map(tr).join("; ") })}</p>
      )}
      <div className="toolbar">
        {groups.isPending ? (
          <span className="muted">{t("browse.details.loadingGroups")}</span>
        ) : (
          <select aria-label={t("browse.details.groupAria")} value={group} onChange={(e) => setGroup(e.target.value)} disabled={list.length === 0}>
            <option value="">{list.length === 0 ? t("browse.details.noGroups") : t("browse.details.chooseGroup")}</option>
            {list.map((g) => (
              <option key={g.groupId} value={g.groupId}>
                {g.groupId}
              </option>
            ))}
          </select>
        )}
        <button
          type="button"
          disabled={!valid}
          onClick={() => {
            openTab({
              kind: "group",
              connectionId,
              groupId: group,
              resetPrefill: { topic, partition: message.partition, offset: message.offset, nonce: Date.now() },
            });
            setOpen(false);
          }}
        >
          {t("browse.details.goToReset")}
        </button>
        <button type="button" onClick={() => setOpen(false)}>
          {t("browse.details.cancel")}
        </button>
      </div>
    </div>
  );
}
