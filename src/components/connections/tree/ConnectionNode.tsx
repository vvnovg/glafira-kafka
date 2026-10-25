import type { HTMLAttributes } from "react";
import { useTranslation } from "react-i18next";
import type { ConnectionProfile } from "../../../api/types";
import { useMcpStatus } from "../../../lib/useMcpStatus";
import { newConsumerKey, useWorkspace } from "../../../state/workspace";
import { EnvBadge } from "../../common/EnvBadge";
import { HealthBadge } from "../../health/HealthBlock";

/**
 * Узел подключения в дереве: точка цвета, имя, окружение и пункты «Обзор», «Топики», «Группы»…
 * rowProps — атрибуты строки (перетаскивание), их className добавляется к `tree-conn`.
 */
export function ConnectionNode({ connection: c, rowProps }: { connection: ConnectionProfile; rowProps?: HTMLAttributes<HTMLDivElement> }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  // Уровень «сообщения» при запрете политикой действует как «метаданные» (G4). Закрашенный значок — только когда статус
  // MCP пришёл и разрешает чтение; пока он грузится или не загрузился — как при запрете.
  const messagesAllowed = useMcpStatus().data?.messagesAllowed === true;
  const level = c.llmAccess === "messages" && !messagesAllowed ? "metadata" : c.llmAccess;

  return (
    <li>
      <div {...rowProps} className={rowProps?.className ? `tree-conn ${rowProps.className}` : "tree-conn"}>
        <span className="dot" style={{ background: c.color }} />
        <span className="tree-name">{c.name}</span>
        <EnvBadge environment={c.environment} readOnly={c.readOnly} />
        {level === "metadata" && (
          <span className="llm-badge" title={t("sidebar.llmAccess")}>
            LLM
          </span>
        )}
        {level === "messages" && (
          <span className="llm-badge llm-badge--messages" title={t("sidebar.llmMessages")}>
            LLM
          </span>
        )}
        <button type="button" className="icon" title={t("sidebar.editConnection")} onClick={() => open({ kind: "connection-form", connectionId: c.id })}>
          ✎
        </button>
      </div>
      <ul>
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "cluster", connectionId: c.id })}>
            {t("sidebar.cluster")}
          </button>
        </li>
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "topics", connectionId: c.id })}>
            {t("sidebar.topics")}
          </button>
        </li>
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "groups", connectionId: c.id })}>
            {t("sidebar.groups")}
          </button>
        </li>
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "findings", connectionId: c.id })}>
            {t("sidebar.findings")} <HealthBadge connectionId={c.id} />
          </button>
        </li>
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "producer", connectionId: c.id })}>
            {t("sidebar.producer")}
          </button>
        </li>
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "consumer", connectionId: c.id, key: newConsumerKey() })}>
            {t("sidebar.consumer")}
          </button>
        </li>
      </ul>
    </li>
  );
}
