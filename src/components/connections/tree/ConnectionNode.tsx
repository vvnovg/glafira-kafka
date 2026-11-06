import type { HTMLAttributes } from "react";
import { useTranslation } from "react-i18next";
import type { ConnectionProfile } from "../../../api/types";
import { useMcpStatus } from "../../../lib/useMcpStatus";
import { newConsumerKey, type TabSpec, useWorkspace } from "../../../state/workspace";
import { EnvBadge } from "../../common/EnvBadge";
import { Icon } from "../../common/Icon";
import { HealthBadge } from "../../health/HealthBlock";
import { TreeRow } from "./TreeRow";

/**
 * Узел подключения в дереве: точка цвета, имя, окружение и пункты «Обзор», «Топики», «Группы»…
 * rowProps — атрибуты строки (перетаскивание), их className добавляется к строке.
 */
export function ConnectionNode({ connection: c, rowProps }: { connection: ConnectionProfile; rowProps?: HTMLAttributes<HTMLDivElement> }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  // Пункт дерева подсвечен и для вкладки объекта из его списка: топик — «Топики», группа — «Consumer groups».
  const activeKind = useWorkspace((s) => {
    const tab = s.tabs.find((x) => x.id === s.activeId);
    if (!tab || !("connectionId" in tab) || tab.connectionId !== c.id) return null;
    return tab.kind === "topic" ? "topics" : tab.kind === "group" ? "groups" : tab.kind;
  });
  // Уровень «сообщения» при запрете политикой действует как «метаданные» (G4). Закрашенный значок — только когда статус
  // MCP пришёл и разрешает чтение; пока он грузится или не загрузился — как при запрете.
  const messagesAllowed = useMcpStatus().data?.messagesAllowed === true;
  const level = c.llmAccess === "messages" && !messagesAllowed ? "metadata" : c.llmAccess;

  const leaf = (kind: TabSpec["kind"], icon: "grid" | "list" | "users" | "flag" | "send" | "play", name: string, spec: TabSpec, extra?: React.ReactNode) => (
    <li>
      <TreeRow icon={icon} name={name} extra={extra} current={activeKind === kind} onActivate={() => open(spec)} />
    </li>
  );

  return (
    <li>
      <TreeRow
        chevron="leaf"
        icon={<span className="dot" style={{ background: c.color }} />}
        name={c.name}
        title={c.name}
        rowProps={{ ...rowProps, className: rowProps?.className ? `tree-conn ${rowProps.className}` : "tree-conn" }}
      >
        <EnvBadge environment={c.environment} readOnly={c.readOnly} />
        {level === "metadata" && (
          <span className="llm-badge" role="img" title={t("sidebar.llmAccess")} aria-label={t("sidebar.llmAccess")}>
            LLM
          </span>
        )}
        {level === "messages" && (
          <span className="llm-badge llm-badge--messages" role="img" title={t("sidebar.llmMessages")} aria-label={t("sidebar.llmMessages")}>
            LLM
          </span>
        )}
        <button
          type="button"
          className="icon"
          title={t("sidebar.editConnection")}
          aria-label={t("sidebar.editConnection")}
          onClick={() => open({ kind: "connection-form", connectionId: c.id })}
        >
          <Icon name="pencil" size={14} />
        </button>
      </TreeRow>
      <ul>
        {leaf("cluster", "grid", t("sidebar.cluster"), { kind: "cluster", connectionId: c.id })}
        {leaf("topics", "list", t("sidebar.topics"), { kind: "topics", connectionId: c.id })}
        {leaf("groups", "users", t("sidebar.groups"), { kind: "groups", connectionId: c.id })}
        {leaf("findings", "flag", t("sidebar.findings"), { kind: "findings", connectionId: c.id }, <HealthBadge connectionId={c.id} />)}
        {leaf("producer", "send", t("sidebar.producer"), { kind: "producer", connectionId: c.id })}
        {leaf("consumer", "play", t("sidebar.consumer"), { kind: "consumer", connectionId: c.id, key: newConsumerKey() })}
      </ul>
    </li>
  );
}
