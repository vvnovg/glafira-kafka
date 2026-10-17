import { useTranslation } from "react-i18next";
import type { ConnectionProfile } from "../../../api/types";
import { newConsumerKey, useWorkspace } from "../../../state/workspace";
import { proHint, useLicense } from "../../../lib/useLicense";
import { ProLock } from "../../common/ProLock";
import { EnvBadge } from "../../common/EnvBadge";

/** Узел подключения в дереве: точка цвета, имя, окружение и пункты «Обзор», «Топики», «Группы»… */
export function ConnectionNode({ connection: c }: { connection: ConnectionProfile }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const { locked } = useLicense();

  return (
    <li>
      <div className="tree-conn">
        <span className="dot" style={{ background: c.color }} />
        <span className="tree-name">{c.name}</span>
        <EnvBadge environment={c.environment} readOnly={c.readOnly} />
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
          <button type="button" className="link" onClick={() => open({ kind: "producer", connectionId: c.id })}>
            {t("sidebar.producer")}
          </button>
        </li>
        <li>
          <button
            type="button"
            className="link"
            disabled={locked}
            title={locked ? proHint() : undefined}
            onClick={() => open({ kind: "consumer", connectionId: c.id, key: newConsumerKey() })}
          >
            {t("sidebar.consumer")}
            {locked && (
              <>
                {" "}
                <ProLock />
              </>
            )}
          </button>
        </li>
      </ul>
    </li>
  );
}
