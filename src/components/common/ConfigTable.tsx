import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { ConfigEntry } from "../../api/types";

export function filterConfigs(entries: ConfigEntry[], query: string, onlyChanged: boolean): ConfigEntry[] {
  const q = query.trim().toLowerCase();
  return entries.filter((e) => (!onlyChanged || !e.isDefault) && (!q || e.name.toLowerCase().includes(q)));
}

export function ConfigTable({ entries }: { entries: ConfigEntry[] }) {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [onlyChanged, setOnlyChanged] = useState(false);
  const rows = filterConfigs(entries, query, onlyChanged);
  return (
    <div>
      <div className="toolbar">
        <input aria-label={t("common.config.searchParam")} placeholder={t("common.config.searchParam")} value={query} onChange={(e) => setQuery(e.target.value)} />
        <label>
          <input type="checkbox" checked={onlyChanged} onChange={(e) => setOnlyChanged(e.target.checked)} /> {t("common.config.onlyChanged")}
        </label>
        <span className="muted">
          {t("common.config.shownOf", { shown: rows.length, total: entries.length })}
        </span>
      </div>
      <table>
        <thead>
          <tr>
            <th>{t("common.config.colParam")}</th>
            <th>{t("common.config.colValue")}</th>
            <th>{t("common.config.colSource")}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((e) => (
            <tr key={e.name}>
              <td style={{ fontWeight: e.isDefault ? 400 : 600 }}>
                {e.name}
                {e.isReadOnly && <span className="muted" role="img" aria-label={t("common.readOnly")} title={t("common.readOnly")}> 🔒</span>}
              </td>
              <td>
                <code>{e.isSensitive ? "••••••" : (e.value ?? "—")}</code>
              </td>
              <td className="muted">{e.source}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
