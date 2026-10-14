import { useState } from "react";
import type { ConfigEntry } from "../../api/types";

export function filterConfigs(entries: ConfigEntry[], query: string, onlyChanged: boolean): ConfigEntry[] {
  const q = query.trim().toLowerCase();
  return entries.filter((e) => (!onlyChanged || !e.isDefault) && (!q || e.name.toLowerCase().includes(q)));
}

export function ConfigTable({ entries }: { entries: ConfigEntry[] }) {
  const [query, setQuery] = useState("");
  const [onlyChanged, setOnlyChanged] = useState(false);
  const rows = filterConfigs(entries, query, onlyChanged);
  return (
    <div>
      <div className="toolbar">
        <input placeholder="Поиск параметра" value={query} onChange={(e) => setQuery(e.target.value)} />
        <label>
          <input type="checkbox" checked={onlyChanged} onChange={(e) => setOnlyChanged(e.target.checked)} /> Только изменённые
        </label>
        <span className="muted">
          {rows.length} из {entries.length}
        </span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Параметр</th>
            <th>Значение</th>
            <th>Источник</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((e) => (
            <tr key={e.name}>
              <td style={{ fontWeight: e.isDefault ? 400 : 600 }}>
                {e.name}
                {e.isReadOnly && <span className="muted" title="Только чтение"> 🔒</span>}
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
