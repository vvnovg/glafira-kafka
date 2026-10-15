import type { ConfigChange, ConfigEntry } from "../../api/types";

export type ConfigEdit = { kind: "set"; value: string } | { kind: "reset" };

export interface DiffRow {
  name: string;
  before: string | null;
  /** null — значение по умолчанию (DELETE) */
  after: string | null;
}

export function isEditable(e: ConfigEntry): boolean {
  return !e.isReadOnly && !e.isSensitive;
}

/** Правки → изменения для IncrementalAlterConfigs и строки diff «было → стало» (по имени параметра). */
export function diffConfigs(
  entries: ConfigEntry[],
  edits: Record<string, ConfigEdit>,
): { changes: ConfigChange[]; rows: DiffRow[] } {
  const byName = new Map(entries.map((e) => [e.name, e]));
  const changes: ConfigChange[] = [];
  const rows: DiffRow[] = [];
  for (const name of Object.keys(edits).sort()) {
    const e = byName.get(name);
    const edit = edits[name];
    if (!e || !isEditable(e)) continue;
    if (edit.kind === "reset") {
      if (e.isDefault) continue;
      changes.push({ op: "delete", name });
      rows.push({ name, before: e.value, after: null });
    } else {
      if (edit.value === (e.value ?? "")) continue;
      changes.push({ op: "set", name, value: edit.value });
      rows.push({ name, before: e.value, after: edit.value });
    }
  }
  return { changes, rows };
}
