import type { GroupPartitionOffset } from "../../api/types";

/**
 * Как total_lag в Rust: null, если хоть одна партиция с ошибкой или коммитов нет вовсе;
 * партиции без коммита не суммируются. */
export function groupTotalLag(rows: GroupPartitionOffset[]): number | null {
  if (rows.some((r) => r.error) || rows.every((r) => r.committed === null)) return null;
  return rows.reduce((sum, r) => sum + (r.lag ?? 0), 0);
}
