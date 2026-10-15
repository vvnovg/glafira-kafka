import type { GroupSummary } from "../../api/types";

/** Детали (участники, лаг) грузятся лениво чанками по 50 групп из видимой области. */
export const GROUP_CHUNK = 50;

export function filterGroups(groups: GroupSummary[], query: string, state: string): GroupSummary[] {
  const q = query.trim().toLowerCase();
  return groups.filter((g) => (state === "all" || g.state === state) && (!q || g.groupId.toLowerCase().includes(q)));
}

export function groupStates(groups: GroupSummary[]): string[] {
  return [...new Set(groups.map((g) => g.state))].sort();
}
