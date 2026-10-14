import type { TopicSummary } from "../../api/types";

export const STATS_CHUNK = 200;

export function filterTopics(topics: TopicSummary[], query: string, showInternal: boolean): TopicSummary[] {
  const q = query.trim();
  let match: (name: string) => boolean;
  if (q.length > 2 && q.startsWith("/") && q.endsWith("/")) {
    let re: RegExp;
    try {
      re = new RegExp(q.slice(1, -1), "i");
    } catch {
      return [];
    }
    match = (n) => re.test(n);
  } else {
    const lower = q.toLowerCase();
    match = (n) => n.toLowerCase().includes(lower);
  }
  return topics.filter((t) => (showInternal || !t.internal) && match(t.name));
}

export function visibleChunks(first: number, last: number, chunk = STATS_CHUNK): number[] {
  if (last < first) return [];
  const out: number[] = [];
  for (let c = Math.floor(first / chunk); c <= Math.floor(last / chunk); c++) out.push(c);
  return out;
}
