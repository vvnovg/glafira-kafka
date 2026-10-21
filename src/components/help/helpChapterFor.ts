import type { ChapterId } from "../../guide/chapters";
import type { TabSpec } from "../../state/workspace";

/** Глава руководства для экрана (F1), таблица §3 спеки D. */
export function helpChapterFor(tab: TabSpec | null): ChapterId {
  switch (tab?.kind) {
    case "connection-form":
      return "connections";
    case "cluster":
    case "topics":
    case "topic":
      return "topics";
    case "producer":
      return "producer";
    case "groups":
    case "group":
      return "groups";
    case "consumer":
      return "consumer-sessions";
    case "findings":
      return "health-check";
    case "about":
      return "updates";
    default:
      return "getting-started";
  }
}
