import type { Tab } from "../state/workspace";

export function TabContent({ tab }: { tab: Tab }) {
  switch (tab.kind) {
    case "connection-form":
      return <p>Форма подключения</p>;
    case "cluster":
      return <p>Обзор кластера</p>;
    case "topics":
      return <p>Топики</p>;
    case "topic":
      return <p>Топик {tab.topic}</p>;
  }
}
