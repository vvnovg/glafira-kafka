import type { Tab } from "../state/workspace";
import { ConnectionFormTab } from "./connections/ConnectionFormTab";
import { ClusterOverview } from "./cluster/ClusterOverview";
import { TopicList } from "./topics/TopicList";

export function TabContent({ tab }: { tab: Tab }) {
  switch (tab.kind) {
    case "connection-form":
      return <ConnectionFormTab connectionId={tab.connectionId} tabId={tab.id} />;
    case "cluster":
      return <ClusterOverview connectionId={tab.connectionId} />;
    case "topics":
      return <TopicList connectionId={tab.connectionId} />;
    case "topic":
      return <p>Топик {tab.topic}</p>;
  }
}
