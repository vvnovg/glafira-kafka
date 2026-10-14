import type { Tab } from "../state/workspace";
import { ConnectionFormTab } from "./connections/ConnectionFormTab";
import { ClusterOverview } from "./cluster/ClusterOverview";
import { TopicList } from "./topics/TopicList";
import { TopicDetails } from "./topics/TopicDetails";

export function TabContent({ tab }: { tab: Tab }) {
  switch (tab.kind) {
    case "connection-form":
      return <ConnectionFormTab connectionId={tab.connectionId} tabId={tab.id} />;
    case "cluster":
      return <ClusterOverview connectionId={tab.connectionId} />;
    case "topics":
      return <TopicList connectionId={tab.connectionId} />;
    case "topic":
      return <TopicDetails connectionId={tab.connectionId} topic={tab.topic} />;
  }
}
