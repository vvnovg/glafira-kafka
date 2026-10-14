import type { Tab } from "../state/workspace";
import { ConnectionFormTab } from "./connections/ConnectionFormTab";
import { ClusterOverview } from "./cluster/ClusterOverview";

export function TabContent({ tab }: { tab: Tab }) {
  switch (tab.kind) {
    case "connection-form":
      return <ConnectionFormTab connectionId={tab.connectionId} tabId={tab.id} />;
    case "cluster":
      return <ClusterOverview connectionId={tab.connectionId} />;
    case "topics":
      return <p>Топики</p>;
    case "topic":
      return <p>Топик {tab.topic}</p>;
  }
}
