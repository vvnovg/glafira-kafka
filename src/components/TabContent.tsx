import { lazy, Suspense } from "react";
import { useTranslation } from "react-i18next";
import type { Tab } from "../state/workspace";
import { ConnectionFormTab } from "./connections/ConnectionFormTab";
import { ClusterOverview } from "./cluster/ClusterOverview";
import { ConsumerPanel } from "./consumer/ConsumerPanel";
import { TopicList } from "./topics/TopicList";
import { GroupList } from "./groups/GroupList";
import { GroupDetails } from "./groups/GroupDetails";
import { FindingsPanel } from "./health/FindingsPanel";
import { ProducerPanel } from "./producer/ProducerPanel";
import { AboutPanel } from "./about/AboutPanel";
import { SettingsPanel } from "./settings/SettingsPanel";
import { TopicDetails } from "./topics/TopicDetails";
import { ChunkBoundary } from "./common/ChunkBoundary";

// Справка (react-markdown и главы) грузится отдельным чанком при первом открытии.
const HelpPanel = lazy(() => import("./help/HelpPanel").then((m) => ({ default: m.HelpPanel })));

function LazyHelp({ tab }: { tab: Extract<Tab, { kind: "help" }> }) {
  const { t } = useTranslation();
  return (
    <ChunkBoundary fallback={<p className="empty">{t("help.loadFailed")}</p>}>
      <Suspense fallback={<p className="empty">{t("common.loading")}</p>}>
        <HelpPanel spec={tab} />
      </Suspense>
    </ChunkBoundary>
  );
}

export function TabContent({ tab }: { tab: Tab }) {
  switch (tab.kind) {
    case "about":
      return <AboutPanel />;
    case "help":
      return <LazyHelp tab={tab} />;
    case "settings":
      return <SettingsPanel />;
    case "connection-form":
      return <ConnectionFormTab connectionId={tab.connectionId} folderId={tab.folderId ?? null} tabId={tab.id} />;
    case "cluster":
      return <ClusterOverview connectionId={tab.connectionId} />;
    case "topics":
      return <TopicList connectionId={tab.connectionId} proposal={tab.proposal} />;
    case "topic":
      return <TopicDetails connectionId={tab.connectionId} topic={tab.topic} proposal={tab.proposal} browsePrefill={tab.browsePrefill} />;
    case "groups":
      return <GroupList connectionId={tab.connectionId} />;
    case "findings":
      return <FindingsPanel connectionId={tab.connectionId} />;
    case "group":
      return <GroupDetails connectionId={tab.connectionId} groupId={tab.groupId} resetPrefill={tab.resetPrefill} proposal={tab.proposal} />;
    case "producer":
      return <ProducerPanel connectionId={tab.connectionId} prefill={tab.prefill} />;
    case "consumer":
      return <ConsumerPanel connectionId={tab.connectionId} topic={tab.topic} groupId={tab.groupId} />;
  }
}
