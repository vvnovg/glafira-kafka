import { Channel, invoke } from "@tauri-apps/api/core";
import type {
  AppError,
  BrowseEvent,
  BrowseParams,
  ClusterOverview,
  ConfigEntry,
  ConnectionProfile,
  TestReport,
  TopicDetails,
  TopicStats,
  TopicSummary,
} from "./types";

export function isAppError(e: unknown): e is AppError {
  return typeof e === "object" && e !== null && "code" in e && "message" in e;
}

export function toAppError(e: unknown): AppError {
  if (isAppError(e)) return e;
  if (e instanceof Error) return { code: "INTERNAL", message: e.message, retryable: false };
  return { code: "INTERNAL", message: String(e), retryable: false };
}

async function call<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  try {
    return await invoke<T>(cmd, args);
  } catch (e) {
    throw toAppError(e);
  }
}

export const api = {
  listConnections: () => call<ConnectionProfile[]>("list_connections"),
  saveConnection: (profile: ConnectionProfile, saslPassword?: string) =>
    call<ConnectionProfile>("save_connection", { input: { profile, saslPassword: saslPassword ?? null } }),
  deleteConnection: (id: string) => call<void>("delete_connection", { id }),
  testConnection: (profile: ConnectionProfile, saslPassword?: string) =>
    call<TestReport>("test_connection", { input: { profile, saslPassword: saslPassword ?? null } }),

  getClusterOverview: (connectionId: string) => call<ClusterOverview>("get_cluster_overview", { connectionId }),
  getBrokerConfig: (connectionId: string, brokerId: number) =>
    call<ConfigEntry[]>("get_broker_config", { connectionId, brokerId }),

  listTopics: (connectionId: string) => call<TopicSummary[]>("list_topics", { connectionId }),
  getTopicStats: (connectionId: string, topics: string[]) =>
    call<TopicStats[]>("get_topic_stats", { connectionId, topics }),
  getTopic: (connectionId: string, topic: string) => call<TopicDetails>("get_topic", { connectionId, topic }),
  getTopicConfigs: (connectionId: string, topic: string) =>
    call<ConfigEntry[]>("get_topic_configs", { connectionId, topic }),

  startBrowse: (params: BrowseParams, onEvent: (e: BrowseEvent) => void) => {
    const channel = new Channel<BrowseEvent>();
    channel.onmessage = onEvent;
    return call<string>("start_browse", { params, onEvent: channel });
  },
  stopSession: (sessionId: string) => call<boolean>("stop_session", { sessionId }),
};
