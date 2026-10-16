import { Channel, invoke } from "@tauri-apps/api/core";
import type {
  AppError,
  BatchEvent,
  BatchParams,
  CommitTarget,
  ConsumeEvent,
  ConsumerParams,
  HistoryEntry,
  AppInfo,
  UpdateCheck,
  LanguageSettings,
  LicenseStatus,
  PolicyStatus,
  ProduceMessage,
  ProduceResult,
  ProducerSettings,
  Template,
  TopicPartitionRef,
  ConfigChange,
  Confirmation,
  GroupDescription,
  GroupLag,
  GroupList,
  GroupPartitionOffset,
  NewTopicSpec,
  OffsetTarget,
  PurgedPartition,
  ResetPreviewRow,
  ResetRequest,
  ResetResult,
  TopicDeleteResult,
  TopicGroups,
  TopicPolicy,
  BrowseEvent,
  BrowseParams,
  ClusterOverview,
  ConfigEntry,
  ConnectionProfile,
  TestReport,
  TlsPasswordsInput,
  TopicDetails,
  TopicStats,
  TopicSummary,
} from "./types";

export function isAppError(e: unknown): e is AppError {
  return typeof e === "object" && e !== null && "code" in e && "message" in e;
}

export function toAppError(e: unknown): AppError {
  // Ошибка из Rust приходит целиком: key/params/details сохраняются как есть.
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
  getAppInfo: () => call<AppInfo>("get_app_info"),
  exportDiagnostics: (path: string) => call<void>("export_diagnostics", { path }),
  checkUpdate: () => call<UpdateCheck>("check_update"),
  installUpdate: (force: boolean) => call<void>("install_update", { force }),
  getLanguageSettings: () => call<LanguageSettings>("get_language_settings"),
  setLanguage: (lang: string | null) => call<LanguageSettings>("set_language", { lang }),
  getLicenseStatus: () => call<LicenseStatus>("get_license_status"),
  setLicenseKey: (key: string) => call<LicenseStatus>("set_license_key", { key }),
  removeLicenseKey: () => call<LicenseStatus>("remove_license_key"),
  getPolicyStatus: () => call<PolicyStatus>("get_policy_status"),
  listConnections: () => call<ConnectionProfile[]>("list_connections"),
  saveConnection: (profile: ConnectionProfile, saslPassword?: string, tlsPasswords?: TlsPasswordsInput) =>
    call<ConnectionProfile>("save_connection", {
      input: { profile, saslPassword: saslPassword ?? null, tlsPasswords: tlsPasswords ?? null },
    }),
  deleteConnection: (id: string) => call<void>("delete_connection", { id }),
  testConnection: (profile: ConnectionProfile, saslPassword?: string, tlsPasswords?: TlsPasswordsInput) =>
    call<TestReport>("test_connection", {
      input: { profile, saslPassword: saslPassword ?? null, tlsPasswords: tlsPasswords ?? null },
    }),

  getClusterOverview: (connectionId: string) => call<ClusterOverview>("get_cluster_overview", { connectionId }),
  getBrokerConfig: (connectionId: string, brokerId: number) =>
    call<ConfigEntry[]>("get_broker_config", { connectionId, brokerId }),

  listTopics: (connectionId: string) => call<TopicSummary[]>("list_topics", { connectionId }),
  getTopicStats: (connectionId: string, topics: string[]) =>
    call<TopicStats[]>("get_topic_stats", { connectionId, topics }),
  getTopic: (connectionId: string, topic: string) => call<TopicDetails>("get_topic", { connectionId, topic }),
  getTopicConfigs: (connectionId: string, topic: string) =>
    call<ConfigEntry[]>("get_topic_configs", { connectionId, topic }),

  createTopic: (connectionId: string, spec: NewTopicSpec, confirm: string | null) =>
    call<void>("create_topic", { connectionId, spec, confirm }),
  alterTopicConfigs: (connectionId: string, topic: string, changes: ConfigChange[], confirm: string | null) =>
    call<void>("alter_topic_configs", { connectionId, topic, changes, confirm }),
  addPartitions: (connectionId: string, topic: string, newTotal: number, confirm: string | null) =>
    call<void>("add_partitions", { connectionId, topic, newTotal, confirm }),
  purgeTopic: (connectionId: string, topic: string, partitions: number[] | null, c: Confirmation) =>
    call<PurgedPartition[]>("purge_topic", { connectionId, topic, partitions, confirm: c.confirm, typedName: c.typedName }),
  deleteTopics: (connectionId: string, topics: string[], c: Confirmation) =>
    call<TopicDeleteResult[]>("delete_topics", {
      connectionId,
      topics,
      confirm: c.confirm,
      typedConfirmation: c.typedName,
    }),
  getTopicPolicies: (connectionId: string, topics: string[]) =>
    call<TopicPolicy[]>("get_topic_policies", { connectionId, topics }),

  listGroups: (connectionId: string) => call<GroupList>("list_groups", { connectionId }),
  describeGroups: (connectionId: string, groupIds: string[]) =>
    call<GroupDescription[]>("describe_groups", { connectionId, groupIds }),
  getGroupOffsets: (connectionId: string, groupId: string) =>
    call<GroupPartitionOffset[]>("get_group_offsets", { connectionId, groupId }),
  getGroupsLag: (connectionId: string, groupIds: string[]) =>
    call<GroupLag[]>("get_groups_lag", { connectionId, groupIds }),
  getTopicGroups: (connectionId: string, topic: string) => call<TopicGroups>("get_topic_groups", { connectionId, topic }),
  previewOffsetReset: (connectionId: string, request: ResetRequest) =>
    call<ResetPreviewRow[]>("preview_offset_reset", { connectionId, request }),
  resetOffsets: (connectionId: string, groupId: string, offsets: OffsetTarget[], confirm: string | null) =>
    call<ResetResult[]>("reset_offsets", { connectionId, groupId, offsets, confirm }),
  deleteGroup: (connectionId: string, groupId: string, c: Confirmation) =>
    call<void>("delete_group", { connectionId, groupId, confirm: c.confirm, typedName: c.typedName }),

  startBrowse: (params: BrowseParams, onEvent: (e: BrowseEvent) => void) => {
    const channel = new Channel<BrowseEvent>();
    channel.onmessage = onEvent;
    return call<string>("start_browse", { params, onEvent: channel });
  },
  stopSession: (sessionId: string) => call<boolean>("stop_session", { sessionId }),

  produce: (connectionId: string, message: ProduceMessage, settings: ProducerSettings, confirm: string | null) =>
    call<ProduceResult>("produce", { connectionId, message, settings, confirm }),
  listHistory: (connectionId: string) => call<HistoryEntry[]>("list_history", { connectionId }),
  listTemplates: (connectionId: string) => call<Template[]>("list_templates", { connectionId }),
  saveTemplate: (template: Template) => call<Template>("save_template", { template }),
  deleteTemplate: (id: string) => call<void>("delete_template", { id }),
  startBatch: (params: BatchParams, onEvent: (e: BatchEvent) => void, confirm: string | null) => {
    const channel = new Channel<BatchEvent>();
    channel.onmessage = onEvent;
    return call<string>("produce_batch", { params, onEvent: channel, confirm });
  },

  startConsumer: (params: ConsumerParams, onEvent: (e: ConsumeEvent) => void, confirm: string | null) => {
    const channel = new Channel<ConsumeEvent>();
    channel.onmessage = onEvent;
    return call<string>("start_consumer", { params, onEvent: channel, confirm });
  },
  commitOffsets: (sessionId: string, target: CommitTarget, confirm: string | null) =>
    call<OffsetTarget[]>("commit_offsets", { sessionId, target, confirm }),
  pausePartitions: (sessionId: string, partitions: TopicPartitionRef[]) =>
    call<void>("pause_partitions", { sessionId, partitions }),
  resumePartitions: (sessionId: string, partitions: TopicPartitionRef[]) =>
    call<void>("resume_partitions", { sessionId, partitions }),
  suggestGroupId: () => call<string>("suggest_group_id"),
};
