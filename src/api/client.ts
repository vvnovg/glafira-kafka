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
  McpAuditEntry,
  McpClientConfigs,
  McpStatus,
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
  HealthView,
  AnnotationStatus,
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
  ConnectionFolder,
  ExportSummary,
  ImportPreview,
  ConnectionProfile,
  TestReport,
  TlsPasswordsInput,
  TopicDetails,
  TopicStats,
  TopicSummary,
  TreeItemRef,
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
  getPolicyStatus: () => call<PolicyStatus>("get_policy_status"),
  getMcpStatus: () => call<McpStatus>("get_mcp_status"),
  setMcpEnabled: (enabled: boolean) => call<McpStatus>("set_mcp_enabled", { enabled }),
  answerMcpApproval: (sessionId: string, approve: boolean) =>
    call<McpStatus>("answer_mcp_approval", { sessionId, approve }),
  revokeMcpClient: (name: string) => call<McpStatus>("revoke_mcp_client", { name }),
  disconnectMcpSessions: (sessionId: string | null) => call<McpStatus>("disconnect_mcp_sessions", { sessionId }),
  requestMcpApproval: (sessionId: string) => call<McpStatus>("request_mcp_approval", { sessionId }),
  getMcpAudit: (limit: number) => call<McpAuditEntry[]>("get_mcp_audit", { limit }),
  getMcpClientConfigs: () => call<McpClientConfigs>("get_mcp_client_configs"),
  revealMcpAudit: () => call<void>("reveal_mcp_audit"),
  runHealthCheck: (connectionId: string) => call<HealthView>("run_health_check", { connectionId }),
  getHealth: (connectionId: string) => call<HealthView>("get_health", { connectionId }),
  setFindingIgnored: (connectionId: string, key: string, ignored: boolean) =>
    call<HealthView>("set_finding_ignored", { connectionId, key, ignored }),
  setAnnotationStatus: (connectionId: string, annotationId: string, status: AnnotationStatus) =>
    call<HealthView>("set_annotation_status", { connectionId, annotationId, status }),
  deleteAnnotation: (connectionId: string, annotationId: string) =>
    call<HealthView>("delete_annotation", { connectionId, annotationId }),
  deleteAllAnnotations: (connectionId: string) => call<HealthView>("delete_all_annotations", { connectionId }),
  setProposalStatus: (connectionId: string, proposalId: string, status: "rejected" | "open", reason: string | null) =>
    call<HealthView>("set_proposal_status", { connectionId, proposalId, status, reason }),
  deleteProposal: (connectionId: string, proposalId: string) =>
    call<HealthView>("delete_proposal", { connectionId, proposalId }),
  deleteAllProposals: (connectionId: string) => call<HealthView>("delete_all_proposals", { connectionId }),
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
  listFolders: () => call<ConnectionFolder[]>("list_folders"),
  createFolder: (name: string, parentId: string | null) =>
    call<ConnectionFolder>("create_folder", { name, parentId }),
  renameFolder: (id: string, name: string) => call<ConnectionFolder>("rename_folder", { id, name }),
  deleteFolder: (id: string) => call<void>("delete_folder", { id }),
  moveItems: (items: TreeItemRef[], parentId: string | null, before: TreeItemRef | null) =>
    call<void>("move_items", { items, parentId, before }),
  exportConnections: (path: string, folderId: string | null) =>
    call<ExportSummary>("export_connections", { path, folderId }),
  previewImport: (path: string, targetFolderId: string | null, updateExisting: boolean) =>
    call<ImportPreview>("preview_import", { path, targetFolderId, updateExisting }),
  applyImport: (path: string, targetFolderId: string | null, updateExisting: boolean) =>
    call<ImportPreview>("apply_import", { path, targetFolderId, updateExisting }),

  getClusterOverview: (connectionId: string) => call<ClusterOverview>("get_cluster_overview", { connectionId }),
  getBrokerConfig: (connectionId: string, brokerId: number) =>
    call<ConfigEntry[]>("get_broker_config", { connectionId, brokerId }),

  listTopics: (connectionId: string) => call<TopicSummary[]>("list_topics", { connectionId }),
  getTopicStats: (connectionId: string, topics: string[]) =>
    call<TopicStats[]>("get_topic_stats", { connectionId, topics }),
  getTopic: (connectionId: string, topic: string) => call<TopicDetails>("get_topic", { connectionId, topic }),
  getTopicConfigs: (connectionId: string, topic: string) =>
    call<ConfigEntry[]>("get_topic_configs", { connectionId, topic }),

  // `proposalId` — форма открыта из предложения LLM: Rust отметит его применённым после успеха.
  createTopic: (connectionId: string, spec: NewTopicSpec, confirm: string | null, proposalId: string | null = null) =>
    call<void>("create_topic", { connectionId, spec, confirm, proposalId }),
  alterTopicConfigs: (connectionId: string, topic: string, changes: ConfigChange[], confirm: string | null, proposalId: string | null = null) =>
    call<void>("alter_topic_configs", { connectionId, topic, changes, confirm, proposalId }),
  addPartitions: (connectionId: string, topic: string, newTotal: number, confirm: string | null, proposalId: string | null = null) =>
    call<void>("add_partitions", { connectionId, topic, newTotal, confirm, proposalId }),
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
  // `proposal` — сброс открыт из предложения LLM: его id и запрос предпросмотра для сравнения.
  resetOffsets: (
    connectionId: string,
    groupId: string,
    offsets: OffsetTarget[],
    confirm: string | null,
    proposal: { id: string; request: ResetRequest } | null = null,
  ) =>
    call<ResetResult[]>("reset_offsets", {
      connectionId,
      groupId,
      offsets,
      confirm,
      proposalId: proposal?.id ?? null,
      request: proposal?.request ?? null,
    }),
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
