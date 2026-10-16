export type ErrorCode =
  | "CONNECTION_FAILED"
  | "TLS_ERROR"
  | "AUTH_FAILED"
  | "NOT_AUTHORIZED"
  | "TIMEOUT"
  | "INVALID_INPUT"
  | "NOT_FOUND"
  | "READ_ONLY"
  | "CONFIRMATION_REQUIRED"
  | "GROUP_NOT_EMPTY"
  | "PRO_REQUIRED"
  | "INTERNAL";

/** Текст из Rust: ключ словаря, параметры и английский текст (запасной вариант). */
export interface LocalizedText {
  key: string;
  params: Record<string, string>;
  text: string;
}

export interface AppError {
  code: ErrorCode;
  /** Ключ словаря; у технического текста без перевода — `errors.generic` с `params.message`. */
  key?: string;
  params?: Record<string, string>;
  /** Английский текст (запасной вариант перевода). */
  message: string;
  details?: string;
  retryable: boolean;
}

export type Environment = "local" | "dev" | "test" | "stage" | "prod";
export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";

export type CertFormat = "pem" | "pkcs12" | "jks";

export interface ClientCert {
  format: CertFormat;
  certificatePath: string;
  keyPath: string | null;
  alias: string | null;
}

export interface TlsConfig {
  caLocation: string | null;
  caFormat: CertFormat;
  verifyHostname: boolean;
  clientCert: ClientCert | null;
}

/** null — не менять сохранённый пароль, "" — удалить, иначе — новый пароль. */
export interface TlsPasswordsInput {
  keyPassword: string | null;
  keystorePassword: string | null;
  truststorePassword: string | null;
}

export interface SaslConfig {
  mechanism: SaslMechanism;
  username: string;
}

export type SecurityConfig =
  | { protocol: "PLAINTEXT" }
  | { protocol: "SSL"; tls: TlsConfig }
  | { protocol: "SASL_PLAINTEXT"; sasl: SaslConfig }
  | { protocol: "SASL_SSL"; tls: TlsConfig; sasl: SaslConfig };

export type Protocol = SecurityConfig["protocol"];

export interface ConnectionProfile {
  id: string;
  name: string;
  color: string;
  environment: Environment;
  readOnly: boolean;
  bootstrapServers: string;
  security: SecurityConfig;
  extraProperties: Record<string, string>;
  requestTimeoutMs: number;
}

export interface TestStep {
  name: "config" | "tcp" | "kafka";
  ok: boolean;
  message: LocalizedText;
  error?: AppError;
}

export interface TestReport {
  ok: boolean;
  steps: TestStep[];
}

export interface BrokerInfo {
  id: number;
  host: string;
  port: number;
}

export interface ClusterOverview {
  clusterId: string | null;
  controllerId: number | null;
  brokers: BrokerInfo[];
  topicCount: number;
  internalTopicCount: number;
  partitionCount: number;
  underReplicatedPartitions: number;
  offlinePartitions: number;
}

export interface ConfigEntry {
  name: string;
  value: string | null;
  source: string;
  isDefault: boolean;
  isReadOnly: boolean;
  isSensitive: boolean;
}

export interface TopicSummary {
  name: string;
  internal: boolean;
  partitionCount: number;
  replicationFactor: number;
  underReplicated: number;
}

export interface TopicStats {
  name: string;
  /** null — для части партиций оффсеты недоступны */
  messageCount: number | null;
}

export interface PartitionDetails {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  low: number | null;
  high: number | null;
  messageCount: number | null;
  offsetsError?: LocalizedText;
  underReplicated: boolean;
}

export interface TopicDetails {
  name: string;
  internal: boolean;
  replicationFactor: number;
  /** null — у какой-либо партиции оффсеты недоступны */
  messageCount: number | null;
  partitions: PartitionDetails[];
}

export type DataFormat = "auto" | "string" | "json" | "hex" | "base64";

export interface DecodedData {
  format: DataFormat;
  text: string;
  size: number;
  truncated: boolean;
  error?: LocalizedText;
}

export interface HeaderView {
  name: string;
  value: string | null;
}

export interface BrowsedMessage {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number | null;
  key: DecodedData | null;
  value: DecodedData | null;
  headers: HeaderView[];
  size: number;
}

export type StartPosition =
  | { type: "earliest" }
  | { type: "latest"; count: number }
  | { type: "offset"; offset: number }
  | { type: "timestamp"; timestampMs: number }
  | { type: "lastMinutes"; minutes: number };

export interface TextMatch {
  pattern: string;
  regex: boolean;
  caseSensitive: boolean;
}

export interface FilterSpec {
  key?: TextMatch;
  value?: TextMatch;
  header?: { name: string; value?: TextMatch };
  jsonPath?: string;
  fromTimestamp?: number;
  toTimestamp?: number;
}

export interface BrowseParams {
  connectionId: string;
  topic: string;
  partitions: number[] | null;
  start: StartPosition;
  limit: number;
  live: boolean;
  keyFormat: DataFormat;
  valueFormat: DataFormat;
  filter: FilterSpec;
}

export type FinishReason = "limit" | "endOfTopic" | "cancelled";

export type BrowseEvent =
  | { type: "messages"; messages: BrowsedMessage[] }
  | { type: "progress"; scanned: number; matched: number }
  | { type: "reachedEnd" }
  | { type: "reconnecting"; message: LocalizedText }
  | { type: "recovered" }
  | { type: "finished"; scanned: number; matched: number; reason: FinishReason }
  | { type: "error"; error: AppError };

export interface Confirmation {
  /** Имя подключения — для prod-подключений. */
  confirm: string | null;
  /** Введённое имя объекта (топика, группы) или число топиков при групповом удалении. */
  typedName: string | null;
}

export interface NewTopicSpec {
  name: string;
  partitions: number;
  /** -1 — значение брокера по умолчанию */
  replicationFactor: number;
  configs: Record<string, string>;
}

export type ConfigChange = { op: "set"; name: string; value: string } | { op: "delete"; name: string };

export interface PurgedPartition {
  partition: number;
  lowWatermark: number | null;
  error?: LocalizedText;
}

export interface TopicDeleteResult {
  name: string;
  error?: AppError;
}

export interface TopicPolicy {
  name: string;
  cleanupPolicy: string | null;
}

export interface TopicPartitionRef {
  topic: string;
  partition: number;
}

export interface GroupSummary {
  groupId: string;
  state: string;
  isSimple: boolean;
}

export interface GroupList {
  groups: GroupSummary[];
  errors: LocalizedText[];
}

export interface GroupMember {
  memberId: string;
  clientId: string;
  host: string;
  assignment: TopicPartitionRef[];
}

export interface GroupDescription {
  groupId: string;
  state: string;
  assignor: string;
  members: GroupMember[];
  error?: string;
}

export interface GroupPartitionOffset {
  topic: string;
  partition: number;
  committed: number | null;
  logEnd: number | null;
  lag: number | null;
  memberClientId: string | null;
  error?: LocalizedText;
}

export interface GroupLag {
  groupId: string;
  totalLag: number | null;
  topics: string[];
  error?: LocalizedText;
}

export interface TopicGroupLag {
  groupId: string;
  state: string;
  lag: number | null;
  partitions: number;
}

export interface TopicGroups {
  groups: TopicGroupLag[];
  incomplete: boolean;
}

export type ResetStrategy =
  | { type: "earliest" }
  | { type: "latest" }
  | { type: "timestamp"; timestampMs: number }
  | { type: "offset"; offset: number }
  | { type: "shift"; delta: number };

export type ResetScope =
  | { type: "all" }
  | { type: "topics"; topics: string[] }
  | { type: "partitions"; partitions: TopicPartitionRef[] };

export interface ResetRequest {
  groupId: string;
  strategy: ResetStrategy;
  scope: ResetScope;
}

export interface ResetPreviewRow {
  topic: string;
  partition: number;
  current: number | null;
  target: number | null;
  low: number | null;
  high: number | null;
  error?: LocalizedText;
}

export interface OffsetTarget {
  topic: string;
  partition: number;
  offset: number;
}

export interface ResetResult {
  topic: string;
  partition: number;
  offset: number;
  error?: LocalizedText;
}

export type PayloadFormat = "string" | "json" | "hex" | "base64" | "null";

export interface PayloadInput {
  format: PayloadFormat;
  text: string;
}

export interface HeaderInput {
  name: string;
  value: string;
  format: "string" | "hex";
}

export interface ProduceMessage {
  topic: string;
  /** null — партицию выбирает партиционер */
  partition: number | null;
  key: PayloadInput;
  value: PayloadInput;
  headers: HeaderInput[];
}

export interface ProducerSettings {
  acks: "0" | "1" | "all";
  compression: "none" | "gzip" | "snappy" | "lz4" | "zstd";
  idempotence: boolean;
}

export interface ProduceResult {
  partition: number;
  /** null — брокер/librdkafka не сообщил offset (например, acks=0) */
  offset: number | null;
  timestamp: number;
}

export interface HistoryEntry {
  id: string;
  sentAt: number;
  message: ProduceMessage;
  settings: ProducerSettings;
  result: ProduceResult;
  /** Значения усечены до 64 КБ — повторить нельзя */
  truncated: boolean;
}

export interface Template {
  /** Пустая строка при создании — id назначает бэкенд */
  id: string;
  name: string;
  /** null — глобальный шаблон */
  connectionId: string | null;
  message: ProduceMessage;
  settings: ProducerSettings;
}

export type BatchSource =
  | { type: "copies"; count: number; message: ProduceMessage }
  | { type: "file"; path: string; format: "jsonl" | "csv"; topic: string };

export interface BatchParams {
  connectionId: string;
  source: BatchSource;
  settings: ProducerSettings;
  ratePerSec: number | null;
}

export interface BatchError {
  /** null — сводная ошибка, не относящаяся к строке */
  line: number | null;
  message: LocalizedText;
}

export type BatchEvent =
  | { type: "progress"; sent: number; failed: number; total: number | null }
  | { type: "errors"; errors: BatchError[] }
  | { type: "finished"; sent: number; failed: number; notSent: number; cancelled: boolean }
  | { type: "error"; error: AppError };

export interface ConsumerParams {
  connectionId: string;
  groupId: string;
  topics: string[];
  topicPattern: string | null;
  autoOffsetReset: "earliest" | "latest";
  enableAutoCommit: boolean;
  isolationLevel: "readCommitted" | "readUncommitted";
  keyFormat: DataFormat;
  valueFormat: DataFormat;
  filter: FilterSpec;
}

export type RebalanceRecord =
  | { kind: "assigned"; partitions: TopicPartitionRef[]; at: number }
  | { kind: "revoked"; partitions: TopicPartitionRef[]; at: number }
  | { kind: "error"; message: string; at: number };

export type CommitTarget =
  | { type: "selected"; topic: string; partition: number; offset: number }
  | { type: "allConsumed" };

export type ConsumeEvent =
  | { type: "messages"; messages: BrowsedMessage[] }
  | { type: "progress"; consumed: number; matched: number }
  | { type: "rebalance"; record: RebalanceRecord }
  | { type: "committed"; offsets: OffsetTarget[] }
  | { type: "paused"; partitions: TopicPartitionRef[] }
  | { type: "resumed"; partitions: TopicPartitionRef[] }
  | { type: "reconnecting"; message: LocalizedText }
  | { type: "recovered" }
  | { type: "finished"; consumed: number; matched: number }
  | { type: "error"; error: AppError };

export type Tier = "free" | "pro";
export type LicenseState = "none" | "valid" | "expiring" | "expired" | "invalid";
export interface LicenseStatus {
  tier: Tier;
  state: LicenseState;
  source: "policy" | "user" | null;
  licensee: string | null;
  kind: "personal" | "organization" | null;
  expires: string | null;
  daysLeft: number | null;
  /** Почему ключ недействителен — полная фраза. */
  error: LocalizedText | null;
}
export interface PolicyStatus {
  managed: boolean;
  source: string | null;
  updatesEnabled: boolean;
  updateUrl: string | null;
  hasLicenseKey: boolean;
  errors: LocalizedText[];
}

export interface AppInfo {
  name: string;
  version: string;
  commit: string;
  os: string;
  arch: string;
  dataDir: string;
  logDir: string;
}

export type UpdateCheck =
  | { status: "disabled"; reason: LocalizedText }
  | { status: "upToDate" }
  | { status: "available"; version: string; notes: string | null; date: string | null };
