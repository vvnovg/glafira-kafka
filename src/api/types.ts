export type ErrorCode =
  | "CONNECTION_FAILED"
  | "TLS_ERROR"
  | "AUTH_FAILED"
  | "NOT_AUTHORIZED"
  | "TIMEOUT"
  | "INVALID_INPUT"
  | "NOT_FOUND"
  | "INTERNAL";

export interface AppError {
  code: ErrorCode;
  message: string;
  details?: string;
  retryable: boolean;
}

export type Environment = "local" | "dev" | "test" | "stage" | "prod";
export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";

export interface TlsConfig {
  caLocation: string | null;
  verifyHostname: boolean;
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
  message: string;
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
  offsetsError?: string;
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
  error?: string;
}

export interface HeaderView {
  name: string;
  value: string | null;
}

export interface BrowsedMessage {
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
  | { type: "reconnecting"; message: string }
  | { type: "recovered" }
  | { type: "finished"; scanned: number; matched: number; reason: FinishReason }
  | { type: "error"; error: AppError };
