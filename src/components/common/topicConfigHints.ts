/** Подсказки к параметрам уровня топика: имя параметра → ключ словаря. Параметр не из списка — без значка. */
export const TOPIC_CONFIG_HINTS: Record<string, string> = {
  "cleanup.policy": "hints.topicConfig.cleanup_policy",
  "retention.ms": "hints.topicConfig.retention_ms",
  "retention.bytes": "hints.topicConfig.retention_bytes",
  "delete.retention.ms": "hints.topicConfig.delete_retention_ms",
  "min.compaction.lag.ms": "hints.topicConfig.min_compaction_lag_ms",
  "max.compaction.lag.ms": "hints.topicConfig.max_compaction_lag_ms",
  "min.cleanable.dirty.ratio": "hints.topicConfig.min_cleanable_dirty_ratio",
  "segment.ms": "hints.topicConfig.segment_ms",
  "segment.bytes": "hints.topicConfig.segment_bytes",
  "segment.jitter.ms": "hints.topicConfig.segment_jitter_ms",
  "file.delete.delay.ms": "hints.topicConfig.file_delete_delay_ms",
  "local.retention.ms": "hints.topicConfig.local_retention_ms",
  "local.retention.bytes": "hints.topicConfig.local_retention_bytes",
  "remote.storage.enable": "hints.topicConfig.remote_storage_enable",
  "remote.log.copy.disable": "hints.topicConfig.remote_log_copy_disable",
  "remote.log.delete.on.disable": "hints.topicConfig.remote_log_delete_on_disable",
};
