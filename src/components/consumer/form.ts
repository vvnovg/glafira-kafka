import { i18n } from "../../i18n";
import type { ConsumerParams, DataFormat } from "../../api/types";
import { buildFilterSpec, type FilterFields } from "../browse/params";

export interface ConsumerFormState extends FilterFields {
  groupId: string;
  /** Топики через запятую, пробел или перевод строки */
  topics: string;
  useRegex: boolean;
  pattern: string;
  autoOffsetReset: ConsumerParams["autoOffsetReset"];
  enableAutoCommit: boolean;
  isolationLevel: ConsumerParams["isolationLevel"];
  keyFormat: DataFormat;
  valueFormat: DataFormat;
  /** Размер буфера сообщений в UI */
  limit: number;
}

export function defaultConsumerForm(init: { topic?: string; groupId?: string } = {}): ConsumerFormState {
  return {
    groupId: init.groupId ?? "",
    topics: init.topic ?? "",
    useRegex: false,
    pattern: "",
    autoOffsetReset: "latest",
    enableAutoCommit: false,
    isolationLevel: "readCommitted",
    keyFormat: "auto",
    valueFormat: "auto",
    limit: 5000,
    keyPattern: "",
    valuePattern: "",
    regex: false,
    caseSensitive: false,
    headerName: "",
    headerValue: "",
    jsonPath: "",
    fromTime: "",
    toTime: "",
  };
}

const MAX_BUFFER = 100_000;

export function buildConsumerParams(f: ConsumerFormState, connectionId: string): ConsumerParams {
  if (!f.groupId.trim()) throw new Error(i18n.t("consumer.validation.groupId"));
  if (!Number.isInteger(f.limit) || f.limit < 1 || f.limit > MAX_BUFFER) throw new Error(i18n.t("consumer.validation.buffer", { max: MAX_BUFFER }));
  let topics: string[] = [];
  let topicPattern: string | null = null;
  if (f.useRegex) {
    if (!f.pattern.trim()) throw new Error(i18n.t("consumer.validation.pattern"));
    try {
      new RegExp(f.pattern.trim());
    } catch {
      throw new Error(i18n.t("consumer.validation.patternInvalid"));
    }
    topicPattern = f.pattern.trim();
  } else {
    topics = f.topics.split(/[\s,]+/).filter(Boolean);
    if (topics.length === 0) throw new Error(i18n.t("consumer.validation.topics"));
  }
  return {
    connectionId,
    groupId: f.groupId.trim(),
    topics,
    topicPattern,
    autoOffsetReset: f.autoOffsetReset,
    enableAutoCommit: f.enableAutoCommit,
    isolationLevel: f.isolationLevel,
    keyFormat: f.keyFormat,
    valueFormat: f.valueFormat,
    filter: buildFilterSpec(f),
  };
}
