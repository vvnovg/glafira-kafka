import { useEffect, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { BrowsedMessage, CommitTarget, ConsumerParams, DataFormat, OffsetTarget, TopicPartitionRef } from "../../api/types";
import { tr } from "../../i18n/text";
import { formatCount } from "../../lib/format";
import { useLicense } from "../../lib/useLicense";
import { useConnection } from "../../lib/useConnection";
import { useWorkspace } from "../../state/workspace";
import { FilterFields } from "../browse/FilterFields";
import { MessageDetails } from "../browse/MessageDetails";
import { messageKey, MessageTable } from "../browse/MessageTable";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { AssignmentPanel } from "./AssignmentPanel";
import { buildConsumerParams, type ConsumerFormState, defaultConsumerForm } from "./form";
import { partitionKey } from "./state";
import { useConsumerSession } from "./useConsumerSession";

const FORMATS: DataFormat[] = ["auto", "string", "json", "hex", "base64"];

// Параметры запуска проверяются до подтверждения: диалог открывается только для корректной формы.
type Pending = { kind: "start"; params: ConsumerParams } | { kind: "commit"; target: CommitTarget } | null;

const formatOffsets = (offsets: OffsetTarget[]) => offsets.map((o) => `${partitionKey(o)} → ${o.offset}`).join(", ");

export function ConsumerPanel({ connectionId, topic, groupId }: { connectionId: string; topic?: string; groupId?: string }) {
  const { t } = useTranslation();
  const connection = useConnection(connectionId);
  const [form, setForm] = useState<ConsumerFormState>(() => defaultConsumerForm({ topic, groupId }));
  const [formError, setFormError] = useState<string | null>(null);
  const [pending, setPending] = useState<Pending>(null);
  const [selected, setSelected] = useState<BrowsedMessage | null>(null);
  const [utc, setUtc] = useState(false);
  const [suggesting, setSuggesting] = useState(!groupId);
  // Параметры текущей сессии — для пояснения ошибок (например, несуществующего топика).
  const [launched, setLaunched] = useState<ConsumerParams | null>(null);
  // lastCommit на момент последнего запроса коммита: событие committed после него — результат этого запроса.
  const [commitBase, setCommitBase] = useState<OffsetTarget[] | null | undefined>(undefined);
  const { state, start, stop } = useConsumerSession(form.limit);
  const running = state.status === "running";
  const { locked } = useLicense();
  const openTab = useWorkspace((s) => s.open);
  const set = (patch: Partial<ConsumerFormState>) => setForm((f) => ({ ...f, ...patch }));

  // Сгенерированный group.id, если группа не задана.
  useEffect(() => {
    if (groupId) return;
    let alive = true;
    void api
      .suggestGroupId()
      .then((id) => {
        if (alive) setForm((f) => (f.groupId ? f : { ...f, groupId: id }));
      })
      .catch(() => {})
      .finally(() => {
        if (alive) setSuggesting(false);
      });
    return () => {
      alive = false;
    };
  }, [groupId]);

  const commit = useMutation({
    mutationFn: ({ target, confirm }: { target: CommitTarget; confirm: string | null }) =>
      api.commitOffsets(state.sessionId as string, target, confirm),
    onSettled: () => setPending(null),
  });
  const control = useMutation({
    mutationFn: ({ pause, p }: { pause: boolean; p: TopicPartitionRef }) =>
      pause ? api.pausePartitions(state.sessionId as string, [p]) : api.resumePartitions(state.sessionId as string, [p]),
  });

  // Подключение переключили в «только чтение» во время сессии — выходим из группы.
  // Бэкенд при сохранении профиля тоже отменяет сессии подключения (cancel_for).
  const readOnly = connection?.readOnly === true;
  useEffect(() => {
    if (readOnly && running) void stop();
  }, [readOnly, running, stop]);

  if (!connection) return <p className="empty">{t("common.loading")}</p>;
  if (locked && !running) {
    return (
      <div className="page">
        <p className="warn">{t("consumer.proOnly")}</p>
        <button type="button" className="link" onClick={() => openTab({ kind: "license" })}>
          {t("common.openLicense")}
        </button>
      </div>
    );
  }
  if (connection.readOnly) {
    return (
      <div className="page">
        <p className="warn">{t("consumer.readOnlyBlocked")}</p>
        {running && (
          <button type="button" onClick={() => void stop()}>
            {t("consumer.disconnect")}
          </button>
        )}
      </div>
    );
  }

  const launch = (params: ConsumerParams, confirm: string | null) => {
    setPending(null);
    setSelected(null);
    setLaunched(params);
    setCommitBase(undefined);
    commit.reset();
    control.reset();
    void start(params, confirm);
  };
  const requestStart = () => {
    let params: ConsumerParams;
    try {
      params = buildConsumerParams(form, connectionId);
    } catch (e) {
      setFormError((e as Error).message);
      return;
    }
    setFormError(null);
    if (needsConfirmation(connection)) setPending({ kind: "start", params });
    else launch(params, null);
  };
  const doCommit = (target: CommitTarget, confirm: string | null) => {
    setCommitBase(state.lastCommit);
    commit.mutate({ target, confirm });
  };
  const requestCommit = (target: CommitTarget) =>
    needsConfirmation(connection) ? setPending({ kind: "commit", target }) : doCommit(target, null);

  // Диалог коммита теряет смысл, если сессия завершилась.
  const dialog = pending && (pending.kind === "start" || running) ? pending : null;
  // Ответ команды или (после таймаута) событие committed, пришедшее после запроса.
  const committed = commit.data ?? (commitBase !== undefined && state.lastCommit !== commitBase ? state.lastCommit : null);
  const commitError = commit.error ? toAppError(commit.error) : null;
  const sessionError = state.error;
  const missingTopic = sessionError?.code === "NOT_FOUND" && launched !== null;

  return (
    <div className="page browse">
      <p className="warn">
        {t("consumer.warn")} <Hint k="hints.consumer.join" label={t("consumer.form.groupTopics")} />
      </p>
      <div className="browse-form">
        <fieldset disabled={running}>
          <legend>{t("consumer.form.groupTopics")}</legend>
          <label className="field">
            <span>group.id</span>
            <input
              aria-label="group.id"
              placeholder={suggesting ? "…" : "my-group"}
              title={suggesting ? t("consumer.form.suggestingTitle") : undefined}
              value={form.groupId}
              onChange={(e) => set({ groupId: e.target.value })}
            />
          </label>
          <label>
            <input type="checkbox" checked={form.useRegex} onChange={(e) => set({ useRegex: e.target.checked })} /> {t("consumer.form.useRegex")}
          </label>
          {form.useRegex ? (
            <label className="field">
              <span>{t("consumer.form.patternLabel")}</span>
              <input
                aria-label={t("consumer.form.patternLabel")}
                placeholder="orders\..*"
                value={form.pattern}
                onChange={(e) => set({ pattern: e.target.value })}
              />
            </label>
          ) : (
            <label className="field">
              <span>{t("consumer.form.topicsLabel")}</span>
              <input aria-label={t("consumer.form.topicsLabel")} placeholder="topic-a, topic-b" value={form.topics} onChange={(e) => set({ topics: e.target.value })} />
            </label>
          )}
          <label className="field">
            <span>auto.offset.reset</span>
            <select
              aria-label="auto.offset.reset"
              value={form.autoOffsetReset}
              onChange={(e) => set({ autoOffsetReset: e.target.value as ConsumerFormState["autoOffsetReset"] })}
            >
              <option value="latest">{t("consumer.form.resetLatest")}</option>
              <option value="earliest">{t("consumer.form.resetEarliest")}</option>
            </select>
          </label>
          <label>
            <input type="checkbox" checked={form.enableAutoCommit} onChange={(e) => set({ enableAutoCommit: e.target.checked })} /> {t("consumer.form.autoCommit")}
          </label>
          <Hint k="hints.consumer.autoCommit" label={t("consumer.form.autoCommit")} />
          <label className="field">
            <span>isolation.level</span>
            <select
              aria-label="isolation.level"
              value={form.isolationLevel}
              onChange={(e) => set({ isolationLevel: e.target.value as ConsumerFormState["isolationLevel"] })}
            >
              <option value="readCommitted">read_committed</option>
              <option value="readUncommitted">read_uncommitted</option>
            </select>
          </label>
        </fieldset>
        <fieldset disabled={running}>
          <legend>{t("consumer.form.formatLegend")}</legend>
          <label className="field">
            <span>{t("consumer.form.keyFormat")}</span>
            <select aria-label={t("consumer.form.keyFormat")} value={form.keyFormat} onChange={(e) => set({ keyFormat: e.target.value as DataFormat })}>
              {FORMATS.map((f) => (
                <option key={f}>{f}</option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>{t("consumer.form.valueFormat")}</span>
            <select aria-label={t("consumer.form.valueFormat")} value={form.valueFormat} onChange={(e) => set({ valueFormat: e.target.value as DataFormat })}>
              {FORMATS.map((f) => (
                <option key={f}>{f}</option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>{t("consumer.form.buffer")}</span>
            <input
              aria-label={t("consumer.form.buffer")}
              type="number"
              min={1}
              max={100000}
              value={form.limit}
              onChange={(e) => set({ limit: Number(e.target.value) })}
              style={{ width: 90 }}
            />
          </label>
        </fieldset>
        <FilterFields value={form} onChange={setForm} disabled={running} />
      </div>
      {dialog ? (
        <ConfirmDialog
          connection={connection}
          title={dialog.kind === "start" ? t("consumer.dialog.startTitle", { group: dialog.params.groupId }) : t("consumer.dialog.commitTitle")}
          confirmLabel={t("consumer.dialog.confirm")}
          busy={commit.isPending}
          onConfirm={(c) => (dialog.kind === "start" ? launch(dialog.params, c.confirm) : doCommit(dialog.target, c.confirm))}
          onCancel={() => setPending(null)}
        />
      ) : (
        <div className="toolbar">
          <button type="button" className="primary" disabled={running} onClick={requestStart}>
            {t("consumer.actions.connect")}
          </button>
          <button type="button" disabled={!running} onClick={() => void stop()}>
            {t("consumer.disconnect")}
          </button>
          <button
            type="button"
            disabled={!running || !selected || !state.sessionId || commit.isPending}
            onClick={() => selected && requestCommit({ type: "selected", topic: selected.topic, partition: selected.partition, offset: selected.offset })}
          >
            {t("consumer.actions.commitSelected")}
          </button>
          <button type="button" disabled={!running || !state.sessionId || commit.isPending} onClick={() => requestCommit({ type: "allConsumed" })}>
            {t("consumer.actions.commitAll")}
          </button>
          <Hint k="hints.consumer.commit" label={t("consumer.actions.commitAll")} />
          <label>
            <input type="checkbox" checked={utc} onChange={(e) => setUtc(e.target.checked)} /> UTC
          </label>
          <span className="muted">
            {t("consumer.status.counters", { consumed: formatCount(state.consumed), matched: formatCount(state.matched), shown: state.messages.length })}
            {running && !state.sessionId && ` · ${t("consumer.status.connecting")}`}
            {running &&
              state.sessionId &&
              ` · ${state.reconnecting ? t("consumer.status.reconnecting", { reason: tr(state.reconnecting) }) : t("consumer.status.inGroup")}`}
            {state.status === "finished" && ` · ${t("consumer.status.disconnected")}`}
          </span>
        </div>
      )}
      {formError && <ErrorBanner error={{ code: "INVALID_INPUT", message: formError, retryable: false }} />}
      {sessionError && <ErrorBanner error={sessionError} />}
      {missingTopic && (
        <p className="warn">
          {launched.topics.length > 0
            ? t("consumer.missingTopic", { topics: launched.topics.join(", ") })
            : t("consumer.missingTopicOrGroup")}
        </p>
      )}
      {commitError && <ErrorBanner error={commitError} />}
      {commitError?.code === "TIMEOUT" && (
        <p className="warn">{t("consumer.commitTimeout")}</p>
      )}
      {control.error && <ErrorBanner error={control.error} />}
      {committed && <p role="status">{t("consumer.committed", { offsets: formatOffsets(committed) })}</p>}
      <div className="browse-body">
        <MessageTable messages={state.messages} selectedKey={selected ? messageKey(selected) : null} onSelect={setSelected} utc={utc} />
        <div className="details-pane">
          <AssignmentPanel
            assignment={state.assignment}
            paused={state.paused}
            log={state.log}
            disabled={!running || !state.sessionId || control.isPending}
            onPause={(p) => control.mutate({ pause: true, p })}
            onResume={(p) => control.mutate({ pause: false, p })}
          />
          <MessageDetails message={selected} utc={utc} connectionId={connectionId} />
        </div>
      </div>
    </div>
  );
}
