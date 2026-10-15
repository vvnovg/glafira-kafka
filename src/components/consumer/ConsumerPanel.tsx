import { useEffect, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { BrowsedMessage, CommitTarget, ConsumerParams, DataFormat, OffsetTarget, TopicPartitionRef } from "../../api/types";
import { formatCount } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import { FilterFields } from "../browse/FilterFields";
import { MessageDetails } from "../browse/MessageDetails";
import { messageKey, MessageTable } from "../browse/MessageTable";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { AssignmentPanel } from "./AssignmentPanel";
import { buildConsumerParams, type ConsumerFormState, defaultConsumerForm } from "./form";
import { partitionKey } from "./state";
import { useConsumerSession } from "./useConsumerSession";

const FORMATS: DataFormat[] = ["auto", "string", "json", "hex", "base64"];

// Параметры запуска проверяются до подтверждения: диалог открывается только для корректной формы.
type Pending = { kind: "start"; params: ConsumerParams } | { kind: "commit"; target: CommitTarget } | null;

const formatOffsets = (offsets: OffsetTarget[]) => offsets.map((o) => `${partitionKey(o)} → ${o.offset}`).join(", ");

export function ConsumerPanel({ connectionId, topic, groupId }: { connectionId: string; topic?: string; groupId?: string }) {
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

  if (!connection) return <p className="empty">Загрузка…</p>;
  if (connection.readOnly) {
    return (
      <div className="page">
        <p className="warn">Consumer-сессии недоступны для подключения только для чтения: вход в группу меняет её состояние.</p>
        {running && (
          <button type="button" onClick={() => void stop()}>
            Отключиться
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
        Сессия вступает в группу и влияет на кластер: у других участников группы произойдёт ребаланс, коммиты меняют оффсеты
        группы.
      </p>
      <div className="browse-form">
        <fieldset disabled={running}>
          <legend>Группа и топики</legend>
          <label className="field">
            <span>group.id</span>
            <input
              aria-label="group.id"
              placeholder={suggesting ? "…" : "my-group"}
              title={suggesting ? "Генерируется имя группы" : undefined}
              value={form.groupId}
              onChange={(e) => set({ groupId: e.target.value })}
            />
          </label>
          <label>
            <input type="checkbox" checked={form.useRegex} onChange={(e) => set({ useRegex: e.target.checked })} /> топики по regex
          </label>
          {form.useRegex ? (
            <label className="field">
              <span>Регулярное выражение топиков</span>
              <input
                aria-label="Регулярное выражение топиков"
                placeholder="orders\..*"
                value={form.pattern}
                onChange={(e) => set({ pattern: e.target.value })}
              />
            </label>
          ) : (
            <label className="field">
              <span>Топики</span>
              <input aria-label="Топики" placeholder="topic-a, topic-b" value={form.topics} onChange={(e) => set({ topics: e.target.value })} />
            </label>
          )}
          <label className="field">
            <span>auto.offset.reset</span>
            <select
              aria-label="auto.offset.reset"
              value={form.autoOffsetReset}
              onChange={(e) => set({ autoOffsetReset: e.target.value as ConsumerFormState["autoOffsetReset"] })}
            >
              <option value="latest">с конца (latest)</option>
              <option value="earliest">с начала (earliest)</option>
            </select>
          </label>
          <label>
            <input type="checkbox" checked={form.enableAutoCommit} onChange={(e) => set({ enableAutoCommit: e.target.checked })} /> автокоммит
          </label>
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
          <legend>Формат</legend>
          <label className="field">
            <span>Формат ключа</span>
            <select aria-label="Формат ключа" value={form.keyFormat} onChange={(e) => set({ keyFormat: e.target.value as DataFormat })}>
              {FORMATS.map((f) => (
                <option key={f}>{f}</option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Формат значения</span>
            <select aria-label="Формат значения" value={form.valueFormat} onChange={(e) => set({ valueFormat: e.target.value as DataFormat })}>
              {FORMATS.map((f) => (
                <option key={f}>{f}</option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Буфер, сообщений</span>
            <input
              aria-label="Буфер, сообщений"
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
          title={dialog.kind === "start" ? `Подключиться к группе «${dialog.params.groupId}»?` : "Закоммитить оффсеты?"}
          confirmLabel="Подтвердить"
          busy={commit.isPending}
          onConfirm={(c) => (dialog.kind === "start" ? launch(dialog.params, c.confirm) : doCommit(dialog.target, c.confirm))}
          onCancel={() => setPending(null)}
        />
      ) : (
        <div className="toolbar">
          <button type="button" className="primary" disabled={running} onClick={requestStart}>
            Подключиться к группе
          </button>
          <button type="button" disabled={!running} onClick={() => void stop()}>
            Отключиться
          </button>
          <button
            type="button"
            disabled={!running || !selected || !state.sessionId || commit.isPending}
            onClick={() => selected && requestCommit({ type: "selected", topic: selected.topic, partition: selected.partition, offset: selected.offset })}
          >
            Закоммитить до выбранного
          </button>
          <button type="button" disabled={!running || !state.sessionId || commit.isPending} onClick={() => requestCommit({ type: "allConsumed" })}>
            Закоммитить всё прочитанное
          </button>
          <label>
            <input type="checkbox" checked={utc} onChange={(e) => setUtc(e.target.checked)} /> UTC
          </label>
          <span className="muted">
            прочитано {formatCount(state.consumed)} · найдено {formatCount(state.matched)} · в таблице {state.messages.length}
            {running && !state.sessionId && " · подключение…"}
            {running && state.sessionId && (state.reconnecting ? ` · переподключение: ${state.reconnecting}…` : " · в группе")}
            {state.status === "finished" && " · отключено"}
          </span>
        </div>
      )}
      {formError && <ErrorBanner error={{ code: "INVALID_INPUT", message: formError, retryable: false }} />}
      {sessionError && <ErrorBanner error={sessionError} />}
      {missingTopic && (
        <p className="warn">
          {launched.topics.length > 0
            ? `Топик не найден в кластере. Проверьте имена: ${launched.topics.join(", ")}.`
            : "Топик или группа не найдены в кластере."}
        </p>
      )}
      {commitError && <ErrorBanner error={commitError} />}
      {commitError?.code === "TIMEOUT" && (
        <p className="warn">Неизвестно, применён ли коммит: сессия не ответила вовремя, но запрос мог выполниться. Проверьте оффсеты группы.</p>
      )}
      {control.error && <ErrorBanner error={control.error} />}
      {committed && <p role="status">Закоммичено: {formatOffsets(committed)}</p>}
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
