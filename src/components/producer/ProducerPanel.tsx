import { useEffect, useId, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { PayloadFormat, PayloadInput, ProduceMessage, ProducerSettings } from "../../api/types";
import { formatTimestamp } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import type { ProducerPrefill } from "../../state/workspace";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { emptyProducerForm, formToMessage, messageToForm, type ProducerForm } from "./form";
import { HeadersEditor } from "./HeadersEditor";
import { BatchPanel } from "./BatchPanel";
import { HistoryList } from "./HistoryList";
import { TemplatesBar } from "./TemplatesBar";

const FORMATS: PayloadFormat[] = ["string", "json", "hex", "base64", "null"];

function PayloadField({ label, value, onChange }: { label: string; value: PayloadInput; onChange: (v: PayloadInput) => void }) {
  const id = useId();
  return (
    <div className="payload-field">
      <div className="toolbar">
        <label htmlFor={`${id}-text`}>{label}</label>
        <label htmlFor={`${id}-format`} className="muted">
          Формат {label === "Ключ" ? "ключа" : "значения"}
        </label>
        <select id={`${id}-format`} value={value.format} onChange={(e) => onChange({ ...value, format: e.target.value as PayloadFormat })}>
          {FORMATS.map((f) => (
            <option key={f} value={f}>
              {f}
            </option>
          ))}
        </select>
      </div>
      <textarea
        id={`${id}-text`}
        rows={label === "Значение" ? 8 : 2}
        disabled={value.format === "null"}
        value={value.text}
        onChange={(e) => onChange({ ...value, text: e.target.value })}
      />
    </div>
  );
}

export function ProducerPanel({ connectionId, prefill }: { connectionId: string; prefill?: ProducerPrefill }) {
  const connection = useConnection(connectionId);
  const queryClient = useQueryClient();
  const uid = useId();
  const [form, setForm] = useState<ProducerForm>(() =>
    prefill ? messageToForm(prefill.message, prefill.settings) : emptyProducerForm(),
  );
  const [formError, setFormError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  const topics = useQuery({ queryKey: ["topics", connectionId], queryFn: () => api.listTopics(connectionId) });
  const send = useMutation({
    mutationFn: ({ message, confirm }: { message: ProduceMessage; confirm: string | null }) =>
      api.produce(connectionId, message, form.settings, confirm),
    onSuccess: () => {
      setConfirming(false);
      void queryClient.invalidateQueries({ queryKey: ["history", connectionId] });
    },
  });
  // Новый prefill («Отправить повторно», «Отправить сообщение» из топика) заменяет форму.
  useEffect(() => {
    if (prefill) {
      setForm(messageToForm(prefill.message, prefill.settings));
      send.reset();
      setFormError(null);
      setConfirming(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [prefill]);
  // Правка формы делает прошлый результат неактуальным.
  const edited = () => {
    if (send.data || send.error) send.reset();
  };
  // Шаблон или запись истории заменяют форму целиком.
  const replaceForm = (f: ProducerForm) => {
    setForm(f);
    send.reset();
    setFormError(null);
    setConfirming(false);
  };
  const set = (patch: Partial<ProducerForm>) => {
    edited();
    setForm((f) => ({ ...f, ...patch }));
  };
  const setSettings = (patch: Partial<ProducerSettings>) => {
    edited();
    setForm((f) => ({ ...f, settings: { ...f.settings, ...patch } }));
  };

  const requestSend = () => {
    if (!connection) return;
    if (!needsConfirmation(connection)) return submit(null);
    try {
      formToMessage(form);
      setFormError(null);
      setConfirming(true);
    } catch (e) {
      setFormError((e as Error).message);
    }
  };

  const submit = (confirm: string | null) => {
    try {
      const message = formToMessage(form);
      setFormError(null);
      send.mutate({ message, confirm });
    } catch (e) {
      setFormError((e as Error).message);
    }
  };

  if (!connection) return <p className="empty">Загрузка…</p>;
  const readOnly = connection.readOnly;

  return (
    <div className="page producer">
      <div className="toolbar">
        <h2>Producer</h2>
        <button type="button" onClick={() => replaceForm(emptyProducerForm(form.topic))}>
          Очистить форму
        </button>
      </div>
      <TemplatesBar connectionId={connectionId} form={form} onLoad={replaceForm} />
      {readOnly && <p className="warn" role="alert">Подключение открыто только для чтения — отправка недоступна.</p>}
      <div className="form">
        <label htmlFor={`${uid}-topic`}>Топик</label>
        <input id={`${uid}-topic`} list={`${uid}-topics`} value={form.topic} onChange={(e) => set({ topic: e.target.value })} />
        <datalist id={`${uid}-topics`}>
          {topics.data?.filter((t) => !t.internal).map((t) => <option key={t.name} value={t.name} />)}
        </datalist>
        <label htmlFor={`${uid}-partition`}>Партиция</label>
        <input id={`${uid}-partition`} placeholder="авто" value={form.partition} onChange={(e) => set({ partition: e.target.value })} />
      </div>
      <PayloadField label="Ключ" value={form.key} onChange={(key) => set({ key })} />
      <PayloadField label="Значение" value={form.value} onChange={(value) => set({ value })} />
      <HeadersEditor headers={form.headers} onChange={(headers) => set({ headers })} />
      <p className="muted">
        Плейсхолдеры: {"{{uuid}} {{now}} {{nowMillis}} {{seq}} {{randomInt 1 100}} {{randomString 8}} {{env NAME}}"}
      </p>
      <fieldset className="toolbar">
        <legend>Параметры продюсера</legend>
        <label>
          acks{" "}
          <select value={form.settings.acks} onChange={(e) => setSettings({ acks: e.target.value as ProducerSettings["acks"] })}>
            <option value="0">0</option>
            <option value="1">1</option>
            <option value="all">all</option>
          </select>
        </label>
        <label>
          сжатие{" "}
          <select value={form.settings.compression} onChange={(e) => setSettings({ compression: e.target.value as ProducerSettings["compression"] })}>
            {["none", "gzip", "snappy", "lz4", "zstd"].map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </label>
        <label>
          <input type="checkbox" checked={form.settings.idempotence} onChange={(e) => setSettings({ idempotence: e.target.checked })} /> идемпотентность
        </label>
      </fieldset>
      {formError && <p className="warn">{formError}</p>}
      {send.error && <ErrorBanner error={send.error} />}
      {send.data && !send.isPending && (
        <p role="status">
          Отправлено: партиция {send.data.partition}, offset{" "}
          {send.data.offset === null ? (
            <span title="Брокер не вернул offset (например, при acks=0)">—</span>
          ) : (
            send.data.offset
          )}
          , {formatTimestamp(send.data.timestamp)}
        </p>
      )}
      {!readOnly &&
        (confirming ? (
          <ConfirmDialog
            connection={connection}
            title={`Отправить сообщение в «${form.topic.trim()}»?`}
            confirmLabel="Отправить в prod"
            busy={send.isPending}
            onConfirm={(c) => submit(c.confirm)}
            onCancel={() => setConfirming(false)}
          />
        ) : (
          <div className="toolbar">
            <button
              type="button"
              className="primary"
              disabled={send.isPending}
              onClick={requestSend}
            >
              Отправить
            </button>
          </div>
        ))}
      {!readOnly && <BatchPanel connection={connection} form={form} />}
      <h3>История (последние 100)</h3>
      <HistoryList connectionId={connectionId} onRepeat={replaceForm} />
    </div>
  );
}
