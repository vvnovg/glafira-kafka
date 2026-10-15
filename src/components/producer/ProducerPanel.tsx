import { useEffect, useState } from "react";
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

const FORMATS: PayloadFormat[] = ["string", "json", "hex", "base64", "null"];

function PayloadField({ label, value, onChange }: { label: string; value: PayloadInput; onChange: (v: PayloadInput) => void }) {
  return (
    <div className="payload-field">
      <div className="toolbar">
        <label htmlFor={`pf-${label}`}>{label}</label>
        <select aria-label={`Формат: ${label}`} value={value.format} onChange={(e) => onChange({ ...value, format: e.target.value as PayloadFormat })}>
          {FORMATS.map((f) => (
            <option key={f} value={f}>
              {f}
            </option>
          ))}
        </select>
      </div>
      <textarea
        id={`pf-${label}`}
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
  const [form, setForm] = useState<ProducerForm>(() =>
    prefill ? messageToForm(prefill.message, prefill.settings) : emptyProducerForm(),
  );
  const [formError, setFormError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  // Новый prefill («Отправить повторно», «Отправить сообщение» из топика) заменяет форму.
  useEffect(() => {
    if (prefill) setForm(messageToForm(prefill.message, prefill.settings));
  }, [prefill]);
  const topics = useQuery({ queryKey: ["topics", connectionId], queryFn: () => api.listTopics(connectionId) });
  const send = useMutation({
    mutationFn: ({ message, confirm }: { message: ProduceMessage; confirm: string | null }) =>
      api.produce(connectionId, message, form.settings, confirm),
    onSuccess: () => {
      setConfirming(false);
      void queryClient.invalidateQueries({ queryKey: ["history", connectionId] });
    },
  });
  const set = (patch: Partial<ProducerForm>) => setForm((f) => ({ ...f, ...patch }));
  const setSettings = (patch: Partial<ProducerSettings>) => setForm((f) => ({ ...f, settings: { ...f.settings, ...patch } }));

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
      </div>
      {readOnly && <p className="warn" role="alert">Подключение открыто только для чтения — отправка недоступна.</p>}
      <div className="form">
        <label htmlFor="pr-topic">Топик</label>
        <input id="pr-topic" list={`topics-${connectionId}`} value={form.topic} onChange={(e) => set({ topic: e.target.value })} />
        <datalist id={`topics-${connectionId}`}>
          {topics.data?.filter((t) => !t.internal).map((t) => <option key={t.name} value={t.name} />)}
        </datalist>
        <label htmlFor="pr-partition">Партиция</label>
        <input id="pr-partition" placeholder="авто" value={form.partition} onChange={(e) => set({ partition: e.target.value })} />
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
              onClick={() => (needsConfirmation(connection) ? setConfirming(true) : submit(null))}
            >
              Отправить
            </button>
          </div>
        ))}
    </div>
  );
}
