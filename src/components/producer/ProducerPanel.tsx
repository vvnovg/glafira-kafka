import { Trans, useTranslation } from "react-i18next";
import { useEffect, useId, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { PayloadFormat, PayloadInput, ProduceMessage, ProducerSettings } from "../../api/types";
import { formatTimestamp } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import type { ProducerPrefill } from "../../state/workspace";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { emptyProducerForm, formToMessage, messageToForm, type ProducerForm } from "./form";
import { HeadersEditor } from "./HeadersEditor";
import { BatchPanel } from "./BatchPanel";
import { HistoryList } from "./HistoryList";
import { TemplatesBar } from "./TemplatesBar";

const FORMATS: PayloadFormat[] = ["string", "json", "hex", "base64", "null"];

function PayloadField({ kind, value, onChange }: { kind: "key" | "value"; value: PayloadInput; onChange: (v: PayloadInput) => void }) {
  const { t } = useTranslation();
  const id = useId();
  const label = t(kind === "key" ? "producer.key" : "producer.value");
  const formatLabel = t(kind === "key" ? "producer.keyFormat" : "producer.valueFormat");
  return (
    <div className="payload-field">
      <div className="toolbar">
        <label htmlFor={`${id}-text`}>{label}</label>
        <label htmlFor={`${id}-format`} className="muted">
          {formatLabel}
        </label>
        <Hint k="hints.producer.formats" label={formatLabel} />
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
        rows={kind === "value" ? 8 : 2}
        disabled={value.format === "null"}
        value={value.text}
        onChange={(e) => onChange({ ...value, text: e.target.value })}
      />
    </div>
  );
}

export function ProducerPanel({ connectionId, prefill }: { connectionId: string; prefill?: ProducerPrefill }) {
  const { t } = useTranslation();
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
  // Форма заменена не шаблоном — выбор шаблона в TemplatesBar снимается.
  const [templateReset, setTemplateReset] = useState(0);
  const replaceFormDetached = (f: ProducerForm) => {
    replaceForm(f);
    setTemplateReset((n) => n + 1);
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

  if (!connection) return <p className="empty">{t("common.loading")}</p>;
  const readOnly = connection.readOnly;

  return (
    <div className="page producer">
      <div className="toolbar">
        <h2>Producer</h2>
        <button type="button" onClick={() => replaceFormDetached(emptyProducerForm(form.topic))}>
          {t("producer.clearForm")}
        </button>
      </div>
      <TemplatesBar connectionId={connectionId} form={form} onLoad={replaceForm} resetKey={templateReset} />
      {readOnly && <p className="warn" role="alert">{t("producer.readOnly")}</p>}
      <div className="form">
        <label htmlFor={`${uid}-topic`}>{t("producer.topic")}</label>
        <input id={`${uid}-topic`} list={`${uid}-topics`} value={form.topic} onChange={(e) => set({ topic: e.target.value })} />
        <datalist id={`${uid}-topics`}>
          {topics.data?.filter((x) => !x.internal).map((x) => <option key={x.name} value={x.name} />)}
        </datalist>
        <span className="field-label">
          <label htmlFor={`${uid}-partition`}>{t("producer.partition")}</label>
          <Hint k="hints.producer.partition" label={t("producer.partition")} />
        </span>
        <input id={`${uid}-partition`} placeholder={t("producer.partitionAuto")} value={form.partition} onChange={(e) => set({ partition: e.target.value })} />
      </div>
      <PayloadField kind="key" value={form.key} onChange={(key) => set({ key })} />
      <PayloadField kind="value" value={form.value} onChange={(value) => set({ value })} />
      <HeadersEditor headers={form.headers} onChange={(headers) => set({ headers })} />
      <p className="muted">
        {t("producer.placeholders", { list: "{{uuid}} {{now}} {{nowMillis}} {{seq}} {{randomInt 1 100}} {{randomString 8}} {{env NAME}}" })}{" "}
        <Hint k="hints.producer.placeholders" label={t("producer.settings.legend")} />
      </p>
      <fieldset className="toolbar">
        <legend>{t("producer.settings.legend")}</legend>
        <label>
          acks{" "}
          <select value={form.settings.acks} onChange={(e) => setSettings({ acks: e.target.value as ProducerSettings["acks"] })}>
            <option value="0">0</option>
            <option value="1">1</option>
            <option value="all">all</option>
          </select>
        </label>
        <Hint k="hints.producer.acks" label="acks" />
        <label>
          {t("producer.settings.compression")}{" "}
          <select value={form.settings.compression} onChange={(e) => setSettings({ compression: e.target.value as ProducerSettings["compression"] })}>
            {["none", "gzip", "snappy", "lz4", "zstd"].map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </label>
        <Hint k="hints.producer.compression" label={t("producer.settings.compression")} />
        <label>
          <input type="checkbox" checked={form.settings.idempotence} onChange={(e) => setSettings({ idempotence: e.target.checked })} /> {t("producer.settings.idempotence")}
        </label>
        <Hint k="hints.producer.idempotence" label={t("producer.settings.idempotence")} />
      </fieldset>
      {formError && <p className="warn">{formError}</p>}
      {send.error && <ErrorBanner error={send.error} />}
      {send.data && !send.isPending && (
        <p role="status">
          <Trans
            i18nKey="producer.sent"
            values={{ partition: send.data.partition, time: formatTimestamp(send.data.timestamp) }}
            components={{
              offset:
                send.data.offset === null ? <span title={t("producer.noOffsetHint")}>—</span> : <>{send.data.offset}</>,
            }}
          />
        </p>
      )}
      {!readOnly &&
        (confirming ? (
          <ConfirmDialog
            connection={connection}
            title={t("producer.confirmTitle", { topic: form.topic.trim() })}
            confirmLabel={t("producer.confirmLabel")}
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
              {t("producer.send")}
            </button>
          </div>
        ))}
      {!readOnly && <BatchPanel connection={connection} form={form} />}
      <h3>{t("producer.history.title")}</h3>
      <HistoryList connectionId={connectionId} onRepeat={replaceFormDetached} />
    </div>
  );
}
