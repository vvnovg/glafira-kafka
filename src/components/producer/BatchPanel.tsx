import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { api, toAppError } from "../../api/client";
import type { BatchSource, ConnectionProfile } from "../../api/types";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { tr } from "../../i18n/text";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { batchReducer, fileFormatOf, initialBatchState } from "./batch";
import { formToMessage, type ProducerForm } from "./form";

type Mode = "copies" | "file";

interface Run {
  stopped: boolean;
}

export function BatchPanel({ connection, form }: { connection: ConnectionProfile; form: ProducerForm }) {
  const { t } = useTranslation();
  const [mode, setMode] = useState<Mode>("copies");
  const [count, setCount] = useState("10");
  const [rate, setRate] = useState("");
  const [path, setPath] = useState<string | null>(null);
  const [formError, setFormError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  const [state, dispatch] = useReducer(batchReducer, initialBatchState);
  const sessionRef = useRef<string | null>(null);
  // Текущий запуск: события предыдущих запусков игнорируются.
  const runRef = useRef<Run | null>(null);
  const running = state.status === "running";

  // Останавливает сессию. Если startBatch ещё не вернул id, сессию остановит start().
  const stopInternal = useCallback(async () => {
    if (runRef.current) runRef.current.stopped = true;
    const id = sessionRef.current;
    sessionRef.current = null;
    if (id) await api.stopSession(id).catch(() => false);
  }, []);
  // «Отменить» всегда сбрасывает состояние: терминальное событие может не прийти (паника в Rust).
  // Поздний finished от бэкенда уточнит счётчики.
  const cancel = useCallback(async () => {
    dispatch({ type: "stopped" });
    await stopInternal();
  }, [stopInternal]);
  // Закрытие вкладки останавливает пакет.
  useEffect(
    () => () => {
      const run = runRef.current;
      runRef.current = null;
      if (run) run.stopped = true;
      const id = sessionRef.current;
      sessionRef.current = null;
      if (id) void api.stopSession(id).catch(() => false);
    },
    [],
  );

  const buildSource = (): BatchSource => {
    if (mode === "copies") {
      const n = Number(count);
      if (!Number.isInteger(n) || n < 1 || n > 1_000_000) throw new Error(t("producer.batch.errCount"));
      return { type: "copies", count: n, message: formToMessage(form) };
    }
    if (!path) throw new Error(t("producer.batch.errNoFile"));
    const format = fileFormatOf(path);
    if (!format) throw new Error(t("producer.batch.errFormat"));
    if (!form.topic.trim()) throw new Error(t("producer.batch.errTopic"));
    return { type: "file", path, format, topic: form.topic.trim() };
  };

  // Проверяет форму; при ошибке показывает её и возвращает null.
  const validate = (): { source: BatchSource; ratePerSec: number | null } | null => {
    try {
      const source = buildSource();
      const r = rate.trim() ? Number(rate) : null;
      if (r !== null && (!Number.isInteger(r) || r < 1)) throw new Error(t("producer.batch.errRate"));
      setFormError(null);
      return { source, ratePerSec: r };
    } catch (e) {
      setFormError((e as Error).message);
      return null;
    }
  };

  const request = () => {
    if (connection.readOnly) return;
    const v = validate();
    if (!v) return;
    if (needsConfirmation(connection)) setConfirming(true);
    else void start(v, null);
  };

  const start = async (v: { source: BatchSource; ratePerSec: number | null }, confirm: string | null) => {
    setConfirming(false);
    await stopInternal();
    const run: Run = { stopped: false };
    runRef.current = run;
    dispatch({ type: "start" });
    try {
      const id = await api.startBatch(
        { connectionId: connection.id, source: v.source, settings: form.settings, ratePerSec: v.ratePerSec },
        (event) => {
          if (runRef.current === run) dispatch({ type: "event", event });
        },
        confirm,
      );
      if (runRef.current !== run || run.stopped) {
        await api.stopSession(id).catch(() => false);
        return;
      }
      sessionRef.current = id;
    } catch (e) {
      if (runRef.current === run) dispatch({ type: "failed", error: toAppError(e) });
    }
  };

  const chooseFile = async () => {
    const selected = await openDialog({
      multiple: false,
      directory: false,
      filters: [{ name: "JSON Lines / CSV", extensions: ["jsonl", "ndjson", "csv"] }],
    });
    if (typeof selected === "string") setPath(selected);
  };

  const shown = state.errors.length;
  const modeName = `batch-mode-${connection.id}`;

  return (
    <fieldset className="panel">
      <legend>{t("producer.batch.legend")}</legend>
      {connection.readOnly ? (
        <p className="warn">{t("producer.batch.readOnly")}</p>
      ) : (
        <>
          <div className="toolbar">
            <label>
              <input type="radio" name={modeName} checked={mode === "copies"} onChange={() => setMode("copies")} /> {t("producer.batch.modeCopies")}
            </label>
            <label>
              <input type="radio" aria-label={t("producer.batch.modeFileAria")} name={modeName} checked={mode === "file"} onChange={() => setMode("file")} /> {t("producer.batch.modeFile")}
            </label>
            {mode === "copies" ? (
              <label>
                {t("producer.batch.count")}{" "}
                <input aria-label={t("producer.batch.count")} type="number" min={1} value={count} onChange={(e) => setCount(e.target.value)} style={{ width: 100 }} />
              </label>
            ) : (
              <>
                <button type="button" onClick={() => void chooseFile()}>
                  {t("producer.batch.chooseFile")}
                </button>
                {path && <code>{path}</code>}
              </>
            )}
            <label>
              {t("producer.batch.rate")}{" "}
              <input
                aria-label={t("producer.batch.rateAria")}
                type="number"
                min={1}
                placeholder={t("producer.batch.rateUnlimited")}
                value={rate}
                onChange={(e) => setRate(e.target.value)}
                style={{ width: 120 }}
              />
            </label>
            <Hint k="hints.producer.batchRate" label={t("producer.batch.rate")} />
          </div>
          <p className="muted">
            {mode === "copies"
              ? t("producer.batch.hintCopies", { seq: "{{seq}}" })
              : t("producer.batch.hintFile")}
          </p>
        </>
      )}
      {formError && <p className="warn">{formError}</p>}
      {state.error && <ErrorBanner error={state.error} />}
      {state.status !== "idle" && (
        <div role="status">
          <p>
            {state.total !== null ? (
              t("producer.batch.progress", { sent: state.sent, total: state.total, failed: state.failed })
            ) : (
              <Trans
                i18nKey="producer.batch.progressUnknown"
                values={{ sent: state.sent, failed: state.failed }}
                components={{ total: <span title={t("producer.batch.totalUnknownHint")}>—</span> }}
              />
            )}
          </p>
          {state.status === "finished" && <p>{state.cancelled ? t("producer.batch.cancelled") : t("producer.batch.done")}</p>}
          {state.status === "finished" && state.notSent > 0 && <p>{t("producer.batch.notSent", { count: state.notSent })}</p>}
        </div>
      )}
      {shown > 0 && (
        <>
          <ul className="warn">
            {state.errors.map((e, i) => (
              <li key={i}>
                {e.line === null ? t("producer.batch.noLine") : t("producer.batch.line", { line: e.line })}
                {tr(e.message)}
              </li>
            ))}
          </ul>
          {state.failed > shown && (
            <p className="muted">
              {t("producer.batch.truncated", { shown, total: state.failed })}
            </p>
          )}
        </>
      )}
      {!connection.readOnly &&
        (confirming ? (
          <ConfirmDialog
            connection={connection}
            title={t("producer.batch.confirmTitle")}
            confirmLabel={t("producer.batch.confirmLabel")}
            onConfirm={(c) => {
              const v = validate();
              if (v) void start(v, c.confirm);
              else setConfirming(false);
            }}
            onCancel={() => setConfirming(false)}
          />
        ) : (
          <div className="toolbar">
            <button type="button" className="primary" disabled={running} onClick={request}>
              {t("producer.batch.start")}
            </button>
            <button type="button" disabled={!running} onClick={() => void cancel()}>
              {t("producer.batch.cancel")}
            </button>
          </div>
        ))}
    </fieldset>
  );
}
