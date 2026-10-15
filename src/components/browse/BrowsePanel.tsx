import { useState } from "react";
import type { BrowsedMessage } from "../../api/types";
import { formatCount } from "../../lib/format";
import { ErrorBanner } from "../common/ErrorBanner";
import { BrowseForm } from "./BrowseForm";
import { MessageDetails } from "./MessageDetails";
import { messageKey, MessageTable } from "./MessageTable";
import { buildBrowseParams, defaultBrowseForm } from "./params";
import { useBrowseSession } from "./useBrowseSession";

const FINISH_TEXT = { limit: "достигнут лимит", endOfTopic: "дочитано до конца", cancelled: "остановлено" } as const;

export function BrowsePanel({ connectionId, topic }: { connectionId: string; topic: string }) {
  const [form, setForm] = useState(defaultBrowseForm);
  const [formError, setFormError] = useState<string | null>(null);
  const [selected, setSelected] = useState<BrowsedMessage | null>(null);
  const [utc, setUtc] = useState(false);
  const { state, start, stop } = useBrowseSession();
  const running = state.status === "running";

  const onStart = () => {
    try {
      const params = buildBrowseParams(form, connectionId, topic);
      setFormError(null);
      setSelected(null);
      void start(params);
    } catch (e) {
      setFormError((e as Error).message);
    }
  };

  return (
    <div className="browse">
      <BrowseForm form={form} onChange={setForm} />
      <div className="toolbar">
        <button type="button" className="primary" onClick={onStart}>
          {running ? "Перезапустить" : "Читать"}
        </button>
        <button type="button" disabled={!running} onClick={() => void stop()}>
          Остановить
        </button>
        <label>
          <input type="checkbox" checked={utc} onChange={(e) => setUtc(e.target.checked)} /> UTC
        </label>
        <span className="muted">
          просмотрено {formatCount(state.scanned)} · найдено {formatCount(state.matched)} · в таблице {state.messages.length}
          {running &&
            (state.reconnecting
              ? ` · переподключение: ${state.reconnecting}…`
              : state.reachedEnd
                ? " · ждём новые сообщения…"
                : " · читаем…")}
          {state.status === "finished" && state.finishReason && ` · ${FINISH_TEXT[state.finishReason]}`}
        </span>
      </div>
      {formError && <ErrorBanner error={{ code: "INVALID_INPUT", message: formError, retryable: false }} />}
      {state.error && <ErrorBanner error={state.error} onRetry={onStart} />}
      <div className="browse-body">
        <MessageTable
          messages={state.messages}
          selectedKey={selected ? messageKey(selected) : null}
          onSelect={setSelected}
          utc={utc}
        />
        <MessageDetails message={selected} utc={utc} connectionId={connectionId} />
      </div>
    </div>
  );
}
