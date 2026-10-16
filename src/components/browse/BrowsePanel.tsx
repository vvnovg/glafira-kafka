import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { BrowsedMessage } from "../../api/types";
import { tr } from "../../i18n/text";
import { formatCount } from "../../lib/format";
import { ErrorBanner } from "../common/ErrorBanner";
import { BrowseForm } from "./BrowseForm";
import { MessageDetails } from "./MessageDetails";
import { messageKey, MessageTable } from "./MessageTable";
import { buildBrowseParams, defaultBrowseForm } from "./params";
import { useBrowseSession } from "./useBrowseSession";

const FINISH_KEYS = { limit: "browse.panel.finishLimit", endOfTopic: "browse.panel.finishEnd", cancelled: "browse.panel.finishCancelled" } as const;

export function BrowsePanel({ connectionId, topic }: { connectionId: string; topic: string }) {
  const { t } = useTranslation();
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
          {running ? t("browse.panel.restart") : t("browse.panel.read")}
        </button>
        <button type="button" disabled={!running} onClick={() => void stop()}>
          {t("browse.panel.stop")}
        </button>
        <label>
          <input type="checkbox" checked={utc} onChange={(e) => setUtc(e.target.checked)} /> UTC
        </label>
        <span className="muted">
          {t("browse.panel.summary", { scanned: formatCount(state.scanned), matched: formatCount(state.matched), shown: state.messages.length })}
          {running &&
            ` · ${
              state.reconnecting
                ? t("browse.panel.reconnecting", { reason: tr(state.reconnecting) })
                : state.reachedEnd
                  ? t("browse.panel.waiting")
                  : t("browse.panel.reading")
            }`}
          {state.status === "finished" && state.finishReason && ` · ${t(FINISH_KEYS[state.finishReason])}`}
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
