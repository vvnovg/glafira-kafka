import { useRef } from "react";
import { useTranslation } from "react-i18next";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { BrowsedMessage } from "../../api/types";
import { activateProps } from "../../lib/activate";
import { formatBytes, formatTimestamp } from "../../lib/format";
import { previewText } from "./params";

const ROW_HEIGHT = 28;

export const messageKey = (m: BrowsedMessage) => `${m.topic}:${m.partition}:${m.offset}`;

interface Props {
  messages: BrowsedMessage[];
  selectedKey: string | null;
  onSelect: (m: BrowsedMessage) => void;
  utc: boolean;
}

export function MessageTable({ messages, selectedKey, onSelect, utc }: Props) {
  const { t } = useTranslation();
  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: messages.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 30,
  });
  return (
    <div className="vlist" ref={parentRef}>
      <div className="row header msg-row" style={{ height: ROW_HEIGHT }}>
        <span>P</span>
        <span>Offset</span>
        <span>{utc ? t("browse.table.colTimeUtc") : t("browse.table.colTime")}</span>
        <span>{t("browse.table.colKey")}</span>
        <span>{t("browse.table.colValue")}</span>
        <span>{t("browse.table.colHeaders")}</span>
        <span>{t("browse.table.colSize")}</span>
      </div>
      <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
        {virtualizer.getVirtualItems().map((item) => {
          const m = messages[item.index];
          const key = messageKey(m);
          return (
            <div
              key={key}
              className={`row clickable msg-row${key === selectedKey ? " selected" : ""}`}
              style={{ top: item.start, height: ROW_HEIGHT }}
              aria-pressed={key === selectedKey}
              {...activateProps(() => onSelect(m))}
            >
              <span>{m.partition}</span>
              <span>{m.offset}</span>
              <span>{formatTimestamp(m.timestamp, utc)}</span>
              <span title={m.key?.text.slice(0, 500)}>{previewText(m.key, 60)}</span>
              <span>{previewText(m.value)}</span>
              <span>{m.headers.length || ""}</span>
              <span>{formatBytes(m.size)}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
