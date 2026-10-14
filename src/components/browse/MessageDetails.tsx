import type { BrowsedMessage, DecodedData } from "../../api/types";
import { formatBytes, formatTimestamp } from "../../lib/format";
import { prettyText } from "./params";

function copy(text: string) {
  try {
    void navigator.clipboard.writeText(text).catch(() => {});
  } catch {
    /* буфер обмена недоступен */
  }
}

function DataBlock({ title, data }: { title: string; data: DecodedData | null }) {
  return (
    <section>
      <h4>
        {title} {data && <span className="muted">({data.format}, {formatBytes(data.size)})</span>}
        {data && (
          <button
            type="button"
            className="link"
            aria-label={`Копировать ${title === "Ключ" ? "ключ" : "значение"}`}
            onClick={() => copy(data.text)}
          >
            копировать
          </button>
        )}
      </h4>
      {data?.error && <p className="warn">{data.error}</p>}
      {data?.truncated && <p className="warn">Показан первый 1 МБ значения.</p>}
      <pre>{data ? prettyText(data) : <span className="muted">null</span>}</pre>
    </section>
  );
}

export function MessageDetails({ message, utc }: { message: BrowsedMessage | null; utc: boolean }) {
  if (!message) return <div className="details-pane muted">Выберите сообщение</div>;
  return (
    <div className="details-pane">
      <p className="muted">
        Партиция {message.partition} · offset {message.offset} · {formatTimestamp(message.timestamp, utc)}
        <button
          type="button"
          className="link"
          aria-label="Копировать сообщение как JSON"
          onClick={() => copy(JSON.stringify(message, null, 2))}
        >
          {" "}
          копировать всё как JSON
        </button>
      </p>
      <DataBlock title="Значение" data={message.value} />
      <DataBlock title="Ключ" data={message.key} />
      <section>
        <h4>Заголовки ({message.headers.length})</h4>
        {message.headers.length > 0 && (
          <table>
            <tbody>
              {message.headers.map((h, i) => (
                <tr key={`${h.name}-${i}`}>
                  <td>{h.name}</td>
                  <td>
                    <code>{h.value ?? "null"}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}
