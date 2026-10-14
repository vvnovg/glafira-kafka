import type { DataFormat, StartPosition } from "../../api/types";
import type { BrowseFormState } from "./params";

const FORMATS: DataFormat[] = ["auto", "string", "json", "hex", "base64"];
const STARTS: { id: StartPosition["type"]; label: string }[] = [
  { id: "latest", label: "Последние N" },
  { id: "earliest", label: "С начала" },
  { id: "offset", label: "С оффсета" },
  { id: "timestamp", label: "С времени" },
  { id: "lastMinutes", label: "За последние X минут" },
];

interface Props {
  form: BrowseFormState;
  onChange: (f: BrowseFormState) => void;
}

export function BrowseForm({ form, onChange }: Props) {
  const set = <K extends keyof BrowseFormState>(k: K, v: BrowseFormState[K]) => onChange({ ...form, [k]: v });
  return (
    <div className="browse-form">
      <fieldset>
        <legend>Откуда</legend>
        <select aria-label="Начальная позиция" value={form.startType} onChange={(e) => set("startType", e.target.value as StartPosition["type"])}>
          {STARTS.map((s) => (
            <option key={s.id} value={s.id}>
              {s.label}
            </option>
          ))}
        </select>
        {form.startType === "latest" && (
          <input aria-label="N" type="number" min={1} value={form.count} onChange={(e) => set("count", Number(e.target.value))} style={{ width: 80 }} />
        )}
        {form.startType === "offset" && (
          <input aria-label="Оффсет" type="number" min={0} value={form.offset} onChange={(e) => set("offset", Number(e.target.value))} style={{ width: 100 }} />
        )}
        {form.startType === "timestamp" && (
          <input aria-label="Время" type="datetime-local" step={1} value={form.timestamp} onChange={(e) => set("timestamp", e.target.value)} />
        )}
        {form.startType === "lastMinutes" && (
          <input aria-label="Минут" type="number" min={1} value={form.minutes} onChange={(e) => set("minutes", Number(e.target.value))} style={{ width: 70 }} />
        )}
        <input aria-label="Партиции" placeholder="партиции: все" value={form.partitions} onChange={(e) => set("partitions", e.target.value)} style={{ width: 110 }} />
        <label>
          Лимит <input type="number" min={1} max={100000} value={form.limit} onChange={(e) => set("limit", Number(e.target.value))} style={{ width: 80 }} />
        </label>
        <label>
          <input type="checkbox" checked={form.live} onChange={(e) => set("live", e.target.checked)} /> Live tail
        </label>
      </fieldset>
      <fieldset>
        <legend>Формат</legend>
        <label>
          ключ{" "}
          <select value={form.keyFormat} onChange={(e) => set("keyFormat", e.target.value as DataFormat)}>
            {FORMATS.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
        </label>
        <label>
          значение{" "}
          <select value={form.valueFormat} onChange={(e) => set("valueFormat", e.target.value as DataFormat)}>
            {FORMATS.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
        </label>
      </fieldset>
      <fieldset>
        <legend>Фильтры</legend>
        <input aria-label="ключ содержит" placeholder="ключ содержит" value={form.keyPattern} onChange={(e) => set("keyPattern", e.target.value)} />
        <input aria-label="значение содержит" placeholder="значение содержит" value={form.valuePattern} onChange={(e) => set("valuePattern", e.target.value)} />
        <label>
          <input type="checkbox" checked={form.regex} onChange={(e) => set("regex", e.target.checked)} /> regex
        </label>
        <label>
          <input type="checkbox" checked={form.caseSensitive} onChange={(e) => set("caseSensitive", e.target.checked)} /> Aa
        </label>
        <input aria-label="заголовок" placeholder="заголовок" value={form.headerName} onChange={(e) => set("headerName", e.target.value)} style={{ width: 110 }} />
        <input aria-label="= значение" placeholder="= значение" value={form.headerValue} onChange={(e) => set("headerValue", e.target.value)} style={{ width: 110 }} />
        <input
          aria-label={'JSONPath: $.order.status == "FAILED"'}
          placeholder={'JSONPath: $.order.status == "FAILED"'}
          value={form.jsonPath}
          onChange={(e) => set("jsonPath", e.target.value)}
          style={{ width: 260 }}
        />
        <label>
          с <input type="datetime-local" step={1} value={form.fromTime} onChange={(e) => set("fromTime", e.target.value)} />
        </label>
        <label>
          по <input type="datetime-local" step={1} value={form.toTime} onChange={(e) => set("toTime", e.target.value)} />
        </label>
      </fieldset>
    </div>
  );
}
