import type { FilterFields as Fields } from "./params";

/** Fieldset «Фильтры»: общий для просмотра топика и consumer-сессии. */
export function FilterFields<T extends Fields>({ value: form, onChange }: { value: T; onChange: (v: T) => void }) {
  const set = <K extends keyof Fields>(k: K, v: Fields[K]) => onChange({ ...form, [k]: v } as T);
  return (
    <fieldset>
      <legend>Фильтры</legend>
      <label className="field">
        <span>Ключ содержит</span>
        <input placeholder="k7" value={form.keyPattern} onChange={(e) => set("keyPattern", e.target.value)} />
      </label>
      <label className="field">
        <span>Значение содержит</span>
        <input placeholder="FAILED" value={form.valuePattern} onChange={(e) => set("valuePattern", e.target.value)} />
      </label>
      <label>
        <input type="checkbox" checked={form.regex} onChange={(e) => set("regex", e.target.checked)} /> regex
      </label>
      <label>
        <input type="checkbox" checked={form.caseSensitive} onChange={(e) => set("caseSensitive", e.target.checked)} /> Aa
      </label>
      <label className="field">
        <span>Заголовок</span>
        <input placeholder="trace-id" value={form.headerName} onChange={(e) => set("headerName", e.target.value)} style={{ width: 110 }} />
      </label>
      <label className="field">
        <span>Значение заголовка</span>
        <input placeholder="abc-123" value={form.headerValue} onChange={(e) => set("headerValue", e.target.value)} style={{ width: 130 }} />
      </label>
      <label className="field">
        <span>JSONPath</span>
        <input
          placeholder={'$.order.status == "FAILED"'}
          value={form.jsonPath}
          onChange={(e) => set("jsonPath", e.target.value)}
          style={{ width: 260 }}
        />
      </label>
      <label className="field">
        <span>Время с</span>
        <input type="datetime-local" step={1} value={form.fromTime} onChange={(e) => set("fromTime", e.target.value)} />
      </label>
      <label className="field">
        <span>Время по</span>
        <input type="datetime-local" step={1} value={form.toTime} onChange={(e) => set("toTime", e.target.value)} />
      </label>
    </fieldset>
  );
}
