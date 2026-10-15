import type { HeaderInput } from "../../api/types";

export function HeadersEditor({ headers, onChange }: { headers: HeaderInput[]; onChange: (h: HeaderInput[]) => void }) {
  const set = (i: number, patch: Partial<HeaderInput>) => onChange(headers.map((h, j) => (j === i ? { ...h, ...patch } : h)));
  return (
    <fieldset className="headers-editor">
      <legend>Заголовки</legend>
      {headers.length > 0 && (
        <div className="toolbar muted headers-captions">
          <span>Имя</span>
          <span>Значение</span>
          <span>Формат</span>
        </div>
      )}
      {headers.map((h, i) => (
        <div key={i} className="toolbar">
          <input aria-label={`Имя заголовка ${i + 1}`} placeholder="имя" value={h.name} onChange={(e) => set(i, { name: e.target.value })} />
          <input aria-label={`Значение заголовка ${i + 1}`} placeholder="значение" value={h.value} onChange={(e) => set(i, { value: e.target.value })} />
          <select aria-label={`Формат заголовка ${i + 1}`} value={h.format} onChange={(e) => set(i, { format: e.target.value as HeaderInput["format"] })}>
            <option value="string">string</option>
            <option value="hex">hex</option>
          </select>
          <button type="button" className="icon" aria-label={`Удалить заголовок ${i + 1}`} onClick={() => onChange(headers.filter((_, j) => j !== i))}>
            ×
          </button>
        </div>
      ))}
      <button type="button" onClick={() => onChange([...headers, { name: "", value: "", format: "string" }])}>
        Добавить заголовок
      </button>
    </fieldset>
  );
}
