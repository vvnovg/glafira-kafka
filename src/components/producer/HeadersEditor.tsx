import { useTranslation } from "react-i18next";
import type { HeaderInput } from "../../api/types";

export function HeadersEditor({ headers, onChange }: { headers: HeaderInput[]; onChange: (h: HeaderInput[]) => void }) {
  const { t } = useTranslation();
  const set = (i: number, patch: Partial<HeaderInput>) => onChange(headers.map((h, j) => (j === i ? { ...h, ...patch } : h)));
  return (
    <fieldset className="headers-editor">
      <legend>{t("producer.headers.legend")}</legend>
      {headers.length > 0 && (
        <div className="toolbar muted headers-captions">
          <span>{t("producer.headers.colName")}</span>
          <span>{t("producer.headers.colValue")}</span>
          <span>{t("producer.headers.colFormat")}</span>
        </div>
      )}
      {headers.map((h, i) => (
        <div key={i} className="toolbar">
          <input aria-label={t("producer.headers.nameAria", { n: i + 1 })} placeholder={t("producer.headers.namePlaceholder")} value={h.name} onChange={(e) => set(i, { name: e.target.value })} />
          <input aria-label={t("producer.headers.valueAria", { n: i + 1 })} placeholder={t("producer.headers.valuePlaceholder")} value={h.value} onChange={(e) => set(i, { value: e.target.value })} />
          <select aria-label={t("producer.headers.formatAria", { n: i + 1 })} value={h.format} onChange={(e) => set(i, { format: e.target.value as HeaderInput["format"] })}>
            <option value="string">string</option>
            <option value="hex">hex</option>
          </select>
          <button type="button" className="icon" aria-label={t("producer.headers.removeAria", { n: i + 1 })} onClick={() => onChange(headers.filter((_, j) => j !== i))}>
            ×
          </button>
        </div>
      ))}
      <button type="button" onClick={() => onChange([...headers, { name: "", value: "", format: "string" }])}>
        {t("producer.headers.add")}
      </button>
    </fieldset>
  );
}
