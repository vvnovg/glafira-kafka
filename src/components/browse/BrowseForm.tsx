import { useTranslation } from "react-i18next";
import type { DataFormat, StartPosition } from "../../api/types";
import { Hint } from "../common/Hint";
import { FilterFields } from "./FilterFields";
import type { BrowseFormState } from "./params";

const FORMATS: DataFormat[] = ["auto", "string", "json", "hex", "base64"];
const STARTS: { id: StartPosition["type"]; labelKey: string }[] = [
  { id: "latest", labelKey: "browse.form.startLatest" },
  { id: "earliest", labelKey: "browse.form.startEarliest" },
  { id: "offset", labelKey: "browse.form.startOffset" },
  { id: "timestamp", labelKey: "browse.form.startTimestamp" },
  { id: "lastMinutes", labelKey: "browse.form.startLastMinutes" },
];

interface Props {
  form: BrowseFormState;
  onChange: (f: BrowseFormState) => void;
}

export function BrowseForm({ form, onChange }: Props) {
  const { t } = useTranslation();
  const set = <K extends keyof BrowseFormState>(k: K, v: BrowseFormState[K]) => onChange({ ...form, [k]: v });
  return (
    <div className="browse-form">
      <fieldset>
        <legend>{t("browse.form.from")}</legend>
        <select aria-label={t("browse.form.startPosition")} value={form.startType} onChange={(e) => set("startType", e.target.value as StartPosition["type"])}>
          {STARTS.map((s) => (
            <option key={s.id} value={s.id}>
              {t(s.labelKey)}
            </option>
          ))}
        </select>
        <Hint k="hints.browse.startPosition" label={t("browse.form.startPosition")} />
        {form.startType === "latest" && (
          <input aria-label="N" type="number" min={1} value={form.count} onChange={(e) => set("count", Number(e.target.value))} style={{ width: 80 }} />
        )}
        {form.startType === "offset" && (
          <input aria-label={t("browse.form.offsetAria")} type="number" min={0} value={form.offset} onChange={(e) => set("offset", Number(e.target.value))} style={{ width: 100 }} />
        )}
        {form.startType === "timestamp" && (
          <input aria-label={t("browse.form.timeAria")} type="datetime-local" step={1} value={form.timestamp} onChange={(e) => set("timestamp", e.target.value)} />
        )}
        {form.startType === "lastMinutes" && (
          <input aria-label={t("browse.form.minutesAria")} type="number" min={1} value={form.minutes} onChange={(e) => set("minutes", Number(e.target.value))} style={{ width: 70 }} />
        )}
        <input aria-label={t("browse.form.partitionsAria")} placeholder={t("browse.form.partitionsPlaceholder")} value={form.partitions} onChange={(e) => set("partitions", e.target.value)} style={{ width: 110 }} />
        <label>
          {t("browse.form.limit")} <input type="number" min={1} max={100000} value={form.limit} onChange={(e) => set("limit", Number(e.target.value))} style={{ width: 80 }} />
        </label>
        <Hint k="hints.browse.limit" label={t("browse.form.limit")} />
        <label>
          <input type="checkbox" checked={form.live} onChange={(e) => set("live", e.target.checked)} /> Live tail
        </label>
      </fieldset>
      <fieldset>
        <legend>
          {t("browse.form.format")} <Hint k="hints.browse.format" label={t("browse.form.format")} />
        </legend>
        <label>
          {t("browse.form.key")}{" "}
          <select value={form.keyFormat} onChange={(e) => set("keyFormat", e.target.value as DataFormat)}>
            {FORMATS.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
        </label>
        <label>
          {t("browse.form.value")}{" "}
          <select value={form.valueFormat} onChange={(e) => set("valueFormat", e.target.value as DataFormat)}>
            {FORMATS.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
        </label>
      </fieldset>
      <FilterFields value={form} onChange={onChange} />
    </div>
  );
}
