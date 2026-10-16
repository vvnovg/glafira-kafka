import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { LANGUAGES } from "../../i18n/languages";
import { i18n, setLanguage } from "../../i18n";
import { ErrorBanner } from "../common/ErrorBanner";

export function SettingsPanel() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const license = useQuery({ queryKey: ["license"], queryFn: api.getLicenseStatus, staleTime: 60_000 });
  const tier = license.data?.tier;
  const lang = useQuery({ queryKey: ["language", tier], queryFn: api.getLanguageSettings });
  const change = useMutation({
    mutationFn: (value: string) => api.setLanguage(value || null),
    onSuccess: (st) => {
      qc.setQueryData(["language", tier], st);
      void setLanguage(st.effective);
    },
  });

  if (lang.error) return <ErrorBanner error={lang.error} onRetry={() => lang.refetch()} />;
  const s = lang.data;
  if (!s) return <p className="empty">{t("common.loading")}</p>;

  const byCode = new Map(LANGUAGES.map((l) => [l.code, l]));
  const systemName = s.systemMatch ? byCode.get(s.systemMatch as never)?.nativeName : undefined;
  const isFree = tier !== "pro";
  const byPolicy = s.policy !== null;
  // Псевдоязык — только dev-сборка, без сохранения: выбор не уходит в бэкенд.
  const pseudoOn = import.meta.env.DEV && i18n.language === "x-pseudo";

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{t("settings.title")}</h2>
      </div>
      <section>
        <h3>{t("settings.language.section")}</h3>
        <div className="field">
          <label htmlFor="settings-language">{t("settings.language.label")}</label>
          <select
            id="settings-language"
            value={pseudoOn ? "x-pseudo" : byPolicy ? s.policy ?? "" : s.chosen ?? ""}
            disabled={byPolicy || change.isPending}
            onChange={(e) => (e.target.value === "x-pseudo" ? void setLanguage("x-pseudo") : change.mutate(e.target.value))}
          >
            <option value="">
              {systemName ? t("settings.language.system", { name: systemName }) : t("settings.language.systemUnknown")}
            </option>
            {LANGUAGES.filter((l) => s.languages.includes(l.code)).map((l) => {
              const locked = l.pro && s.proLanguages.includes(l.code) && isFree;
              return (
                <option key={l.code} value={l.code} disabled={locked}>
                  {locked ? t("settings.language.proSuffix", { name: l.nativeName }) : l.nativeName}
                </option>
              );
            })}
            {import.meta.env.DEV && <option value="x-pseudo">Pseudo (dev)</option>}
          </select>
        </div>
        {byPolicy && <p className="hint">{t("settings.language.policy")}</p>}
        {s.reason === "proRequired" && <p className="hint">{t("settings.language.proRequired")}</p>}
        {change.error && <ErrorBanner error={change.error} />}
      </section>
    </div>
  );
}
