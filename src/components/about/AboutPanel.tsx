import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useMutation, useQuery } from "@tanstack/react-query";
import { save } from "@tauri-apps/plugin-dialog";
import { api, toAppError } from "../../api/client";
import { tr } from "../../i18n/text";
import { useDiagnosticsRequest } from "../../state/diagnosticsRequest";
import { ErrorBanner } from "../common/ErrorBanner";

export function AboutPanel() {
  const { t } = useTranslation();
  const info = useQuery({ queryKey: ["app-info"], queryFn: api.getAppInfo });
  const license = useQuery({ queryKey: ["license"], queryFn: api.getLicenseStatus });
  const policy = useQuery({ queryKey: ["policy"], queryFn: api.getPolicyStatus });
  const [saved, setSaved] = useState<string | null>(null);
  const diag = useMutation({
    mutationFn: async () => {
      setSaved(null);
      const date = new Date().toISOString().slice(0, 10);
      const path = await save({ defaultPath: `diagnostics-${date}.zip`, filters: [{ name: "ZIP", extensions: ["zip"] }] });
      if (!path) return null;
      await api.exportDiagnostics(path);
      return path;
    },
    onSuccess: (path) => {
      if (path) setSaved(path);
    },
  });
  // «Собрать диагностику…» из меню Help: тот же сбор, что и по кнопке; запрос забирается один раз.
  const diagRequested = useDiagnosticsRequest((s) => s.pending);
  const { mutate: collect } = diag;
  useEffect(() => {
    if (diagRequested && useDiagnosticsRequest.getState().consume()) collect();
  }, [diagRequested, collect]);

  if (info.error) return <ErrorBanner error={info.error} onRetry={() => info.refetch()} />;
  const i = info.data;
  if (!i) return <p className="empty">{t("common.loading")}</p>;
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{i.name}</h2>
      </div>
      <p className="about-summary">{t("about.summary")}</p>
      <dl className="stats">
        <div><dt>{t("about.version")}</dt><dd>{i.version}</dd></div>
        <div><dt>{t("about.build")}</dt><dd>{i.commit}</dd></div>
        <div><dt>{t("about.system")}</dt><dd>{i.os} / {i.arch}</dd></div>
        <div><dt>{t("about.tier")}</dt><dd>{license.data ? (license.data.tier === "pro" ? "Pro" : "Free") : "…"}</dd></div>
        <div><dt>{t("about.policy")}</dt><dd>{policy.data ? (policy.data.managed ? t("about.policySet") : t("about.policyNone")) : "…"}</dd></div>
      </dl>
      {policy.data?.errors.map((e, i) => (
        <p key={i} className="warn">
          {t("about.policyError", { error: tr(e) })}
        </p>
      ))}
      <p className="muted">{t("about.dataDir")} <code>{i.dataDir}</code></p>
      <p className="muted">{t("about.logDir")} <code>{i.logDir}</code></p>
      <UpdatesSection />
      <section className="panel">
        <h3>{t("about.diagnostics.title")}</h3>
        <p className="muted">
          {t("about.diagnostics.hint")}
        </p>
        <div className="toolbar">
          <button type="button" disabled={diag.isPending} onClick={() => diag.mutate()}>
            {t("about.diagnostics.collect")}
          </button>
        </div>
        {saved && <p role="status">{t("about.diagnostics.saved", { path: saved })}</p>}
        {diag.error && <ErrorBanner error={diag.error} />}
      </section>
    </div>
  );
}

function UpdatesSection() {
  const { t } = useTranslation();
  const check = useMutation({ mutationFn: api.checkUpdate });
  const [confirm, setConfirm] = useState<string | null>(null);
  const install = useMutation({
    mutationFn: (force: boolean) => api.installUpdate(force),
    onMutate: () => setConfirm(null),
    onError: (e) => {
      const err = toAppError(e);
      if (err.code === "CONFIRMATION_REQUIRED") setConfirm(err.message);
    },
  });
  const r = check.data;
  const installError = install.error && toAppError(install.error).code !== "CONFIRMATION_REQUIRED" ? install.error : null;
  return (
    <section className="panel">
      <h3>{t("about.updates.title")}</h3>
      <div className="toolbar">
        <button
          type="button"
          disabled={check.isPending || install.isPending}
          onClick={() => {
            install.reset();
            setConfirm(null);
            check.mutate();
          }}
        >
          {t("about.updates.check")}
        </button>
      </div>
      {check.error && <ErrorBanner error={check.error} onRetry={() => check.mutate()} />}
      {r?.status === "disabled" && <p className="muted">{tr(r.reason)}</p>}
      {r?.status === "upToDate" && <p role="status">{t("about.updates.upToDate")}</p>}
      {r?.status === "available" && (
        <>
          <p role="status">{t("about.updates.available", { version: r.version })}</p>
          {r.notes && <pre>{r.notes}</pre>}
          {confirm ? (
            <div className="confirm" role="alertdialog" aria-label={t("about.updates.confirmAria")}>
              <p>{confirm}</p>
              <div className="toolbar">
                <button type="button" disabled={install.isPending} onClick={() => install.mutate(true)}>
                  {t("about.updates.stopAndUpdate")}
                </button>
                <button type="button" onClick={() => setConfirm(null)}>
                  {t("common.cancel")}
                </button>
              </div>
            </div>
          ) : (
            <div className="toolbar">
              <button type="button" disabled={install.isPending} onClick={() => install.mutate(false)}>
                {t("about.updates.install")}
              </button>
            </div>
          )}
          {install.isPending && <p className="muted">{t("about.updates.installing")}</p>}
          {installError && <ErrorBanner error={installError} />}
        </>
      )}
    </section>
  );
}
