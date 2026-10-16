import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { save } from "@tauri-apps/plugin-dialog";
import { api } from "../../api/client";
import { ErrorBanner } from "../common/ErrorBanner";

export function AboutPanel() {
  const info = useQuery({ queryKey: ["app-info"], queryFn: api.getAppInfo });
  const license = useQuery({ queryKey: ["license"], queryFn: api.getLicenseStatus });
  const policy = useQuery({ queryKey: ["policy"], queryFn: api.getPolicyStatus });
  const [saved, setSaved] = useState<string | null>(null);
  const diag = useMutation({
    mutationFn: async () => {
      const date = new Date().toISOString().slice(0, 10);
      const path = await save({ defaultPath: `diagnostics-${date}.zip`, filters: [{ name: "ZIP", extensions: ["zip"] }] });
      if (!path) return null;
      await api.exportDiagnostics(path);
      return path;
    },
    onSuccess: (path) => setSaved(path),
  });

  if (info.error) return <ErrorBanner error={info.error} onRetry={() => info.refetch()} />;
  const i = info.data;
  if (!i) return <p className="empty">Загрузка…</p>;
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{i.name}</h2>
      </div>
      <dl className="stats">
        <div><dt>Версия</dt><dd>{i.version}</dd></div>
        <div><dt>Сборка</dt><dd>{i.commit}</dd></div>
        <div><dt>Система</dt><dd>{i.os} / {i.arch}</dd></div>
        <div><dt>Тариф</dt><dd>{license.data ? (license.data.tier === "pro" ? "Pro" : "Free") : "…"}</dd></div>
        <div><dt>Политика организации</dt><dd>{policy.data ? (policy.data.managed ? "задана" : "нет") : "…"}</dd></div>
      </dl>
      <p className="muted">Данные: <code>{i.dataDir}</code></p>
      <p className="muted">Логи: <code>{i.logDir}</code></p>
      <section className="panel">
        <h3>Диагностика</h3>
        <p className="muted">
          Архив с логами, версией и профилями подключений без паролей. Приложите его к обращению — само приложение ничего не отправляет.
        </p>
        <div className="toolbar">
          <button type="button" disabled={diag.isPending} onClick={() => diag.mutate()}>
            Собрать диагностику
          </button>
        </div>
        {saved && <p role="status">Архив сохранён: {saved}</p>}
        {diag.error && <ErrorBanner error={diag.error} />}
      </section>
    </div>
  );
}
