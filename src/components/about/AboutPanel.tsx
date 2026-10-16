import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { save } from "@tauri-apps/plugin-dialog";
import { api, toAppError } from "../../api/client";
import { ErrorBanner } from "../common/ErrorBanner";

export function AboutPanel() {
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
      <UpdatesSection />
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

function UpdatesSection() {
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
      <h3>Обновления</h3>
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
          Проверить обновления
        </button>
      </div>
      {check.error && <ErrorBanner error={check.error} onRetry={() => check.mutate()} />}
      {r?.status === "disabled" && <p className="muted">{r.reason}</p>}
      {r?.status === "upToDate" && <p role="status">Установлена последняя версия</p>}
      {r?.status === "available" && (
        <>
          <p role="status">Доступна версия {r.version}</p>
          {r.notes && <pre>{r.notes}</pre>}
          {confirm ? (
            <div className="confirm" role="alertdialog" aria-label="Подтверждение обновления">
              <p>{confirm}</p>
              <div className="toolbar">
                <button type="button" disabled={install.isPending} onClick={() => install.mutate(true)}>
                  Остановить сессии и обновить
                </button>
                <button type="button" onClick={() => setConfirm(null)}>
                  Отмена
                </button>
              </div>
            </div>
          ) : (
            <div className="toolbar">
              <button type="button" disabled={install.isPending} onClick={() => install.mutate(false)}>
                Установить и перезапустить
              </button>
            </div>
          )}
          {install.isPending && <p className="muted">Загрузка и установка обновления…</p>}
          {installError && <ErrorBanner error={installError} />}
        </>
      )}
    </section>
  );
}
