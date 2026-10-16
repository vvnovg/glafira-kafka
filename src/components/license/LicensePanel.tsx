import { useId, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { LicenseStatus } from "../../api/types";
import { ErrorBanner } from "../common/ErrorBanner";

const KIND = { personal: "личная", organization: "организации" } as const;

export function LicensePanel() {
  const uid = useId();
  const queryClient = useQueryClient();
  const license = useQuery({ queryKey: ["license"], queryFn: api.getLicenseStatus });
  const policy = useQuery({ queryKey: ["policy"], queryFn: api.getPolicyStatus });
  const [key, setKey] = useState("");
  const [confirmRemove, setConfirmRemove] = useState(false);
  const apply = (st: LicenseStatus) => queryClient.setQueryData(["license"], st);
  const activate = useMutation({
    mutationFn: (k: string) => api.setLicenseKey(k),
    onSuccess: (st) => {
      apply(st);
      setKey("");
    },
  });
  const remove = useMutation({
    mutationFn: () => api.removeLicenseKey(),
    onSuccess: (st) => {
      apply(st);
      setConfirmRemove(false);
    },
  });

  if (license.error) return <ErrorBanner error={license.error} onRetry={() => license.refetch()} />;
  const st = license.data;
  if (!st) return <p className="empty">Загрузка…</p>;
  const managed = st.source === "policy";

  return (
    <div className="page">
      <div className="toolbar">
        <h2>Лицензия</h2>
      </div>
      <dl className="stats">
        <div>
          <dt>Тариф</dt>
          <dd>{st.tier === "pro" ? "Pro" : "Free"}</dd>
        </div>
        <div>
          <dt>Владелец</dt>
          <dd>{st.licensee ?? "—"}</dd>
        </div>
        <div>
          <dt>Тип</dt>
          <dd>{st.kind ? KIND[st.kind] : "—"}</dd>
        </div>
        <div>
          <dt>Действует до</dt>
          <dd>{st.expires ?? (st.licensee ? "бессрочно" : "—")}</dd>
        </div>
      </dl>
      {st.state === "expiring" && <p className="warn">Лицензия истекает через {st.daysLeft} дн. ({st.expires}).</p>}
      {st.state === "expired" && <p className="warn">Лицензия истекла {st.expires}: функции Pro отключены, данные сохранены.</p>}
      {st.state === "invalid" && <p className="warn">Ключ недействителен: {st.error}</p>}

      {managed ? (
        <p className="muted">Лицензия управляется вашей организацией.</p>
      ) : (
        <div className="panel">
          <label htmlFor={`${uid}-key`}>Лицензионный ключ</label>
          <textarea id={`${uid}-key`} rows={3} value={key} onChange={(e) => setKey(e.target.value)} />
          <div className="toolbar">
            <button type="button" className="primary" disabled={!key.trim() || activate.isPending} onClick={() => activate.mutate(key.trim())}>
              Активировать
            </button>
            {st.source === "user" &&
              (confirmRemove ? (
                <span role="group" aria-label="Подтверждение удаления ключа">
                  Удалить ключ? Функции Pro отключатся.{" "}
                  <button type="button" className="danger" disabled={remove.isPending} onClick={() => remove.mutate()}>
                    Да, удалить
                  </button>{" "}
                  <button type="button" onClick={() => setConfirmRemove(false)}>
                    Отмена
                  </button>
                </span>
              ) : (
                <button type="button" className="danger" onClick={() => setConfirmRemove(true)}>
                  Удалить ключ
                </button>
              ))}
          </div>
          {activate.error && <ErrorBanner error={activate.error} />}
          {remove.error && <ErrorBanner error={remove.error} />}
        </div>
      )}

      <section>
        <h3>Что даёт Pro</h3>
        <ul>
          <li>Consumer-сессии: чтение как участник группы, коммит оффсетов, пауза партиций.</li>
          <li>
            Режим prod: создание и перевод подключений в prod (с подтверждением именем подключения на любое
            изменение).
          </li>
        </ul>
      </section>

      {policy.data?.managed && (
        <section>
          <h3>Политика организации</h3>
          <p className="muted">Источник: {policy.data.source}</p>
          {policy.data.errors.map((e) => (
            <p key={e} className="warn">
              {e}
            </p>
          ))}
        </section>
      )}
    </div>
  );
}
