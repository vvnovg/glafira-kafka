import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { AppError, ConnectionProfile, Environment, Protocol, SaslMechanism, TestReport } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { formatProps, parseProps, withProtocol } from "./profile";

const ENVIRONMENTS: Environment[] = ["local", "dev", "test", "stage", "prod"];
const PROTOCOLS: Protocol[] = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];
const MECHANISMS: SaslMechanism[] = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];
const STEP_LABELS = { config: "Конфигурация", tcp: "DNS / TCP", kafka: "Kafka (TLS, SASL, метаданные)" } as const;

export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile; tabId: string }) {
  const [profile, setProfile] = useState(initial);
  const [password, setPassword] = useState("");
  const [propsText, setPropsText] = useState(formatProps(initial.extraProperties));
  const [report, setReport] = useState<TestReport | null>(null);
  const [error, setError] = useState<AppError | null>(null);
  const [busy, setBusy] = useState(false);
  // Подтверждение удаления внутри формы: нативный window.confirm в Tauri 2 не показывается и возвращает false.
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const queryClient = useQueryClient();
  const open = useWorkspace((s) => s.open);
  const close = useWorkspace((s) => s.close);
  const closeForConnection = useWorkspace((s) => s.closeForConnection);
  const isNew = initial.id === "";
  const sec = profile.security;

  const update = (patch: Partial<ConnectionProfile>) => setProfile((p) => ({ ...p, ...patch }));
  const current = (): ConnectionProfile => ({ ...profile, extraProperties: parseProps(propsText) });

  async function run(fn: () => Promise<void>) {
    setBusy(true);
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError(toAppError(e));
    } finally {
      setBusy(false);
    }
  }

  const onTest = () =>
    run(async () => {
      setReport(null);
      setReport(await api.testConnection(current(), password || undefined));
    });

  const onSave = () =>
    run(async () => {
      const saved = await api.saveConnection(current(), password || undefined);
      await queryClient.invalidateQueries({ queryKey: ["connections"] });
      queryClient.removeQueries({ predicate: (q) => q.queryKey[1] === saved.id });
      close(tabId);
      open({ kind: "cluster", connectionId: saved.id });
    });

  const onDelete = () =>
    run(async () => {
      setConfirmingDelete(false);
      await api.deleteConnection(profile.id);
      await queryClient.invalidateQueries({ queryKey: ["connections"] });
      // Вкладки и кэш удалённого подключения больше не нужны (сессии чтения отменяет backend).
      queryClient.removeQueries({ predicate: (q) => q.queryKey[1] === profile.id });
      closeForConnection(profile.id);
      close(tabId);
    });

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{isNew ? "Новое подключение" : `Подключение «${initial.name}»`}</h2>
      </div>
      <div className="form">
        <label htmlFor="cf-name">Имя</label>
        <input id="cf-name" value={profile.name} onChange={(e) => update({ name: e.target.value })} />

        <label htmlFor="cf-env">Окружение</label>
        <select
          id="cf-env"
          value={profile.environment}
          onChange={(e) => {
            const environment = e.target.value as Environment;
            update({ environment, readOnly: isNew && environment === "prod" ? true : profile.readOnly });
          }}
        >
          {ENVIRONMENTS.map((env) => (
            <option key={env} value={env}>
              {env}
            </option>
          ))}
        </select>

        <label htmlFor="cf-ro">Только чтение</label>
        <input id="cf-ro" type="checkbox" checked={profile.readOnly} onChange={(e) => update({ readOnly: e.target.checked })} />

        <label htmlFor="cf-color">Цвет</label>
        <input id="cf-color" type="color" value={profile.color} onChange={(e) => update({ color: e.target.value })} />

        <label htmlFor="cf-bootstrap">Bootstrap-серверы</label>
        <input
          id="cf-bootstrap"
          placeholder="host1:9092,host2:9092"
          value={profile.bootstrapServers}
          onChange={(e) => update({ bootstrapServers: e.target.value })}
        />

        <label htmlFor="cf-protocol">Протокол</label>
        <select id="cf-protocol" value={sec.protocol} onChange={(e) => update({ security: withProtocol(sec, e.target.value as Protocol) })}>
          {PROTOCOLS.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>

        {"tls" in sec && (
          <>
            <label htmlFor="cf-ca">CA-сертификат (путь к PEM)</label>
            <input
              id="cf-ca"
              placeholder="пусто — системное хранилище"
              value={sec.tls.caLocation ?? ""}
              onChange={(e) => update({ security: { ...sec, tls: { ...sec.tls, caLocation: e.target.value || null } } })}
            />
            <label htmlFor="cf-verify">Проверять hostname</label>
            <input
              id="cf-verify"
              type="checkbox"
              checked={sec.tls.verifyHostname}
              onChange={(e) => update({ security: { ...sec, tls: { ...sec.tls, verifyHostname: e.target.checked } } })}
            />
          </>
        )}

        {"sasl" in sec && (
          <>
            <label htmlFor="cf-mech">Механизм SASL</label>
            <select
              id="cf-mech"
              value={sec.sasl.mechanism}
              onChange={(e) => update({ security: { ...sec, sasl: { ...sec.sasl, mechanism: e.target.value as SaslMechanism } } })}
            >
              {MECHANISMS.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
            <label htmlFor="cf-user">Пользователь</label>
            <input
              id="cf-user"
              value={sec.sasl.username}
              onChange={(e) => update({ security: { ...sec, sasl: { ...sec.sasl, username: e.target.value } } })}
            />
            <label htmlFor="cf-pass">Пароль</label>
            <input
              id="cf-pass"
              type="password"
              autoComplete="off"
              placeholder={isNew ? "" : "оставьте пустым, чтобы не менять"}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </>
        )}

        <label htmlFor="cf-timeout">Таймаут запроса, мс</label>
        <input
          id="cf-timeout"
          type="number"
          min={1000}
          step={1000}
          value={profile.requestTimeoutMs}
          onChange={(e) => update({ requestTimeoutMs: Number(e.target.value) })}
        />

        <label htmlFor="cf-props">Доп. свойства librdkafka</label>
        <textarea id="cf-props" rows={4} placeholder="key=value на строку" value={propsText} onChange={(e) => setPropsText(e.target.value)} />
      </div>

      <div className="toolbar">
        <button type="button" disabled={busy} onClick={onTest}>
          Проверить подключение
        </button>
        <button type="button" className="primary" disabled={busy} onClick={onSave}>
          Сохранить
        </button>
        {!isNew && (
          confirmingDelete ? (
            <span role="group" aria-label="Подтверждение удаления">
              <strong>Удалить подключение «{profile.name}»?</strong>{" "}
              <button type="button" className="danger" disabled={busy} onClick={onDelete}>
                Да, удалить
              </button>{" "}
              <button type="button" disabled={busy} onClick={() => setConfirmingDelete(false)}>
                Отмена
              </button>
            </span>
          ) : (
            <button type="button" className="danger" disabled={busy} onClick={() => setConfirmingDelete(true)}>
              Удалить
            </button>
          )
        )}
        {busy && <span className="muted">Выполняется…</span>}
      </div>

      {error && <ErrorBanner error={error} />}

      {report && (
        <ul className="steps">
          {report.steps.map((s) => (
            <li key={s.name}>
              {s.ok ? "✅" : "❌"} <strong>{STEP_LABELS[s.name]}:</strong> <span>{s.message}</span>
              {s.error?.details && <pre className="muted">{s.error.details}</pre>}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
