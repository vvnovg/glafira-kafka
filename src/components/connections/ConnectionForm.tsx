import { useId, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { AppError, ConnectionProfile, Environment, LlmAccess, Protocol, SaslMechanism, TestReport, TlsPasswordsInput } from "../../api/types";
import { tr } from "../../i18n/text";
import { useMcpStatus } from "../../lib/useMcpStatus";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { STANDARD_MASK_FIELDS } from "./maskFields";
import { formatProps, parseProps, withProtocol } from "./profile";
import { folderPaths } from "./tree/folderTree";
import { emptyTlsSecrets, secretValue, TlsSection, type TlsSecrets } from "./TlsSection";

const ENVIRONMENTS: Environment[] = ["local", "dev", "test", "stage", "prod"];
const PROTOCOLS: Protocol[] = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];
const MECHANISMS: SaslMechanism[] = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];
const LLM_LEVELS: LlmAccess[] = ["off", "metadata", "messages"];

export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile; tabId: string }) {
  const { t } = useTranslation();
  // Вкладки только скрываются, и формы нескольких подключений бывают в документе одновременно: с общими id подписи
  // вели бы к полям чужой формы, а с общим именем радиокнопки уровней LLM были бы одной группой на все формы.
  const uid = useId();
  const [profile, setProfile] = useState(initial);
  const [password, setPassword] = useState("");
  const [tlsSecrets, setTlsSecrets] = useState<TlsSecrets>(emptyTlsSecrets);
  const [propsText, setPropsText] = useState(formatProps(initial.extraProperties));
  const [report, setReport] = useState<TestReport | null>(null);
  const [error, setError] = useState<AppError | null>(null);
  const [busy, setBusy] = useState(false);
  // Подтверждение удаления внутри формы: нативный window.confirm в Tauri 2 не показывается и возвращает false.
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  // Политика может запретить чтение сообщений (G4). Строго: третий уровень и блок «Маскирование» — только когда статус
  // пришёл и разрешает (true), пометки политики — только при явном запрете (false). Пока статус грузится, третий уровень
  // недоступен без пометки, не загрузился — с пометкой; сохранение оставляет сохранённый уровень как есть.
  const mcpStatus = useMcpStatus();
  const messagesAllowed = mcpStatus.data?.messagesAllowed;
  const prodWarning = messagesAllowed === true && profile.llmAccess === "messages" && profile.environment === "prod";
  const [maskText, setMaskText] = useState((initial.llmMaskFields ?? STANDARD_MASK_FIELDS).join("\n"));
  const queryClient = useQueryClient();
  const foldersQ = useQuery({ queryKey: ["folders"], queryFn: api.listFolders });
  const paths = useMemo(() => folderPaths(foldersQ.data ?? []), [foldersQ.data]);
  // Ссылка на несуществующую папку означает корень (так её показывает и дерево).
  const knownFolder = (id: string | null | undefined): string | null =>
    !id || (foldersQ.isSuccess && !paths.some((p) => p.id === id)) ? null : id;
  // Папка, с которой форма открылась: перенос — только если пользователь сменил её в списке.
  // Пока список не трогали, показывается текущая папка из дерева (её могли сменить перетаскиванием).
  const [loadedFolderId] = useState(initial.folderId);
  const [folderTouched, setFolderTouched] = useState(false);
  const folderId = knownFolder(folderTouched ? profile.folderId : initial.folderId);
  const open = useWorkspace((s) => s.open);
  const close = useWorkspace((s) => s.close);
  const closeForConnection = useWorkspace((s) => s.closeForConnection);
  const isNew = initial.id === "";
  const sec = profile.security;

  const update = (patch: Partial<ConnectionProfile>) => setProfile((p) => ({ ...p, ...patch }));
  const current = (): ConnectionProfile => ({ ...profile, folderId, extraProperties: parseProps(propsText) });

  const tlsPasswords = (): TlsPasswordsInput | undefined =>
    "tls" in profile.security
      ? {
          keyPassword: secretValue(tlsSecrets.key),
          keystorePassword: secretValue(tlsSecrets.keystore),
          truststorePassword: secretValue(tlsSecrets.truststore),
        }
      : undefined;

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
      setReport(await api.testConnection(current(), password || undefined, tlsPasswords()));
    });

  const onSave = () =>
    run(async () => {
      const saved = await api.saveConnection(current(), password || undefined, tlsPasswords());
      // save_connection не меняет место существующего подключения в дереве — перенос отдельной командой.
      try {
        if (!isNew && folderTouched && folderId !== knownFolder(loadedFolderId)) {
          await api.moveItems([{ kind: "connection", id: saved.id }], folderId, null);
        }
      } finally {
        // Настройки уже сохранены, даже если перенос не удался: список нужно обновить в любом случае.
        await queryClient.invalidateQueries({ queryKey: ["connections"] });
      }
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
        <h2>{isNew ? t("connections.form.titleNew") : t("connections.form.titleEdit", { name: initial.name })}</h2>
      </div>
      <div className="form">
        <label htmlFor={`${uid}-name`}>{t("connections.form.name")}</label>
        <input id={`${uid}-name`} value={profile.name} onChange={(e) => update({ name: e.target.value })} />

        <label htmlFor={`${uid}-folder`}>{t("connections.form.folder")}</label>
        <select
          id={`${uid}-folder`}
          value={folderId ?? ""}
          onChange={(e) => {
            setFolderTouched(true);
            update({ folderId: e.target.value || null });
          }}
        >
          <option value="">{t("connections.form.folderRoot")}</option>
          {paths.map((p) => (
            <option key={p.id} value={p.id}>
              {p.path.join(" / ")}
            </option>
          ))}
        </select>

        <span className="field-label">
          <label htmlFor={`${uid}-env`}>{t("connections.form.environment")}</label>
          <Hint k="hints.connections.environment" label={t("connections.form.environment")} />
        </span>
        <select
          id={`${uid}-env`}
          value={profile.environment}
          onChange={(e) => {
            const environment = e.target.value as Environment;
            // Новое prod-подключение по умолчанию только для чтения; снять флажок можно сразу.
            update({ environment, readOnly: environment === "prod" && isNew ? true : profile.readOnly });
          }}
        >
          {ENVIRONMENTS.map((env) => (
            <option key={env} value={env}>
              {env}
            </option>
          ))}
        </select>

        <span className="field-label">
          <label htmlFor={`${uid}-ro`}>{t("common.readOnly")}</label>
          <Hint k="hints.connections.readOnly" label={t("common.readOnly")} />
        </span>
        <span>
          <input id={`${uid}-ro`} type="checkbox" checked={profile.readOnly} onChange={(e) => update({ readOnly: e.target.checked })} />
        </span>

        <span className="field-label">
          <span id={`${uid}-llm`}>{t("connections.form.llmAccess")}</span>
          <Hint k="hints.connections.llmAccess" label={t("connections.form.llmAccess")} />
        </span>
        <div role="radiogroup" aria-labelledby={`${uid}-llm`} aria-describedby={prodWarning ? `${uid}-llm-prod` : undefined} className="llm-levels">
          {LLM_LEVELS.map((level) => (
            <label key={level}>
              <input
                type="radio"
                name={`${uid}-llm`}
                checked={(profile.llmAccess ?? "off") === level}
                disabled={level === "messages" && messagesAllowed !== true}
                onChange={() => update({ llmAccess: level })}
              />{" "}
              {t(`connections.form.llmLevel.${level}`)}
            </label>
          ))}
          {messagesAllowed === false && (
            <span className="muted">
              {profile.llmAccess === "messages" ? t("connections.form.llmMessagesAsMetadata") : t("connections.form.llmMessagesPolicy")}
            </span>
          )}
          {messagesAllowed === undefined && mcpStatus.isError && (
            <span className="muted">{t("connections.form.llmStatusUnavailable")}</span>
          )}
          {/* Живая область есть всегда: текст, вписанный в уже существующую область, программа чтения экрана объявит. */}
          <span id={`${uid}-llm-prod`} className="warn" role="status">
            {prodWarning ? t("connections.form.llmProdWarning") : ""}
          </span>
        </div>

        {messagesAllowed === true && profile.llmAccess === "messages" && (
          <>
            {/* Подпись блока, а не флажка: щелчок по ней не должен переключать «Стандартный список» и стирать свой список. */}
            <span className="field-label">
              <span id={`${uid}-mask-label`}>{t("connections.form.mask")}</span>
              <Hint k="hints.connections.mask" label={t("connections.form.mask")} />
            </span>
            <div className="llm-mask" role="group" aria-labelledby={`${uid}-mask-label`} aria-describedby={`${uid}-mask-note`}>
              <label>
                <input
                  id={`${uid}-mask-std`}
                  type="checkbox"
                  checked={profile.llmMaskFields == null}
                  onChange={(e) => {
                    if (e.target.checked) {
                      update({ llmMaskFields: null });
                    } else {
                      setMaskText(STANDARD_MASK_FIELDS.join("\n"));
                      update({ llmMaskFields: [...STANDARD_MASK_FIELDS] });
                    }
                  }}
                />{" "}
                {t("connections.form.maskStandard")}
              </label>
              {profile.llmMaskFields == null ? (
                <code className="muted">{STANDARD_MASK_FIELDS.join(", ")}</code>
              ) : (
                <textarea
                  aria-label={t("connections.form.maskFieldsAria")}
                  rows={6}
                  value={maskText}
                  onChange={(e) => {
                    setMaskText(e.target.value);
                    update({ llmMaskFields: e.target.value.split("\n") });
                  }}
                />
              )}
              <span id={`${uid}-mask-note`} className="muted">
                {t("connections.form.maskNote")}
              </span>
            </div>
          </>
        )}

        <label htmlFor={`${uid}-color`}>{t("connections.form.color")}</label>
        <input id={`${uid}-color`} type="color" value={profile.color} onChange={(e) => update({ color: e.target.value })} />

        <span className="field-label">
          <label htmlFor={`${uid}-bootstrap`}>{t("connections.form.bootstrap")}</label>
          <Hint k="hints.connections.bootstrap" label={t("connections.form.bootstrap")} />
        </span>
        <input
          id={`${uid}-bootstrap`}
          placeholder="host1:9092,host2:9092"
          value={profile.bootstrapServers}
          onChange={(e) => update({ bootstrapServers: e.target.value })}
        />

        <span className="field-label">
          <label htmlFor={`${uid}-protocol`}>{t("connections.form.protocol")}</label>
          <Hint k="hints.connections.protocol" label={t("connections.form.protocol")} />
        </span>
        <select id={`${uid}-protocol`} value={sec.protocol} onChange={(e) => update({ security: withProtocol(sec, e.target.value as Protocol) })}>
          {PROTOCOLS.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>

        {"tls" in sec && (
          <TlsSection
            tls={sec.tls}
            onChange={(tls) => update({ security: { ...sec, tls } })}
            secrets={tlsSecrets}
            onSecrets={setTlsSecrets}
            isNew={isNew}
          />
        )}

        {"sasl" in sec && (
          <>
            <span className="field-label">
              <label htmlFor={`${uid}-mech`}>{t("connections.form.saslMechanism")}</label>
              <Hint k="hints.connections.saslMechanism" label={t("connections.form.saslMechanism")} />
            </span>
            <select
              id={`${uid}-mech`}
              value={sec.sasl.mechanism}
              onChange={(e) => update({ security: { ...sec, sasl: { ...sec.sasl, mechanism: e.target.value as SaslMechanism } } })}
            >
              {MECHANISMS.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
            <label htmlFor={`${uid}-user`}>{t("connections.form.username")}</label>
            <input
              id={`${uid}-user`}
              value={sec.sasl.username}
              onChange={(e) => update({ security: { ...sec, sasl: { ...sec.sasl, username: e.target.value } } })}
            />
            <label htmlFor={`${uid}-pass`}>{t("connections.form.password")}</label>
            <input
              id={`${uid}-pass`}
              type="password"
              autoComplete="off"
              placeholder={isNew ? "" : t("connections.form.passwordKeep")}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </>
        )}

        <span className="field-label">
          <label htmlFor={`${uid}-timeout`}>{t("connections.form.timeout")}</label>
          <Hint k="hints.connections.timeout" label={t("connections.form.timeout")} />
        </span>
        <input
          id={`${uid}-timeout`}
          type="number"
          min={1000}
          step={1000}
          value={profile.requestTimeoutMs}
          onChange={(e) => update({ requestTimeoutMs: Number(e.target.value) })}
        />

        <span className="field-label">
          <label htmlFor={`${uid}-props`}>{t("connections.form.props")}</label>
          <Hint k="hints.connections.props" label={t("connections.form.props")} />
        </span>
        <textarea id={`${uid}-props`} rows={4} placeholder={t("connections.form.propsPlaceholder")} value={propsText} onChange={(e) => setPropsText(e.target.value)} />
      </div>

      <div className="toolbar">
        <button type="button" disabled={busy} onClick={onTest}>
          {t("connections.form.test")}
        </button>
        <Hint k="hints.connections.test" label={t("connections.form.test")} />
        <button type="button" className="primary" disabled={busy} onClick={onSave}>
          {t("common.save")}
        </button>
        {!isNew && (
          confirmingDelete ? (
            <span role="group" aria-label={t("connections.form.deleteGroup")}>
              <strong>{t("connections.form.deleteConfirm", { name: profile.name })}</strong>{" "}
              <button type="button" className="danger" disabled={busy} onClick={onDelete}>
                {t("connections.form.deleteYes")}
              </button>{" "}
              <button type="button" disabled={busy} onClick={() => setConfirmingDelete(false)}>
                {t("common.cancel")}
              </button>
            </span>
          ) : (
            <button type="button" className="danger" disabled={busy} onClick={() => setConfirmingDelete(true)}>
              {t("common.delete")}
            </button>
          )
        )}
        {busy && <span className="muted">{t("connections.form.busy")}</span>}
      </div>

      {error && <ErrorBanner error={error} />}

      {report && (
        <ul className="steps">
          {report.steps.map((s) => (
            <li key={s.name}>
              {s.ok ? "✅" : "❌"} <strong>{t(`connections.form.step.${s.name}`)}:</strong> <span>{tr(s.message)}</span>
              {s.error?.details && <pre className="muted">{s.error.details}</pre>}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
