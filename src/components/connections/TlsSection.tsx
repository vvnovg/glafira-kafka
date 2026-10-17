import { useId, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import type { CertFormat, ClientCert, TlsConfig } from "../../api/types";
import { Hint } from "../common/Hint";
import { formatFromPath, newClientCert } from "./profile";

export type SecretField = { text: string; clear: boolean };
export const emptySecret: SecretField = { text: "", clear: false };
export type TlsSecrets = { key: SecretField; keystore: SecretField; truststore: SecretField };
export const emptyTlsSecrets: TlsSecrets = { key: emptySecret, keystore: emptySecret, truststore: emptySecret };

/** null — не менять сохранённый пароль, "" — удалить, иначе — новый пароль. */
export function secretValue(f: SecretField): string | null {
  if (f.clear) return "";
  return f.text ? f.text : null;
}

const FORMATS: { value: CertFormat; label: string }[] = [
  { value: "pem", label: "PEM" },
  { value: "pkcs12", label: "PKCS#12 (.p12, .pfx)" },
  { value: "jks", label: "JKS" },
];

/** Подпись строки сетки; значок подсказки — рядом с `<label>`, а не внутри (иначе попадёт в доступное имя поля). */
function RowLabel({ id, label, hint }: { id: string; label: string; hint?: ReactNode }) {
  if (!hint) return <label htmlFor={id}>{label}</label>;
  return (
    <span className="field-label">
      <label htmlFor={id}>{label}</label>
      {hint}
    </span>
  );
}

async function pickFile(onPath: (p: string) => void) {
  const path = await openDialog({ multiple: false, directory: false });
  if (typeof path === "string") onPath(path);
}

function PathRow(props: { id: string; label: string; hint?: ReactNode; value: string; placeholder?: string; onChange: (v: string) => void }) {
  const { t } = useTranslation();
  return (
    <>
      <RowLabel id={props.id} label={props.label} hint={props.hint} />
      <div className="path-field">
        <input id={props.id} placeholder={props.placeholder} value={props.value} onChange={(e) => props.onChange(e.target.value)} />
        <button type="button" aria-label={t("connections.tls.pickAria", { label: props.label })} onClick={() => void pickFile(props.onChange)}>
          {t("connections.tls.pick")}
        </button>
      </div>
    </>
  );
}

function FormatRow(props: { id: string; label: string; hint?: ReactNode; value: CertFormat; onChange: (f: CertFormat) => void }) {
  return (
    <>
      <RowLabel id={props.id} label={props.label} hint={props.hint} />
      <select id={props.id} value={props.value} onChange={(e) => props.onChange(e.target.value as CertFormat)}>
        {FORMATS.map((f) => (
          <option key={f.value} value={f.value}>
            {f.label}
          </option>
        ))}
      </select>
    </>
  );
}

function PasswordRow(props: { id: string; label: string; hint?: ReactNode; value: SecretField; isNew: boolean; onChange: (v: SecretField) => void }) {
  const { t } = useTranslation();
  return (
    <>
      <RowLabel id={props.id} label={props.label} hint={props.hint} />
      <div className="path-field">
        <input
          id={props.id}
          type="password"
          autoComplete="off"
          placeholder={props.isNew ? t("connections.tls.noPassword") : t("connections.form.passwordKeep")}
          disabled={props.value.clear}
          value={props.value.text}
          onChange={(e) => props.onChange({ text: e.target.value, clear: false })}
        />
        {!props.isNew && (
          <label className="inline">
            <input
              type="checkbox"
              aria-label={t("connections.tls.clearAria", { label: props.label })}
              checked={props.value.clear}
              onChange={(e) => props.onChange({ text: "", clear: e.target.checked })}
            />{" "}
            {t("connections.tls.clear")}
          </label>
        )}
      </div>
    </>
  );
}

interface Props {
  tls: TlsConfig;
  onChange: (tls: TlsConfig) => void;
  secrets: TlsSecrets;
  onSecrets: (s: TlsSecrets) => void;
  isNew: boolean;
}

/** Строки сетки `.form`: доверенный CA и клиентский сертификат (mTLS). */
export function TlsSection({ tls, onChange, secrets, onSecrets, isNew }: Props) {
  const { t } = useTranslation();
  const uid = useId();
  const set = (patch: Partial<TlsConfig>) => onChange({ ...tls, ...patch });
  const cc = tls.clientCert;
  const setCc = (patch: Partial<ClientCert>) => cc && set({ clientCert: { ...cc, ...patch } });
  const setSecret = (k: keyof TlsSecrets) => (v: SecretField) => onSecrets({ ...secrets, [k]: v });
  const keyPasswordLabel = cc?.format === "jks" ? t("connections.tls.keyPasswordJks") : t("connections.tls.keyPasswordPem");

  return (
    <>
      <PathRow
        id={`${uid}-ca`}
        label={t("connections.tls.ca")}
        hint={<Hint k="hints.connections.ca" label={t("connections.tls.ca")} />}
        placeholder={t("connections.tls.caPlaceholder")}
        value={tls.caLocation ?? ""}
        onChange={(v) => set({ caLocation: v || null, caFormat: formatFromPath(v) ?? tls.caFormat })}
      />
      {tls.caLocation && (
        <>
          <FormatRow
            id={`${uid}-caf`}
            label={t("connections.tls.caFormat")}
            hint={<Hint k="hints.connections.caFormat" label={t("connections.tls.caFormat")} />}
            value={tls.caFormat}
            onChange={(caFormat) => set({ caFormat })}
          />
          {tls.caFormat !== "pem" && (
            <PasswordRow id={`${uid}-tsp`} label={t("connections.tls.truststorePassword")} value={secrets.truststore} isNew={isNew} onChange={setSecret("truststore")} />
          )}
        </>
      )}
      <RowLabel
        id={`${uid}-verify`}
        label={t("connections.tls.verifyHostname")}
        hint={<Hint k="hints.connections.verifyHostname" label={t("connections.tls.verifyHostname")} />}
      />
      <input id={`${uid}-verify`} type="checkbox" checked={tls.verifyHostname} onChange={(e) => set({ verifyHostname: e.target.checked })} />

      <RowLabel
        id={`${uid}-mtls`}
        label={t("connections.tls.mtls")}
        hint={<Hint k="hints.connections.mtls" label={t("connections.tls.mtls")} />}
      />
      <input
        id={`${uid}-mtls`}
        type="checkbox"
        checked={cc !== null}
        onChange={(e) => set({ clientCert: e.target.checked ? newClientCert() : null })}
      />
      {cc && (
        <>
          <PathRow
            id={`${uid}-cert`}
            label={t("connections.tls.cert")}
            placeholder={cc.format === "pem" ? "tls.crt, client.pem" : "client.p12, keystore.jks"}
            value={cc.certificatePath}
            onChange={(v) => setCc({ certificatePath: v, format: formatFromPath(v) ?? cc.format })}
          />
          <FormatRow id={`${uid}-certf`} label={t("connections.tls.certFormat")} value={cc.format} onChange={(format) => setCc({ format })} />
          {cc.format === "pem" && (
            <PathRow id={`${uid}-key`} label={t("connections.tls.key")} placeholder="tls.key, client.key" value={cc.keyPath ?? ""} onChange={(v) => setCc({ keyPath: v })} />
          )}
          {cc.format === "jks" && (
            <>
              <RowLabel
                id={`${uid}-alias`}
                label={t("connections.tls.alias")}
                hint={<Hint k="hints.connections.alias" label={t("connections.tls.alias")} />}
              />
              <input id={`${uid}-alias`} placeholder={t("connections.tls.aliasPlaceholder")} value={cc.alias ?? ""} onChange={(e) => setCc({ alias: e.target.value || null })} />
            </>
          )}
          {cc.format !== "pem" && (
            <PasswordRow id={`${uid}-ksp`} label={t("connections.tls.keystorePassword")} value={secrets.keystore} isNew={isNew} onChange={setSecret("keystore")} />
          )}
          {cc.format !== "pkcs12" && (
            <PasswordRow
              id={`${uid}-kp`}
              label={keyPasswordLabel}
              hint={<Hint k="hints.connections.keyPassword" label={keyPasswordLabel} />}
              value={secrets.key}
              isNew={isNew}
              onChange={setSecret("key")}
            />
          )}
        </>
      )}
    </>
  );
}
