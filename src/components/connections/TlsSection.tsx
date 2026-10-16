import { useId } from "react";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import type { CertFormat, ClientCert, TlsConfig } from "../../api/types";
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

async function pickFile(onPath: (p: string) => void) {
  const path = await openDialog({ multiple: false, directory: false });
  if (typeof path === "string") onPath(path);
}

function PathRow(props: { id: string; label: string; value: string; placeholder?: string; onChange: (v: string) => void }) {
  return (
    <>
      <label htmlFor={props.id}>{props.label}</label>
      <div className="path-field">
        <input id={props.id} placeholder={props.placeholder} value={props.value} onChange={(e) => props.onChange(e.target.value)} />
        <button type="button" aria-label={`Выбрать файл: ${props.label}`} onClick={() => void pickFile(props.onChange)}>
          Выбрать…
        </button>
      </div>
    </>
  );
}

function FormatRow(props: { id: string; label: string; value: CertFormat; onChange: (f: CertFormat) => void }) {
  return (
    <>
      <label htmlFor={props.id}>{props.label}</label>
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

function PasswordRow(props: { id: string; label: string; value: SecretField; isNew: boolean; onChange: (v: SecretField) => void }) {
  return (
    <>
      <label htmlFor={props.id}>{props.label}</label>
      <div className="path-field">
        <input
          id={props.id}
          type="password"
          autoComplete="off"
          placeholder={props.isNew ? "без пароля" : "оставьте пустым, чтобы не менять"}
          disabled={props.value.clear}
          value={props.value.text}
          onChange={(e) => props.onChange({ text: e.target.value, clear: false })}
        />
        {!props.isNew && (
          <label className="inline">
            <input
              type="checkbox"
              aria-label={`Удалить сохранённый: ${props.label}`}
              checked={props.value.clear}
              onChange={(e) => props.onChange({ text: "", clear: e.target.checked })}
            />{" "}
            удалить сохранённый
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
  const uid = useId();
  const set = (patch: Partial<TlsConfig>) => onChange({ ...tls, ...patch });
  const cc = tls.clientCert;
  const setCc = (patch: Partial<ClientCert>) => cc && set({ clientCert: { ...cc, ...patch } });
  const setSecret = (k: keyof TlsSecrets) => (v: SecretField) => onSecrets({ ...secrets, [k]: v });

  return (
    <>
      <PathRow
        id={`${uid}-ca`}
        label="Доверенный CA"
        placeholder="пусто — системные корневые сертификаты"
        value={tls.caLocation ?? ""}
        onChange={(v) => set({ caLocation: v || null, caFormat: formatFromPath(v) ?? tls.caFormat })}
      />
      {tls.caLocation && (
        <>
          <FormatRow id={`${uid}-caf`} label="Формат CA" value={tls.caFormat} onChange={(caFormat) => set({ caFormat })} />
          {tls.caFormat !== "pem" && (
            <PasswordRow id={`${uid}-tsp`} label="Пароль хранилища CA" value={secrets.truststore} isNew={isNew} onChange={setSecret("truststore")} />
          )}
        </>
      )}
      <label htmlFor={`${uid}-verify`}>Проверять hostname</label>
      <input id={`${uid}-verify`} type="checkbox" checked={tls.verifyHostname} onChange={(e) => set({ verifyHostname: e.target.checked })} />

      <label htmlFor={`${uid}-mtls`}>Клиентский сертификат (mTLS)</label>
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
            label="Сертификат клиента"
            placeholder={cc.format === "pem" ? "tls.crt, client.pem" : "client.p12, keystore.jks"}
            value={cc.certificatePath}
            onChange={(v) => setCc({ certificatePath: v, format: formatFromPath(v) ?? cc.format })}
          />
          <FormatRow id={`${uid}-certf`} label="Формат клиентского сертификата" value={cc.format} onChange={(format) => setCc({ format })} />
          {cc.format === "pem" && (
            <PathRow id={`${uid}-key`} label="Ключ клиента" placeholder="tls.key, client.key" value={cc.keyPath ?? ""} onChange={(v) => setCc({ keyPath: v })} />
          )}
          {cc.format === "jks" && (
            <>
              <label htmlFor={`${uid}-alias`}>Alias ключа</label>
              <input id={`${uid}-alias`} placeholder="если ключей несколько" value={cc.alias ?? ""} onChange={(e) => setCc({ alias: e.target.value || null })} />
            </>
          )}
          {cc.format !== "pem" && (
            <PasswordRow id={`${uid}-ksp`} label="Пароль хранилища клиента" value={secrets.keystore} isNew={isNew} onChange={setSecret("keystore")} />
          )}
          {cc.format !== "pkcs12" && (
            <PasswordRow
              id={`${uid}-kp`}
              label={cc.format === "jks" ? "Пароль ключа (если отличается)" : "Пароль ключа (если зашифрован)"}
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
