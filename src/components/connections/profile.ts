import { i18n } from "../../i18n";
import type { CertFormat, ClientCert, ConnectionProfile, Protocol, SaslConfig, SecurityConfig, TlsConfig } from "../../api/types";

export function newProfile(): ConnectionProfile {
  return {
    id: "",
    name: "",
    color: "#3b82f6",
    environment: "local",
    readOnly: false,
    bootstrapServers: "localhost:9092",
    security: { protocol: "PLAINTEXT" },
    extraProperties: {},
    requestTimeoutMs: 30000,
  };
}

const DEFAULT_TLS: TlsConfig = { caLocation: null, caFormat: "pem", verifyHostname: true, clientCert: null };

export function newClientCert(): ClientCert {
  return { format: "pem", certificatePath: "", keyPath: "", alias: null };
}

/** Формат по расширению файла; null — не угадали, пользователь выбирает сам. */
export function formatFromPath(path: string): CertFormat | null {
  const ext = path.toLowerCase().split(/[\\/]/).pop()?.split(".").pop() ?? "";
  if (ext === "p12" || ext === "pfx") return "pkcs12";
  if (ext === "jks" || ext === "keystore" || ext === "truststore") return "jks";
  if (ext === "pem" || ext === "crt" || ext === "cer" || ext === "key") return "pem";
  return null;
}
const DEFAULT_SASL: SaslConfig = { mechanism: "PLAIN", username: "" };

export function withProtocol(sec: SecurityConfig, protocol: Protocol): SecurityConfig {
  const tls = "tls" in sec ? sec.tls : DEFAULT_TLS;
  const sasl = "sasl" in sec ? sec.sasl : DEFAULT_SASL;
  switch (protocol) {
    case "PLAINTEXT":
      return { protocol };
    case "SSL":
      return { protocol, tls };
    case "SASL_PLAINTEXT":
      return { protocol, sasl };
    case "SASL_SSL":
      return { protocol, tls, sasl };
  }
}

/** Формат: одна пара key=value на строку; пустые строки и строки с # пропускаются. */
export function parseProps(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  text.split("\n").forEach((raw, i) => {
    const line = raw.trim();
    if (!line || line.startsWith("#")) return;
    const eq = line.indexOf("=");
    if (eq <= 0) throw new Error(i18n.t("connections.form.propsLineError", { line: i + 1 }));
    out[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  });
  return out;
}

export function formatProps(props: Record<string, string>): string {
  return Object.entries(props)
    .map(([k, v]) => `${k}=${v}`)
    .join("\n");
}
