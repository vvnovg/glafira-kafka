# M3a «mTLS, политика, лицензии» Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Клиентские сертификаты (PEM / PKCS#12 / JKS) в бесплатной версии, корпоративная политика и офлайн-лицензии Free/Pro с ограничением consumer-сессий и режима prod.

**Architecture:** Новый модуль `tls` переводит сертификаты любого формата в PEM в памяти и передаёт их librdkafka (`ssl.*.pem`), пароли лежат в keychain. Модуль `policy` один раз при запуске читает политику из источника ОС (plist / реестр / `/etc`). Лицензия — подписанный Ed25519 токен. Формат токена вынесен в отдельный crate `crates/license-format`, общий для приложения и утилиты `tools/issue-license`. Проверка Pro выполняется в Rust (`require_pro`), интерфейс только показывает замки.

**Tech Stack:** Rust (Tauri 2, rdkafka 0.37, `openssl` 0.10 vendored, `ed25519-dalek` 2, `plist`, `winreg`), React 19 + TanStack Query, vitest, GitHub Actions.

**Спецификация:** `docs/superpowers/specs/2026-10-16-kafka-explorer-m3-distribution-design.md` (§2–§5). Второй план, M3b «Релиз», реализует §6.

## Global Constraints

- Ошибки никогда не превращаются в 0 или в успех: неизвестное значение — `Option` / «—» с причиной в title.
- Изменяющие команды сначала проходят guard (`AppState::guarded`): read-only → `READ_ONLY`; prod требует `confirm` == имя подключения → `CONFIRMATION_REQUIRED`. **Guard от лицензии не зависит.**
- Блокирующие вызовы librdkafka, keychain и файловой системы не выполняются в async-рантайме: только через `commands::blocking` или в отдельном потоке.
- Подтверждения показываются только внутри страницы (`window.confirm` в Tauri 2 не работает). У каждого поля формы есть видимая подпись.
- Секреты (пароли, ключи, содержимое сертификатов, лицензионный ключ) не попадают в `connections.json`, логи и доп. свойства.
- mTLS — функция **Free**. Pro: consumer-сессии (`start_consumer`, `commit_offsets`, `pause_partitions`, `resume_partitions`) и перевод профиля в окружение `prod` (новый prod-профиль или смена окружения на `prod`). Pro-элементы видны с замком и подсказкой «Доступно в Pro».
- Понижение тарифа не ослабляет безопасность: существующие prod-профили сохраняют подтверждения.
- Код ошибки без лицензии — `PRO_REQUIRED`, не повторяемый (`retryable: false`).
- Все пароли TLS необязательны. Пустой пароль допустим для PEM, PKCS#12 и JKS.
- Файлы сертификатов читаются при каждом создании клиента: после ротации подхватывается новый файл. Временных файлов нет.
- Пути к сертификатам — абсолютные.
- Ключ политики `updatesEnabled`: если источник политики существует, но не разбирается, значение `false` (безопасно закрыто).
- Срок предупреждения о лицензии — 14 дней. Дата `expires` включительна: в сам день окончания лицензия ещё действует.
- Коммиты заканчиваются пустой строкой и `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Push — только с согласия пользователя (его просит контроллер).
- Окружение: `source "$HOME/.cargo/env"` перед cargo. Локальная Kafka: `docker compose -f dev/docker-compose.yml up -d`. Интеграционные тесты: `KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 KE_IT_MTLS_BOOTSTRAP=localhost:9095`.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `.github/workflows/ci.yml` | CI: Linux (полный прогон с Kafka) и проверка сборки на macOS/Windows |
| `dev/certs/generate.sh`, `dev/certs/out/*` | тестовые CA и сертификаты (закоммичены, только для разработки) |
| `dev/docker-compose.yml` | + mTLS-листенер `localhost:9095` |
| `src-tauri/src/tls/mod.rs` | загрузка CA и клиентской пары → PEM, проверки и тексты ошибок |
| `src-tauri/src/tls/jks.rs` | разбор JKS и расшифровка ключа (Sun KeyProtector) |
| `src-tauri/src/tls/der.rs` | минимальный разбор DER (TLV) |
| `src-tauri/src/storage/connections.rs` | `TlsConfig` + `CertFormat`, `ClientCert`; валидация |
| `src-tauri/src/kafka/config.rs` | `apply_tls` через `tls::*`; имена секретов TLS |
| `src-tauri/src/secrets.rs` | `OverlayStore` (несохранённые пароли поверх keychain), список полей секретов |
| `src-tauri/src/commands/connections.rs` | пароли TLS при сохранении и проверке, Pro-проверка окружения prod |
| `src-tauri/src/error.rs` | `PRO_REQUIRED`, понятные сообщения TLS |
| `src-tauri/src/product.rs` | идентификатор, вендор, имя сервиса keychain — в одном месте |
| `src-tauri/src/policy/mod.rs`, `policy/sources.rs` | разбор и источники политики |
| `crates/license-format/` | формат токена, подпись и проверка, dev-ключи (feature `dev-keys`) |
| `src-tauri/license-public.key` | открытый ключ лицензий, вшивается в приложение |
| `src-tauri/src/license/mod.rs` | `LicenseManager`: источник, хранение, статус, `require_pro` |
| `src-tauri/src/commands/license.rs` | команды лицензии и политики |
| `tools/issue-license/` | утилита владельца: keygen / issue / verify |
| `src/components/connections/TlsSection.tsx` | блоки «Доверенный CA» и «Клиентский сертификат» |
| `src/lib/useLicense.ts`, `src/components/common/ProLock.tsx` | статус лицензии и замок |
| `src/components/license/LicensePanel.tsx` | экран «Лицензия» |
| `docs/licensing.md`, `README.md` | выдача лицензий; mTLS и Free/Pro |

---

### Task 1: CI на трёх ОС

Главный технический риск этапа — сборка rdkafka (cmake + vendored OpenSSL) на Windows. Поэтому CI появляется первым: так сразу видно, собирается ли проект на всех платформах.

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `scripts/wait-kafka.sh`

**Interfaces:**
- Produces: workflow `ci` с jobs `linux` (каждый push в `main`, PR и ручной запуск) и `platforms` (только ручной запуск `workflow_dispatch` и теги `v*`). Последующие задачи добавляют в job `linux` переменную `KE_IT_MTLS_BOOTSTRAP` (Task 2).

Минуты GitHub Actions в приватном репозитории тарифицируются с множителем: macOS ×10, Windows ×2. Поэтому macOS и Windows собираются только по ручному запуску и на тегах, а каждый push проверяется на Linux.

- [ ] **Step 1: Скрипт ожидания Kafka** — создать `scripts/wait-kafka.sh` (исполняемый):

```bash
#!/usr/bin/env bash
# Ждёт готовности dev-стенда: брокер отвечает и одноразовая инициализация SCRAM завершилась.
set -euo pipefail
for _ in $(seq 1 60); do
  if docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null
code=$(docker wait ke-kafka-init)
if [ "$code" != "0" ]; then
  docker logs ke-kafka-init
  exit 1
fi
```

Run: `chmod +x scripts/wait-kafka.sh && docker compose -f dev/docker-compose.yml up -d && scripts/wait-kafka.sh; echo $?`
Expected: `0`.

- [ ] **Step 2: Workflow** — создать `.github/workflows/ci.yml`:

```yaml
name: ci

on:
  push:
    branches: [main]
    tags: ["v*"]
  pull_request:
  workflow_dispatch:

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  linux:
    runs-on: ubuntu-22.04
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with:
          version: 11
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: pnpm
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: src-tauri -> target
      - name: Системные зависимости Tauri
        run: |
          sudo apt-get update
          sudo apt-get install -y libwebkit2gtk-4.1-dev libappindicator3-dev librsvg2-dev patchelf libsecret-1-dev cmake
      - run: pnpm install --frozen-lockfile
      - run: pnpm test
      - run: pnpm build
      - run: cargo fmt --manifest-path src-tauri/Cargo.toml --check
      - run: cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
      - name: Kafka
        run: |
          docker compose -f dev/docker-compose.yml up -d
          scripts/wait-kafka.sh
      - name: Тесты Rust (unit + интеграционные)
        run: cargo test --manifest-path src-tauri/Cargo.toml --lib --tests
        env:
          KE_IT_BOOTSTRAP: localhost:9092
          KE_IT_SASL_BOOTSTRAP: localhost:9094

  platforms:
    if: github.event_name == 'workflow_dispatch' || startsWith(github.ref, 'refs/tags/')
    strategy:
      fail-fast: false
      matrix:
        os: [macos-14, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with:
          version: 11
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: pnpm
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: src-tauri -> target
      - run: pnpm install --frozen-lockfile
      - run: pnpm test
      - run: pnpm build
      - name: Unit-тесты Rust
        run: cargo test --manifest-path src-tauri/Cargo.toml --lib
```

- [ ] **Step 3: Локальная проверка синтаксиса**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml scripts/wait-kafka.sh
git commit -m "ci: Linux с Kafka на каждый push, macOS и Windows по запросу

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Прогон в GitHub (через контроллера)** — **не пушить самостоятельно.** В отчёте попросить контроллера:
  1. получить согласие пользователя на push ветки;
  2. выполнить `git push -u origin <ветка>` и `gh workflow run ci --ref <ветка>`;
  3. дождаться результата (`gh run watch`).

Expected: jobs `linux`, `platforms (macos-14)` и `platforms (windows-latest)` зелёные. Если Windows падает на сборке rdkafka-sys, контроллер передаёт лог в отдельную задачу-исправление. Варианты исправления:
  - `choco install nasm` перед сборкой (сборка OpenSSL);
  - `CMAKE_GENERATOR: "Visual Studio 17 2022"`;
  - на крайний случай фича `dynamic-linking` с OpenSSL из vcpkg только для Windows.

Остальные задачи плана от исхода не зависят и могут идти параллельно с починкой Windows.

---

### Task 2: Тестовые сертификаты и mTLS-листенер dev-стенда

**Files:**
- Create: `dev/certs/generate.sh`, `dev/certs/README.md`
- Create (результат скрипта, коммитится): `dev/certs/out/*`
- Modify: `dev/docker-compose.yml`
- Modify: `src-tauri/tests/common/mod.rs` (`mtls_bootstrap()`, `cert_path()`)
- Modify: `.github/workflows/ci.yml` (переменная `KE_IT_MTLS_BOOTSTRAP`)

**Interfaces:**
- Produces: файлы в `dev/certs/out/` (пароль хранилищ — `storepass`, пароль отдельного ключа — `keypass`):
  - `ca.pem`, `ca.key` — тестовый CA;
  - `client.pem` (сертификат клиента), `client.key` (ключ без пароля), `client-enc.key` (PKCS#8, зашифрован паролем `keypass`);
  - `client.p12` (пароль `storepass`), `client-nopass.p12` (пустой пароль);
  - `client.jks` (хранилище и ключ — `storepass`), `client-keypass.jks` (хранилище `storepass`, ключ `keypass`), alias `client`;
  - `truststore.p12`, `truststore.jks` (пароль `storepass`, запись `ca`);
  - `broker.p12`, `broker_creds` — для брокера.
- Produces: брокер слушает `SSL` с обязательным клиентским сертификатом на `localhost:9095`.
- Produces: `common::mtls_bootstrap() -> Option<String>` (переменная `KE_IT_MTLS_BOOTSTRAP`) и `common::cert_path(name: &str) -> String` (абсолютный путь к `dev/certs/out/<name>`).

- [ ] **Step 1: Скрипт** — создать `dev/certs/generate.sh` (исполняемый):

```bash
#!/usr/bin/env bash
# Тестовые сертификаты для mTLS-листенера dev-стенда и тестов. ТОЛЬКО ДЛЯ РАЗРАБОТКИ.
# Требует openssl (OpenSSL или LibreSSL) и docker (keytool берётся из образа Kafka).
set -euo pipefail
cd "$(dirname "$0")"
OUT=out
P=storepass
DAYS=3650
rm -rf "$OUT" && mkdir -p "$OUT"

openssl req -x509 -newkey rsa:2048 -nodes -days $DAYS -subj "/CN=ke-dev-ca" \
  -keyout $OUT/ca.key -out $OUT/ca.pem

# Брокер: имя localhost / kafka / 127.0.0.1
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout $OUT/broker.key -out $OUT/broker.csr
printf "subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1\n" > $OUT/broker.ext
openssl x509 -req -in $OUT/broker.csr -CA $OUT/ca.pem -CAkey $OUT/ca.key -CAcreateserial \
  -days $DAYS -extfile $OUT/broker.ext -out $OUT/broker.pem
openssl pkcs12 -export -name broker -in $OUT/broker.pem -inkey $OUT/broker.key \
  -certfile $OUT/ca.pem -passout pass:$P -out $OUT/broker.p12
printf "%s" "$P" > $OUT/broker_creds

# Клиент
openssl req -newkey rsa:2048 -nodes -subj "/CN=ke-client" -keyout $OUT/client.key -out $OUT/client.csr
printf "extendedKeyUsage=clientAuth\n" > $OUT/client.ext
openssl x509 -req -in $OUT/client.csr -CA $OUT/ca.pem -CAkey $OUT/ca.key -CAcreateserial \
  -days $DAYS -extfile $OUT/client.ext -out $OUT/client.pem
openssl pkcs8 -topk8 -v2 aes-256-cbc -in $OUT/client.key -passout pass:keypass -out $OUT/client-enc.key
openssl pkcs12 -export -name client -in $OUT/client.pem -inkey $OUT/client.key \
  -certfile $OUT/ca.pem -passout pass:$P -out $OUT/client.p12
openssl pkcs12 -export -name client -in $OUT/client.pem -inkey $OUT/client.key \
  -certfile $OUT/ca.pem -passout pass: -out $OUT/client-nopass.p12

# keytool (Java) — из образа Kafka
KT=(docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/$OUT:/out" -w /out --entrypoint keytool confluentinc/cp-kafka:7.6.1)
"${KT[@]}" -importcert -noprompt -alias ca -file ca.pem -keystore truststore.p12 -storetype PKCS12 -storepass $P
"${KT[@]}" -importcert -noprompt -alias ca -file ca.pem -keystore truststore.jks -storetype JKS -storepass $P
"${KT[@]}" -importkeystore -noprompt -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass $P \
  -srcalias client -destalias client -destkeystore client.jks -deststoretype JKS -deststorepass $P -destkeypass $P
"${KT[@]}" -importkeystore -noprompt -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass $P \
  -srcalias client -destalias client -destkeystore client-keypass.jks -deststoretype JKS -deststorepass $P -destkeypass keypass

rm -f $OUT/*.csr $OUT/*.ext $OUT/*.srl
echo "Готово: $(ls $OUT | tr '\n' ' ')"
```

Run: `chmod +x dev/certs/generate.sh && dev/certs/generate.sh`
Expected: в конце строка `Готово:` со всеми файлами из списка Interfaces. Проверка: `file dev/certs/out/client.jks` → `Java KeyStore`.

- [ ] **Step 2: README каталога** — `dev/certs/README.md`:

```markdown
# Тестовые сертификаты (только для разработки)

Создаются `./generate.sh` и закоммичены, чтобы тесты и dev-стенд работали без шага генерации.
Ключ CA `ca.key` — тестовый, публичный, нигде кроме dev-стенда не использовать.

Пароли: хранилища — `storepass`, отдельный ключ (`client-enc.key`, `client-keypass.jks`) — `keypass`.
Срок действия — 10 лет; после истечения перезапустите `./generate.sh` и закоммитьте `out/`.
```

- [ ] **Step 3: Листенер в docker-compose** — в `dev/docker-compose.yml` у сервиса `kafka`:
  - в `ports` добавить `- "9095:9095"`;
  - добавить `volumes`:
    ```yaml
    volumes:
      - ./certs/out:/etc/kafka/secrets:ro
    ```
  - заменить три переменные и добавить SSL-настройки:
    ```yaml
      KAFKA_LISTENERS: INTERNAL://:19092,CONTROLLER://:9093,EXTERNAL://:9092,SASL_EXT://:9094,MTLS_EXT://:9095
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092,SASL_EXT://localhost:9094,MTLS_EXT://localhost:9095
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,SASL_EXT:SASL_PLAINTEXT,MTLS_EXT:SSL
      # mTLS: файлы из dev/certs/out (см. dev/certs/README.md)
      KAFKA_SSL_KEYSTORE_FILENAME: broker.p12
      KAFKA_SSL_KEYSTORE_TYPE: PKCS12
      KAFKA_SSL_KEYSTORE_CREDENTIALS: broker_creds
      KAFKA_SSL_KEY_CREDENTIALS: broker_creds
      KAFKA_SSL_TRUSTSTORE_FILENAME: truststore.p12
      KAFKA_SSL_TRUSTSTORE_TYPE: PKCS12
      KAFKA_SSL_TRUSTSTORE_CREDENTIALS: broker_creds
      KAFKA_SSL_CLIENT_AUTH: required
      KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: ""
    ```

Run:
```bash
docker compose -f dev/docker-compose.yml up -d --force-recreate kafka && scripts/wait-kafka.sh
openssl s_client -connect localhost:9095 -cert dev/certs/out/client.pem -key dev/certs/out/client.key -CAfile dev/certs/out/ca.pem </dev/null 2>/dev/null | grep "Verify return code"
```
Expected: `Verify return code: 0 (ok)`. Без `-cert/-key` брокер разрывает соединение: `docker logs ke-kafka 2>&1 | tail` содержит `SSL handshake failed` или `certificate`. Это нормально.

- [ ] **Step 4: Хелперы IT** — в `src-tauri/tests/common/mod.rs` после `sasl_bootstrap`:

```rust
pub fn mtls_bootstrap() -> Option<String> {
    let v = std::env::var("KE_IT_MTLS_BOOTSTRAP").ok();
    if v.is_none() {
        eprintln!("skip: KE_IT_MTLS_BOOTSTRAP не задан");
    }
    v
}

/// Абсолютный путь к тестовому сертификату из dev/certs/out.
pub fn cert_path(name: &str) -> String {
    format!("{}/../dev/certs/out/{name}", env!("CARGO_MANIFEST_DIR"))
}
```

- [ ] **Step 5: CI** — в `.github/workflows/ci.yml`, job `linux`, шаг «Тесты Rust», в `env` добавить `KE_IT_MTLS_BOOTSTRAP: localhost:9095`.

- [ ] **Step 6: Проверка, что старые тесты не сломались**

Run: `source "$HOME/.cargo/env" && KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --lib --tests 2>&1 | grep -E "^test result" | awk '{p+=$4; f+=$6} END {print p, f}'`
Expected: `249 0` (или больше прошедших, 0 упавших).

- [ ] **Step 7: Commit**

```bash
git add dev/certs dev/docker-compose.yml src-tauri/tests/common/mod.rs .github/workflows/ci.yml
git commit -m "dev: тестовые сертификаты и mTLS-листенер localhost:9095

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Модель сертификатов и загрузка PEM / PKCS#12

**Files:**
- Modify: `src-tauri/Cargo.toml` (`openssl = { version = "0.10", features = ["vendored"] }`)
- Modify: `src-tauri/src/storage/connections.rs` (`CertFormat`, `ClientCert`, поля `TlsConfig`, валидация)
- Create: `src-tauri/src/tls/mod.rs`
- Create: `src-tauri/src/tls/jks.rs` (заглушка `is_jks`; полный разбор — Task 4)
- Modify: `src-tauri/src/lib.rs` (`pub mod tls;`)

**Interfaces:**
- Produces (в `storage::connections`):
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
  #[serde(rename_all = "lowercase")]
  pub enum CertFormat { #[default] Pem, Pkcs12, Jks }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  #[serde(rename_all = "camelCase")]
  pub struct ClientCert {
      pub format: CertFormat,
      pub certificate_path: String,
      #[serde(default)] pub key_path: Option<String>,
      #[serde(default)] pub alias: Option<String>,
  }

  pub struct TlsConfig {
      #[serde(default)] pub ca_location: Option<String>,
      #[serde(default)] pub ca_format: CertFormat,
      #[serde(default = "default_true")] pub verify_hostname: bool,
      #[serde(default)] pub client_cert: Option<ClientCert>,
  }
  ```
  JSON: `{"caLocation": null, "caFormat": "pem", "verifyHostname": true, "clientCert": {"format": "jks", "certificatePath": "/a.jks", "keyPath": null, "alias": "client"}}`.
- Produces (в `tls`):
  ```rust
  pub struct ClientIdentity { pub certificate_pem: String, pub key_pem: String }
  pub fn load_ca(format: CertFormat, path: &str, password: Option<&str>) -> AppResult<String>;
  pub fn load_identity(cert: &ClientCert, key_password: Option<&str>, store_password: Option<&str>) -> AppResult<ClientIdentity>;
  ```
  Все ошибки — `INVALID_INPUT` с текстом для пользователя, исходная ошибка OpenSSL — в `details`.

- [ ] **Step 1: Зависимость** — в `src-tauri/Cargo.toml` в `[dependencies]` добавить:

```toml
openssl = { version = "0.10", features = ["vendored"] }
```

(rdkafka с `ssl-vendored` уже тянет `openssl-sys` с vendored OpenSSL 3. Тот же экземпляр используется и здесь.)

- [ ] **Step 2: Модель профиля — тесты** — в `mod tests` файла `storage/connections.rs`:

```rust
    #[test]
    fn old_tls_profiles_get_defaults() {
        let tls: TlsConfig =
            serde_json::from_str(r#"{"caLocation":"/ca.pem","verifyHostname":false}"#).unwrap();
        assert_eq!(tls.ca_format, CertFormat::Pem);
        assert_eq!(tls.client_cert, None);
        assert!(!tls.verify_hostname);
    }

    #[test]
    fn client_cert_json_shape() {
        let tls = TlsConfig {
            ca_location: None,
            ca_format: CertFormat::Jks,
            verify_hostname: true,
            client_cert: Some(ClientCert {
                format: CertFormat::Pkcs12,
                certificate_path: "/c.p12".into(),
                key_path: None,
                alias: None,
            }),
        };
        let v = serde_json::to_value(&tls).unwrap();
        assert_eq!(v["caFormat"], "jks");
        assert_eq!(v["clientCert"]["format"], "pkcs12");
        assert_eq!(v["clientCert"]["certificatePath"], "/c.p12");
    }

    fn tls_profile(tls: TlsConfig) -> ConnectionProfile {
        let mut p = sample_profile("a");
        p.security = SecurityConfig::Ssl { tls };
        p
    }

    #[test]
    fn validation_of_certificate_paths() {
        let base = TlsConfig {
            ca_location: None,
            ca_format: CertFormat::Pem,
            verify_hostname: true,
            client_cert: None,
        };
        let pem = |cert: &str, key: Option<&str>| TlsConfig {
            client_cert: Some(ClientCert {
                format: CertFormat::Pem,
                certificate_path: cert.into(),
                key_path: key.map(String::from),
                alias: None,
            }),
            ..base.clone()
        };
        let abs = if cfg!(windows) { "C:\\c\\c.pem" } else { "/c/c.pem" };
        assert!(validate_profile(&tls_profile(pem(abs, Some(abs)))).is_ok());
        for (tls, needle) in [
            (pem("", Some(abs)), "сертификат"),
            (pem("rel/c.pem", Some(abs)), "абсолютный"),
            (pem(abs, None), "ключ"),
            (
                TlsConfig { ca_location: Some("ca.pem".into()), ..base.clone() },
                "абсолютный",
            ),
        ] {
            let e = validate_profile(&tls_profile(tls)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput);
            assert!(e.message.to_lowercase().contains(needle), "{}", e.message);
        }
    }

    #[test]
    fn private_key_material_is_rejected_in_extra_properties() {
        for key in ["ssl.key.pem", "SSL.KEYSTORE.KEY"] {
            let mut p = sample_profile("a");
            p.extra_properties.insert(key.into(), "x".into());
            assert_eq!(validate_profile(&p).unwrap_err().code, ErrorCode::InvalidInput, "{key}");
        }
    }
```

В существующем тесте `persists_and_reloads` конструктор `TlsConfig { ca_location, verify_hostname }` дополнить полями `ca_format: CertFormat::Pem, client_cert: None`. Аналогичные конструкторы есть в `kafka/config.rs` (тесты) и в интеграционных тестах; `grep -rn "TlsConfig {" src-tauri` найдёт все места.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib storage::connections`
Expected: FAIL — нет `CertFormat`.

- [ ] **Step 3: Модель профиля — реализация** — в `storage/connections.rs` добавить типы из Interfaces, расширить `TlsConfig` и валидацию:

```rust
fn is_secret_property(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k.contains("password")
        || k.contains("secret")
        || k.contains("sasl.jaas.config")
        || k == "ssl.key.pem"
        || k == "ssl.keystore.key"
}

fn validate_path(path: &str, what: &str) -> AppResult<()> {
    if path.trim().is_empty() {
        return Err(AppError::invalid(format!("Укажите файл: {what}")));
    }
    if !std::path::Path::new(path.trim()).is_absolute() {
        return Err(AppError::invalid(format!(
            "{what}: укажите абсолютный путь (сейчас «{path}»)"
        )));
    }
    Ok(())
}

fn validate_tls(tls: &TlsConfig) -> AppResult<()> {
    if let Some(ca) = tls.ca_location.as_deref().filter(|s| !s.trim().is_empty()) {
        validate_path(ca, "доверенный CA")?;
    }
    if let Some(cc) = &tls.client_cert {
        validate_path(&cc.certificate_path, "клиентский сертификат")?;
        if cc.format == CertFormat::Pem {
            match cc.key_path.as_deref() {
                Some(k) if !k.trim().is_empty() => validate_path(k, "ключ клиента")?,
                _ => return Err(AppError::invalid("Укажите файл: ключ клиента (PEM)")),
            }
        }
    }
    Ok(())
}
```

В `validate_profile` после проверки SASL:

```rust
    if let SecurityConfig::Ssl { tls } | SecurityConfig::SaslSsl { tls, .. } = &p.security {
        validate_tls(tls)?;
    }
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib storage::connections`
Expected: PASS.

- [ ] **Step 4: Тесты `tls`** — создать `src-tauri/src/tls/mod.rs` пока только с тестами (реализация — Step 5). Фикстуры — из `dev/certs/out` (Task 2). Сертификаты с нужными сроками генерируются в тесте:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use openssl::asn1::Asn1Time;
    use openssl::ec::{EcGroup, EcKey};
    use openssl::hash::MessageDigest;
    use openssl::nid::Nid;
    use openssl::pkcs12::Pkcs12;
    use openssl::x509::{X509Builder, X509NameBuilder};

    fn fixture(name: &str) -> String {
        format!("{}/../dev/certs/out/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    fn pem_cert(format: CertFormat, cert: &str, key: Option<&str>) -> ClientCert {
        ClientCert {
            format,
            certificate_path: fixture(cert),
            key_path: key.map(fixture),
            alias: None,
        }
    }

    /// Самоподписанный сертификат со сроком [now+from_days, now+to_days].
    fn self_signed(from_days: i64, to_days: i64) -> (PKey<Private>, X509) {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "t").unwrap();
        let name = name.build();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut b = X509Builder::new().unwrap();
        b.set_version(2).unwrap();
        b.set_subject_name(&name).unwrap();
        b.set_issuer_name(&name).unwrap();
        b.set_pubkey(&key).unwrap();
        b.set_not_before(&Asn1Time::from_unix(now + from_days * 86_400).unwrap()).unwrap();
        b.set_not_after(&Asn1Time::from_unix(now + to_days * 86_400).unwrap()).unwrap();
        b.sign(&key, MessageDigest::sha256()).unwrap();
        (key, b.build())
    }

    fn write_pair(dir: &std::path::Path, key: &PKey<Private>, cert: &X509) -> ClientCert {
        let c = dir.join("c.pem");
        let k = dir.join("k.pem");
        std::fs::write(&c, cert.to_pem().unwrap()).unwrap();
        std::fs::write(&k, key.private_key_to_pem_pkcs8().unwrap()).unwrap();
        ClientCert {
            format: CertFormat::Pem,
            certificate_path: c.to_string_lossy().into(),
            key_path: Some(k.to_string_lossy().into()),
            alias: None,
        }
    }

    #[test]
    fn pem_identity_without_password() {
        let id = load_identity(&pem_cert(CertFormat::Pem, "client.pem", Some("client.key")), None, None).unwrap();
        assert!(id.certificate_pem.contains("BEGIN CERTIFICATE"));
        assert!(id.key_pem.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn encrypted_pem_key_needs_password() {
        let cc = pem_cert(CertFormat::Pem, "client.pem", Some("client-enc.key"));
        let e = load_identity(&cc, None, None).unwrap_err();
        assert!(e.message.contains("зашифрован"), "{}", e.message);
        let e = load_identity(&cc, Some("wrong"), None).unwrap_err();
        assert!(e.message.contains("неверный пароль"), "{}", e.message);
        let id = load_identity(&cc, Some("keypass"), None).unwrap();
        assert!(!id.key_pem.contains("ENCRYPTED"), "в librdkafka уходит расшифрованный ключ");
    }

    #[test]
    fn pkcs12_identity_with_and_without_password() {
        let id = load_identity(&pem_cert(CertFormat::Pkcs12, "client.p12", None), None, Some("storepass")).unwrap();
        assert!(id.certificate_pem.contains("BEGIN CERTIFICATE"));
        let e = load_identity(&pem_cert(CertFormat::Pkcs12, "client.p12", None), None, Some("nope")).unwrap_err();
        assert!(e.message.contains("неверный пароль"), "{}", e.message);
        load_identity(&pem_cert(CertFormat::Pkcs12, "client-nopass.p12", None), None, None).unwrap();
    }

    #[test]
    fn modern_aes_pkcs12_is_supported() {
        let dir = tempfile::tempdir().unwrap();
        let (key, cert) = self_signed(-1, 30);
        let p12 = Pkcs12::builder().name("x").pkey(&key).cert(&cert).build2("pw").unwrap();
        let path = dir.path().join("a.p12");
        std::fs::write(&path, p12.to_der().unwrap()).unwrap();
        let cc = ClientCert {
            format: CertFormat::Pkcs12,
            certificate_path: path.to_string_lossy().into(),
            key_path: None,
            alias: None,
        };
        load_identity(&cc, None, Some("pw")).unwrap();
    }

    #[test]
    fn key_must_match_certificate() {
        let dir = tempfile::tempdir().unwrap();
        let (_, cert) = self_signed(-1, 30);
        let (other_key, _) = self_signed(-1, 30);
        let cc = write_pair(dir.path(), &other_key, &cert);
        let e = load_identity(&cc, None, None).unwrap_err();
        assert!(e.message.contains("не соответствует"), "{}", e.message);
    }

    #[test]
    fn expired_and_not_yet_valid_certificates() {
        let dir = tempfile::tempdir().unwrap();
        let (key, cert) = self_signed(-30, -1);
        let e = load_identity(&write_pair(dir.path(), &key, &cert), None, None).unwrap_err();
        assert!(e.message.contains("истёк"), "{}", e.message);
        let (key, cert) = self_signed(2, 30);
        let e = load_identity(&write_pair(dir.path(), &key, &cert), None, None).unwrap_err();
        assert!(e.message.contains("ещё не действует"), "{}", e.message);
    }

    #[test]
    fn missing_file_names_the_path() {
        let cc = pem_cert(CertFormat::Pem, "nope.pem", Some("client.key"));
        let e = load_identity(&cc, None, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("nope.pem"), "{}", e.message);
    }

    #[test]
    fn ca_from_pem_pkcs12_and_jks_truststores() {
        for (format, file, pw) in [
            (CertFormat::Pem, "ca.pem", None),
            (CertFormat::Pkcs12, "truststore.p12", Some("storepass")),
        ] {
            let pem = load_ca(format, &fixture(file), pw).unwrap();
            assert!(pem.contains("BEGIN CERTIFICATE"), "{file}");
        }
        let e = load_ca(CertFormat::Pem, &fixture("client.key"), None).unwrap_err();
        assert!(e.message.contains("сертификат"), "{}", e.message);
    }
}
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib tls::`
Expected: FAIL — нет реализации.

- [ ] **Step 5: Реализация `tls`** — в начало `src-tauri/src/tls/mod.rs`:

```rust
//! Сертификаты TLS → PEM в памяти для librdkafka (`ssl.ca.pem`, `ssl.certificate.pem`, `ssl.key.pem`).
//! Файлы читаются при каждом создании клиента, чтобы подхватывать ротацию; временных файлов нет.

mod der;
pub mod jks;

use std::cmp::Ordering;
use std::path::Path;
use std::sync::OnceLock;

use openssl::asn1::Asn1Time;
use openssl::error::ErrorStack;
use openssl::pkcs12::{ParsedPkcs12_2, Pkcs12};
use openssl::pkey::{PKey, Private};
use openssl::provider::Provider;
use openssl::x509::X509;

use crate::error::{AppError, AppResult};
#[cfg(test)]
use crate::error::ErrorCode;
use crate::storage::connections::{CertFormat, ClientCert};

pub struct ClientIdentity {
    pub certificate_pem: String,
    pub key_pem: String,
}

const CLIENT: &str = "Клиентский сертификат";
const KEY: &str = "Ключ клиента";
const CA: &str = "Доверенный CA";

fn read_file(path: &str, what: &str) -> AppResult<Vec<u8>> {
    let p = Path::new(path.trim());
    if !p.is_absolute() {
        return Err(AppError::invalid(format!("{what}: укажите абсолютный путь (сейчас «{path}»)")));
    }
    std::fs::read(p).map_err(|e| AppError::invalid(format!("{what}: не удалось прочитать «{path}»: {e}")))
}

fn ssl_err(message: String, e: ErrorStack) -> AppError {
    AppError::invalid(message).with_details(e.to_string())
}

fn non_empty(p: Option<&str>) -> Option<&str> {
    p.filter(|s| !s.is_empty())
}

/// Доверенный CA в PEM (цепочка сертификатов).
pub fn load_ca(format: CertFormat, path: &str, password: Option<&str>) -> AppResult<String> {
    let data = read_file(path, CA)?;
    let certs: Vec<X509> = match format {
        CertFormat::Pem => X509::stack_from_pem(&data)
            .map_err(|e| ssl_err(format!("{CA}: «{path}» не содержит PEM-сертификатов"), e))?,
        CertFormat::Jks if jks::is_jks(&data) => jks::trusted_certificates(&data, password)?,
        // Начиная с Java 9 keytool по умолчанию создаёт PKCS#12, даже если файл назван .jks.
        CertFormat::Pkcs12 | CertFormat::Jks => {
            let p = parse_pkcs12(&data, password, CA)?;
            p.cert.into_iter().chain(p.ca.into_iter().flatten()).collect()
        }
    };
    if certs.is_empty() {
        return Err(AppError::invalid(format!("{CA}: в «{path}» нет сертификатов")));
    }
    to_pem_chain(&certs)
}

/// Клиентский сертификат и ключ в PEM. Проверяет пароль, соответствие ключа сертификату и срок действия.
pub fn load_identity(
    cert: &ClientCert,
    key_password: Option<&str>,
    store_password: Option<&str>,
) -> AppResult<ClientIdentity> {
    let (key, chain) = match cert.format {
        CertFormat::Pem => {
            let chain = X509::stack_from_pem(&read_file(&cert.certificate_path, CLIENT)?).map_err(|e| {
                ssl_err(format!("{CLIENT}: «{}» не содержит PEM-сертификатов", cert.certificate_path), e)
            })?;
            let key_path = cert
                .key_path
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| AppError::invalid(format!("{KEY}: укажите файл ключа")))?;
            (load_pem_key(&read_file(key_path, KEY)?, key_password)?, chain)
        }
        CertFormat::Jks => {
            let data = read_file(&cert.certificate_path, CLIENT)?;
            if jks::is_jks(&data) {
                jks::identity(&data, cert.alias.as_deref(), store_password, key_password)?
            } else {
                pkcs12_identity(&data, store_password)?
            }
        }
        CertFormat::Pkcs12 => pkcs12_identity(&read_file(&cert.certificate_path, CLIENT)?, store_password)?,
    };
    finish_identity(key, chain)
}

fn load_pem_key(data: &[u8], password: Option<&str>) -> AppResult<PKey<Private>> {
    let encrypted = String::from_utf8_lossy(data).contains("ENCRYPTED");
    match non_empty(password) {
        None if encrypted => Err(AppError::invalid(format!("{KEY}: ключ зашифрован — укажите пароль ключа"))),
        None => PKey::private_key_from_pem(data)
            .map_err(|e| ssl_err(format!("{KEY}: файл не содержит PEM-ключа"), e)),
        Some(pw) => PKey::private_key_from_pem_passphrase(data, pw.as_bytes()).map_err(|e| {
            let msg = if encrypted {
                format!("{KEY}: неверный пароль ключа")
            } else {
                format!("{KEY}: файл не содержит PEM-ключа")
            };
            ssl_err(msg, e)
        }),
    }
}

fn pkcs12_identity(data: &[u8], password: Option<&str>) -> AppResult<(PKey<Private>, Vec<X509>)> {
    let p = parse_pkcs12(data, password, CLIENT)?;
    let key = p
        .pkey
        .ok_or_else(|| AppError::invalid(format!("{CLIENT}: в хранилище нет закрытого ключа")))?;
    let cert = p
        .cert
        .ok_or_else(|| AppError::invalid(format!("{CLIENT}: в хранилище нет сертификата")))?;
    Ok((key, std::iter::once(cert).chain(p.ca.into_iter().flatten()).collect()))
}

static LEGACY: OnceLock<Option<Provider>> = OnceLock::new();

/// Старые PKCS#12 (RC2/3DES, например созданные LibreSSL или Java 8) требуют провайдер legacy OpenSSL 3.
fn load_legacy_provider() -> bool {
    LEGACY
        .get_or_init(|| Provider::try_load(None, "legacy", true).ok())
        .is_some()
}

fn is_unsupported(e: &ErrorStack) -> bool {
    let t = e.to_string().to_lowercase();
    t.contains("unsupported") || t.contains("rc2") || t.contains("fetch")
}

fn parse_pkcs12(data: &[u8], password: Option<&str>, what: &str) -> AppResult<ParsedPkcs12_2> {
    let p12 = Pkcs12::from_der(data)
        .map_err(|e| ssl_err(format!("{what}: файл не является хранилищем PKCS#12"), e))?;
    let pw = password.unwrap_or("");
    match p12.parse2(pw) {
        Ok(p) => Ok(p),
        Err(e) if is_unsupported(&e) && load_legacy_provider() => {
            p12.parse2(pw).map_err(|e| pkcs12_err(what, e))
        }
        Err(e) => Err(pkcs12_err(what, e)),
    }
}

fn pkcs12_err(what: &str, e: ErrorStack) -> AppError {
    let t = e.to_string().to_lowercase();
    let msg = if t.contains("mac verify") || t.contains("bad decrypt") {
        format!("{what}: неверный пароль хранилища PKCS#12")
    } else if is_unsupported(&e) {
        format!(
            "{what}: хранилище PKCS#12 зашифровано устаревшим алгоритмом (RC2/3DES), который недоступен. \
             Пересоздайте его с AES: openssl pkcs12 -export -keypbe AES-256-CBC -certpbe AES-256-CBC …"
        )
    } else {
        format!("{what}: не удалось прочитать хранилище PKCS#12")
    };
    ssl_err(msg, e)
}

fn finish_identity(key: PKey<Private>, chain: Vec<X509>) -> AppResult<ClientIdentity> {
    let leaf = chain
        .first()
        .ok_or_else(|| AppError::invalid(format!("{CLIENT}: файл не содержит сертификатов")))?;
    let public = leaf
        .public_key()
        .map_err(|e| ssl_err(format!("{CLIENT}: не удалось прочитать открытый ключ"), e))?;
    if !public.public_eq(&key) {
        return Err(AppError::invalid(format!("{KEY} не соответствует клиентскому сертификату")));
    }
    let now = Asn1Time::days_from_now(0).map_err(|e| ssl_err("Не удалось получить текущее время".into(), e))?;
    let cmp = |t: &openssl::asn1::Asn1TimeRef| t.compare(&now).unwrap_or(Ordering::Equal);
    if cmp(leaf.not_before()) == Ordering::Greater {
        return Err(AppError::invalid(format!(
            "{CLIENT} ещё не действует (действует с {})",
            leaf.not_before()
        )));
    }
    if cmp(leaf.not_after()) == Ordering::Less {
        return Err(AppError::invalid(format!("Срок действия: {CLIENT} истёк {}", leaf.not_after())));
    }
    let key_pem = key
        .private_key_to_pem_pkcs8()
        .map_err(|e| ssl_err(format!("{KEY}: не удалось преобразовать в PEM"), e))?;
    Ok(ClientIdentity {
        certificate_pem: to_pem_chain(&chain)?,
        key_pem: String::from_utf8(key_pem).map_err(|_| AppError::internal("PEM не в UTF-8"))?,
    })
}

fn to_pem_chain(certs: &[X509]) -> AppResult<String> {
    let mut out = String::new();
    for c in certs {
        let pem = c
            .to_pem()
            .map_err(|e| ssl_err("Не удалось преобразовать сертификат в PEM".into(), e))?;
        out.push_str(&String::from_utf8_lossy(&pem));
    }
    Ok(out)
}
```

Временная заглушка `src-tauri/src/tls/jks.rs` (полностью заменяется в Task 4):

```rust
use openssl::pkey::{PKey, Private};
use openssl::x509::X509;

use crate::error::{AppError, AppResult};

const JKS_MAGIC: [u8; 4] = [0xFE, 0xED, 0xFE, 0xED];

pub fn is_jks(data: &[u8]) -> bool {
    data.starts_with(&JKS_MAGIC)
}

pub fn trusted_certificates(_data: &[u8], _password: Option<&str>) -> AppResult<Vec<X509>> {
    Err(AppError::invalid("Формат JKS пока не поддерживается"))
}

pub fn identity(
    _data: &[u8],
    _alias: Option<&str>,
    _store_password: Option<&str>,
    _key_password: Option<&str>,
) -> AppResult<(PKey<Private>, Vec<X509>)> {
    Err(AppError::invalid("Формат JKS пока не поддерживается"))
}
```

`src-tauri/src/tls/der.rs` — пустой файл с комментарием `//! Минимальный разбор DER (Task 4).`. В `lib.rs` добавить `pub mod tls;`.

Если `client.p12` создан LibreSSL (macOS) и Provider legacy недоступен в vendored-сборке, тест `pkcs12_identity_with_and_without_password` упадёт с текстом про RC2/3DES. Тогда:
- проверить `Provider::try_load(None, "legacy", true)` в отдельном тесте;
- если провайдер не загружается, оставить понятную ошибку, а фикстуры `client*.p12` пересоздать с AES: добавить в `generate.sh` для OpenSSL 3 `-keypbe AES-256-CBC -certpbe AES-256-CBC -macalg sha256`;
- описать решение в отчёте.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib tls:: storage::connections`
Expected: PASS. `ca_from_pem_pkcs12_and_jks_truststores` проверяет только PEM и PKCS#12, JKS появится в Task 4.

- [ ] **Step 6: Полный прогон и clippy**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && cargo fmt --manifest-path src-tauri/Cargo.toml --check`
Expected: всё зелёное.

- [ ] **Step 7: Commit**

```bash
git add src-tauri
git commit -m "feat(tls): модель клиентского сертификата, загрузка PEM и PKCS#12 с проверками

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Разбор JKS

**Files:**
- Modify: `src-tauri/src/tls/jks.rs` (полная реализация)
- Modify: `src-tauri/src/tls/der.rs`

**Interfaces:**
- Consumes: фикстуры `client.jks`, `client-keypass.jks`, `truststore.jks` (Task 2); `tls::load_identity` / `load_ca` уже вызывают `jks::identity` и `jks::trusted_certificates` (Task 3).
- Produces:
  ```rust
  pub fn is_jks(data: &[u8]) -> bool;
  pub fn trusted_certificates(data: &[u8], store_password: Option<&str>) -> AppResult<Vec<X509>>;
  pub fn identity(data: &[u8], alias: Option<&str>, store_password: Option<&str>, key_password: Option<&str>)
      -> AppResult<(PKey<Private>, Vec<X509>)>;
  ```

Формат JKS (big-endian):
- `magic 0xFEEDFEED`, `version (1|2)`, `count`;
- записи:
  - `tag=1` (ключ): `alias(UTF)`, `timestamp u64`, `u32 len + EncryptedPrivateKeyInfo`, `u32 n` × (`v2: type(UTF)`, `u32 len + DER-сертификат`);
  - `tag=2` (доверенный сертификат): `alias`, `timestamp`, `v2: type(UTF)`, `u32 len + DER`;
- в конце SHA-1 от `UTF-16BE(пароль) || "Mighty Aphrodite" || все предшествующие байты`.

Ключ защищён алгоритмом Sun KeyProtector (OID `1.3.6.1.4.1.42.2.17.1.1`). Данные: `salt(20) || шифртекст || check(20)`. Гамма вычисляется так: `d_0 = salt`, `d_i = SHA1(pw || d_{i-1})`; открытый текст — PKCS#8 DER. Проверка: `SHA1(pw || plain) == check`. `pw` — UTF-16BE.

JCEKS (`0xCECECECE`) не поддерживается: выдаём понятное сообщение.

- [ ] **Step 1: Тесты** — в `src-tauri/src/tls/jks.rs` (заменив заглушку целиком, сначала тесты):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(format!("{}/../dev/certs/out/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
    }

    #[test]
    fn detects_magic() {
        assert!(is_jks(&fixture("client.jks")));
        assert!(!is_jks(&fixture("client.p12")));
        assert!(!is_jks(b"\xFE\xED"));
    }

    #[test]
    fn identity_with_store_password_as_key_password() {
        let (key, chain) = identity(&fixture("client.jks"), None, Some("storepass"), None).unwrap();
        assert!(chain[0].public_key().unwrap().public_eq(&key));
        assert!(!chain.is_empty());
    }

    #[test]
    fn separate_key_password() {
        let data = fixture("client-keypass.jks");
        let e = identity(&data, None, Some("storepass"), None).unwrap_err();
        assert!(e.message.contains("пароль ключа"), "{}", e.message);
        identity(&data, Some("CLIENT"), Some("storepass"), Some("keypass")).unwrap();
    }

    #[test]
    fn wrong_store_password_and_skipped_integrity_check() {
        let data = fixture("client.jks");
        let e = identity(&data, None, Some("nope"), Some("storepass")).unwrap_err();
        assert!(e.message.contains("пароль хранилища"), "{}", e.message);
        // Без пароля хранилища проверка целостности пропускается, ключ расшифровывается своим паролем.
        identity(&data, None, None, Some("storepass")).unwrap();
    }

    #[test]
    fn unknown_alias_lists_available() {
        let e = identity(&fixture("client.jks"), Some("other"), Some("storepass"), None).unwrap_err();
        assert!(e.message.contains("client"), "{}", e.message);
    }

    #[test]
    fn truststore_certificates() {
        let certs = trusted_certificates(&fixture("truststore.jks"), Some("storepass")).unwrap();
        assert_eq!(certs.len(), 1);
        let e = trusted_certificates(&fixture("client.jks"), Some("storepass")).unwrap_err();
        assert!(e.message.contains("доверенных"), "{}", e.message);
    }

    #[test]
    fn jceks_and_garbage_are_rejected_with_reason() {
        let e = identity(b"\xCE\xCE\xCE\xCE\0\0\0\x02\0\0\0\0", None, None, None).unwrap_err();
        assert!(e.message.contains("JCEKS"), "{}", e.message);
        let mut truncated = fixture("client.jks");
        truncated.truncate(40);
        let e = identity(&truncated, None, None, Some("storepass")).unwrap_err();
        assert!(e.message.contains("повреж"), "{}", e.message);
    }

    #[test]
    fn via_load_identity_and_load_ca() {
        use crate::storage::connections::{CertFormat, ClientCert};
        let path = format!("{}/../dev/certs/out/client.jks", env!("CARGO_MANIFEST_DIR"));
        let cc = ClientCert { format: CertFormat::Jks, certificate_path: path, key_path: None, alias: None };
        crate::tls::load_identity(&cc, None, Some("storepass")).unwrap();
        let ts = format!("{}/../dev/certs/out/truststore.jks", env!("CARGO_MANIFEST_DIR"));
        assert!(crate::tls::load_ca(CertFormat::Jks, &ts, Some("storepass")).unwrap().contains("BEGIN CERTIFICATE"));
    }
}
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib tls::jks`
Expected: FAIL (заглушка возвращает ошибку «пока не поддерживается»).

- [ ] **Step 2: DER** — `src-tauri/src/tls/der.rs`:

```rust
//! Минимальный разбор DER: тег, содержимое и остаток (для EncryptedPrivateKeyInfo в JKS).

pub fn read_tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (first as usize, rest)
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n].iter().fold(0usize, |a, b| (a << 8) | *b as usize);
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_and_long_lengths() {
        assert_eq!(read_tlv(&[0x04, 0x02, 1, 2, 9]), Some((0x04, &[1u8, 2][..], &[9u8][..])));
        let mut long = vec![0x04, 0x81, 0x80];
        long.extend(std::iter::repeat_n(7u8, 0x80));
        assert_eq!(read_tlv(&long).unwrap().1.len(), 0x80);
        assert_eq!(read_tlv(&[0x04, 0x05, 1]), None);
    }
}
```

- [ ] **Step 3: Реализация JKS** — начало `src-tauri/src/tls/jks.rs` (над тестами):

```rust
//! Хранилища Java KeyStore (JKS). Только чтение: записи с ключом и доверенные сертификаты.

use openssl::pkey::{PKey, Private};
use openssl::sha::Sha1;
use openssl::x509::X509;

use super::der::read_tlv;
use crate::error::{AppError, AppResult};

const JKS_MAGIC: [u8; 4] = [0xFE, 0xED, 0xFE, 0xED];
const JCEKS_MAGIC: [u8; 4] = [0xCE, 0xCE, 0xCE, 0xCE];
const WHITENER: &[u8] = b"Mighty Aphrodite";
/// 1.3.6.1.4.1.42.2.17.1.1 — Sun KeyProtector.
const KEY_PROTECTOR_OID: [u8; 10] = [0x2B, 0x06, 0x01, 0x04, 0x01, 0x2A, 0x02, 0x11, 0x01, 0x01];

pub fn is_jks(data: &[u8]) -> bool {
    data.starts_with(&JKS_MAGIC)
}

struct KeyEntry {
    alias: String,
    protected_key: Vec<u8>,
    chain: Vec<Vec<u8>>,
}

struct Store {
    keys: Vec<KeyEntry>,
    trusted: Vec<Vec<u8>>,
}

fn damaged() -> AppError {
    AppError::invalid("Хранилище JKS повреждено или обрезано")
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> AppResult<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|e| *e <= self.data.len()).ok_or_else(damaged)?;
        let s = &self.data[self.pos..end];
        self.pos = end;
        Ok(s)
    }
    fn u16(&mut self) -> AppResult<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> AppResult<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    /// Java writeUTF: длина u16 + modified UTF-8 (для обычных alias совпадает с UTF-8).
    fn utf(&mut self) -> AppResult<String> {
        let n = self.u16()? as usize;
        Ok(String::from_utf8_lossy(self.take(n)?).into_owned())
    }
    fn blob(&mut self) -> AppResult<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

fn utf16be(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(u16::to_be_bytes).collect()
}

fn parse(data: &[u8], store_password: Option<&str>) -> AppResult<Store> {
    if data.starts_with(&JCEKS_MAGIC) {
        return Err(AppError::invalid(
            "Хранилище JCEKS не поддерживается — сконвертируйте его в PKCS#12: \
             keytool -importkeystore -srcstoretype JCEKS -srckeystore <файл> -deststoretype PKCS12 -destkeystore <файл>.p12",
        ));
    }
    let mut r = Reader { data, pos: 0 };
    if r.take(4)? != JKS_MAGIC {
        return Err(AppError::invalid("Файл не является хранилищем JKS"));
    }
    let version = r.u32()?;
    if version != 1 && version != 2 {
        return Err(AppError::invalid(format!("Неподдерживаемая версия JKS: {version}")));
    }
    let count = r.u32()?;
    let mut store = Store { keys: Vec::new(), trusted: Vec::new() };
    for _ in 0..count {
        let tag = r.u32()?;
        let alias = r.utf()?;
        r.take(8)?; // время создания записи
        match tag {
            1 => {
                let protected_key = r.blob()?.to_vec();
                let n = r.u32()?;
                let mut chain = Vec::new();
                for _ in 0..n {
                    if version == 2 {
                        r.utf()?; // тип сертификата, «X.509»
                    }
                    chain.push(r.blob()?.to_vec());
                }
                store.keys.push(KeyEntry { alias, protected_key, chain });
            }
            2 => {
                if version == 2 {
                    r.utf()?;
                }
                store.trusted.push(r.blob()?.to_vec());
            }
            other => return Err(AppError::invalid(format!("Неизвестный тип записи JKS: {other}"))),
        }
    }
    let body = r.pos;
    let digest = r.take(20)?;
    if let Some(pw) = store_password.filter(|p| !p.is_empty()) {
        let mut h = Sha1::new();
        h.update(&utf16be(pw));
        h.update(WHITENER);
        h.update(&data[..body]);
        if h.finish() != digest {
            return Err(AppError::invalid("Неверный пароль хранилища JKS (или файл повреждён)"));
        }
    }
    Ok(store)
}

fn protected_data(info: &[u8]) -> AppResult<&[u8]> {
    let bad = || AppError::invalid("Хранилище JKS: повреждённая запись ключа");
    let (t, seq, _) = read_tlv(info).ok_or_else(bad)?;
    if t != 0x30 {
        return Err(bad());
    }
    let (t, alg, rest) = read_tlv(seq).ok_or_else(bad)?;
    if t != 0x30 {
        return Err(bad());
    }
    let (t, oid, _) = read_tlv(alg).ok_or_else(bad)?;
    if t != 0x06 || oid != KEY_PROTECTOR_OID {
        return Err(AppError::invalid(
            "Хранилище JKS: ключ защищён неподдерживаемым алгоритмом — сконвертируйте хранилище в PKCS#12",
        ));
    }
    let (t, data, _) = read_tlv(rest).ok_or_else(bad)?;
    if t != 0x04 {
        return Err(bad());
    }
    Ok(data)
}

/// Расшифровка Sun KeyProtector → PKCS#8 DER.
fn decrypt_key(info: &[u8], password: &str) -> AppResult<Vec<u8>> {
    let data = protected_data(info)?;
    if data.len() < 40 {
        return Err(AppError::invalid("Хранилище JKS: повреждённая запись ключа"));
    }
    let (salt, rest) = data.split_at(20);
    let (encrypted, check) = rest.split_at(rest.len() - 20);
    let pw = utf16be(password);
    let mut plain = Vec::with_capacity(encrypted.len());
    let mut digest = [0u8; 20];
    digest.copy_from_slice(salt);
    for chunk in encrypted.chunks(20) {
        let mut h = Sha1::new();
        h.update(&pw);
        h.update(&digest);
        digest = h.finish();
        plain.extend(chunk.iter().zip(digest.iter()).map(|(a, b)| a ^ b));
    }
    let mut h = Sha1::new();
    h.update(&pw);
    h.update(&plain);
    if h.finish() != check {
        return Err(AppError::invalid(
            "Неверный пароль ключа в хранилище JKS — укажите пароль ключа (если он отличается от пароля хранилища)",
        ));
    }
    Ok(plain)
}

fn x509(der: &[u8]) -> AppResult<X509> {
    X509::from_der(der).map_err(|e| AppError::invalid("Хранилище JKS: повреждённый сертификат").with_details(e.to_string()))
}

pub fn trusted_certificates(data: &[u8], store_password: Option<&str>) -> AppResult<Vec<X509>> {
    let store = parse(data, store_password)?;
    if store.trusted.is_empty() {
        return Err(AppError::invalid("В хранилище JKS нет доверенных сертификатов (записей trustedCertEntry)"));
    }
    store.trusted.iter().map(|d| x509(d)).collect()
}

pub fn identity(
    data: &[u8],
    alias: Option<&str>,
    store_password: Option<&str>,
    key_password: Option<&str>,
) -> AppResult<(PKey<Private>, Vec<X509>)> {
    let store = parse(data, store_password)?;
    let names = || store.keys.iter().map(|k| k.alias.as_str()).collect::<Vec<_>>().join(", ");
    let entry = match alias.map(str::trim).filter(|a| !a.is_empty()) {
        // keytool хранит alias в нижнем регистре.
        Some(a) => store.keys.iter().find(|k| k.alias.eq_ignore_ascii_case(a)).ok_or_else(|| {
            AppError::invalid(format!("В хранилище JKS нет ключа «{a}». Доступны: {}", names()))
        })?,
        None => match store.keys.as_slice() {
            [] => return Err(AppError::invalid("В хранилище JKS нет закрытого ключа")),
            [one] => one,
            _ => {
                return Err(AppError::invalid(format!(
                    "В хранилище JKS несколько ключей — укажите alias: {}",
                    names()
                )));
            }
        },
    };
    let pw = key_password
        .filter(|p| !p.is_empty())
        .or(store_password)
        .unwrap_or("");
    let pkcs8 = decrypt_key(&entry.protected_key, pw)?;
    let key = PKey::private_key_from_pkcs8(&pkcs8)
        .map_err(|e| AppError::invalid("Хранилище JKS: не удалось прочитать ключ").with_details(e.to_string()))?;
    let chain = entry.chain.iter().map(|d| x509(d)).collect::<AppResult<Vec<_>>>()?;
    Ok((key, chain))
}
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib tls::`
Expected: PASS.

- [ ] **Step 4: clippy, fmt, commit**

Run: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && cargo fmt --manifest-path src-tauri/Cargo.toml --check`

```bash
git add src-tauri/src/tls
git commit -m "feat(tls): разбор хранилищ JKS (ключи и доверенные сертификаты)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Сертификаты в подключении: конфигурация, секреты, ошибки TLS

**Files:**
- Modify: `src-tauri/src/kafka/config.rs` (`apply_tls` через `tls::*`, имена секретов)
- Modify: `src-tauri/src/secrets.rs` (`OverlayStore`, `CONNECTION_SECRET_FIELDS`)
- Modify: `src-tauri/src/commands/connections.rs` (`TlsPasswordsInput`, сохранение и проверка)
- Modify: `src-tauri/src/state.rs` (`delete_connection_full` удаляет все секреты)
- Modify: `src-tauri/src/kafka/connectivity.rs` (сертификаты проверяются на шаге «config»)
- Modify: `src-tauri/src/error.rs` (`tls_message`)
- Create: `src-tauri/tests/it_mtls.rs`

**Interfaces:**
- Consumes: `tls::load_ca`, `tls::load_identity` (Task 3–4); `common::mtls_bootstrap`, `common::cert_path` (Task 2).
- Produces (в `kafka::config`):
  ```rust
  pub const SASL_PASSWORD: &str = "sasl.password";            // уже есть
  pub const SSL_KEY_PASSWORD: &str = "ssl.key.password";
  pub const SSL_KEYSTORE_PASSWORD: &str = "ssl.keystore.password";
  pub const SSL_TRUSTSTORE_PASSWORD: &str = "ssl.truststore.password";
  ```
- Produces (в `secrets`):
  ```rust
  pub const CONNECTION_SECRET_FIELDS: [&str; 4] = [SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD];
  /// Несохранённые значения поверх основного хранилища: Some("") — «пароль удалён».
  pub struct OverlayStore { pub base: Arc<dyn SecretStore>, pub overrides: HashMap<String, String> }
  ```
- Produces (в `commands::connections`):
  ```rust
  #[derive(Debug, Default, Deserialize)] #[serde(rename_all = "camelCase")]
  pub struct TlsPasswordsInput { pub key_password: Option<String>, pub keystore_password: Option<String>, pub truststore_password: Option<String> }
  pub struct ConnectionInput { pub profile, pub sasl_password: Option<String>, #[serde(default)] pub tls_passwords: Option<TlsPasswordsInput> }
  ```
  Семантика каждого пароля TLS: `null` или отсутствует — оставить сохранённый, `""` — удалить, иное — сохранить. Пароль SASL работает как раньше: пустой — оставить.
  JSON команды: `{"input": {"profile": …, "saslPassword": null, "tlsPasswords": {"keyPassword": null, "keystorePassword": "x", "truststorePassword": ""}}}`.
- Produces: сообщения `TLS_ERROR`:
  - «Имя хоста брокера не совпадает с его сертификатом»;
  - «Сертификат брокера не подписан доверенным CA — проверьте «Доверенный CA»»;
  - «Брокер не принял клиентский сертификат»;
  - иначе «Ошибка TLS-соединения».

Какие секреты нужны профилю (иначе удаляются при сохранении):
- `ssl.truststore.password` — есть `caLocation` и `caFormat` ≠ `pem`;
- `ssl.keystore.password` — `clientCert.format` ∈ {`pkcs12`, `jks`};
- `ssl.key.password` — `clientCert.format` ∈ {`pem`, `jks`};
- `sasl.password` — как раньше.

- [ ] **Step 1: Тесты конфигурации** — в `kafka/config.rs` (`mod tests`) заменить `sasl_ssl_reads_password_from_secrets` и `ssl_uses_probe_…` под новые поля, добавить тесты сертификатов:

```rust
    fn fixture(name: &str) -> String {
        format!("{}/../dev/certs/out/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    fn tls(ca: Option<String>, client: Option<ClientCert>) -> TlsConfig {
        TlsConfig { ca_location: ca, ca_format: CertFormat::Pem, verify_hostname: true, client_cert: client }
    }

    #[test]
    fn ssl_uses_probe_when_no_ca_and_can_disable_hostname_check() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::Ssl { tls: TlsConfig { verify_hostname: false, ..tls(None, None) } };
        let c = build_client_config(&p, &MemoryStore::default()).unwrap();
        assert_eq!(c.get("security.protocol"), Some("ssl"));
        assert_eq!(c.get("ssl.ca.location"), Some("probe"));
        assert_eq!(c.get("ssl.endpoint.identification.algorithm"), Some("none"));
    }

    #[test]
    fn ca_and_client_identity_are_passed_as_pem() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::Ssl {
            tls: tls(
                Some(fixture("ca.pem")),
                Some(ClientCert {
                    format: CertFormat::Jks,
                    certificate_path: fixture("client-keypass.jks"),
                    key_path: None,
                    alias: None,
                }),
            ),
        };
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("c1", SSL_KEYSTORE_PASSWORD), "storepass").unwrap();
        secrets.set(&secret_key("c1", SSL_KEY_PASSWORD), "keypass").unwrap();
        let c = build_client_config(&p, &secrets).unwrap();
        assert!(c.get("ssl.ca.pem").unwrap().contains("BEGIN CERTIFICATE"));
        assert_eq!(c.get("ssl.ca.location"), None);
        assert!(c.get("ssl.certificate.pem").unwrap().contains("BEGIN CERTIFICATE"));
        assert!(c.get("ssl.key.pem").unwrap().contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn certificate_problems_are_invalid_input() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::Ssl {
            tls: tls(
                None,
                Some(ClientCert {
                    format: CertFormat::Pem,
                    certificate_path: fixture("client.pem"),
                    key_path: Some(fixture("client-enc.key")),
                    alias: None,
                }),
            ),
        };
        let e = build_client_config(&p, &MemoryStore::default()).err().unwrap();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("зашифрован"), "{}", e.message);
    }

    #[test]
    fn sasl_ssl_reads_password_from_secrets() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::SaslSsl {
            tls: tls(Some(fixture("ca.pem")), None),
            sasl: SaslConfig { mechanism: SaslMechanism::ScramSha512, username: "alice".into() },
        };
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("c1", SASL_PASSWORD), "pw").unwrap();
        let c = build_client_config(&p, &secrets).unwrap();
        assert_eq!(c.get("security.protocol"), Some("sasl_ssl"));
        assert!(c.get("ssl.ca.pem").unwrap().contains("BEGIN CERTIFICATE"));
        assert_eq!(c.get("ssl.endpoint.identification.algorithm"), Some("https"));
        assert_eq!(c.get("sasl.password"), Some("pw"));
    }
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::config`
Expected: FAIL (нет констант, `ssl.ca.pem` не задаётся).

- [ ] **Step 2: Реализация конфигурации** — в `kafka/config.rs`:

```rust
pub const SASL_PASSWORD: &str = "sasl.password";
pub const SSL_KEY_PASSWORD: &str = "ssl.key.password";
pub const SSL_KEYSTORE_PASSWORD: &str = "ssl.keystore.password";
pub const SSL_TRUSTSTORE_PASSWORD: &str = "ssl.truststore.password";
```

В `build_client_config` заменить вызовы `apply_tls(&mut c, tls)` на `apply_tls(&mut c, &p.id, tls, secrets)?`. Новая `apply_tls`:

```rust
/// Сертификаты читаются при каждом создании клиента (ротация) и передаются как PEM в памяти.
/// Вызывается только из blocking-контекста: читает файлы и keychain.
fn apply_tls(c: &mut ClientConfig, id: &str, tls: &TlsConfig, secrets: &dyn SecretStore) -> AppResult<()> {
    let secret = |field: &str| secrets.get(&secret_key(id, field));
    match tls.ca_location.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        // "probe" — librdkafka сам ищет системные CA (vendored OpenSSL не знает путей ОС).
        None => {
            c.set("ssl.ca.location", "probe");
        }
        Some(path) => {
            let pem = crate::tls::load_ca(tls.ca_format, path, secret(SSL_TRUSTSTORE_PASSWORD)?.as_deref())?;
            c.set("ssl.ca.pem", pem);
        }
    }
    if let Some(cert) = &tls.client_cert {
        let id_pem = crate::tls::load_identity(
            cert,
            secret(SSL_KEY_PASSWORD)?.as_deref(),
            secret(SSL_KEYSTORE_PASSWORD)?.as_deref(),
        )?;
        c.set("ssl.certificate.pem", id_pem.certificate_pem);
        c.set("ssl.key.pem", id_pem.key_pem);
    }
    c.set(
        "ssl.endpoint.identification.algorithm",
        if tls.verify_hostname { "https" } else { "none" },
    );
    Ok(())
}
```

Все вызывающие (`pool.rs`, `producer.rs`, `browse/session.rs`, `consume/session.rs`, `produce/batch.rs`, `connectivity.rs`) уже работают в blocking-контексте или в отдельных потоках. Проверить `grep -rn "build_client_config\|KafkaHandles::create\|producers.get" src-tauri/src` и убедиться, что ни один вызов не идёт напрямую из `async fn` без `blocking`. Если найдётся, обернуть.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::config`
Expected: PASS.

- [ ] **Step 3: Тесты секретов и сохранения** — в `secrets.rs` (`mod tests`):

```rust
    #[test]
    fn overlay_prefers_overrides_and_empty_means_absent() {
        let base = Arc::new(MemoryStore::default());
        base.set("c/a", "base-a").unwrap();
        base.set("c/b", "base-b").unwrap();
        let mut overrides = HashMap::new();
        overrides.insert("c/a".to_string(), "new-a".to_string());
        overrides.insert("c/b".to_string(), String::new());
        let o = OverlayStore { base, overrides };
        assert_eq!(o.get("c/a").unwrap().as_deref(), Some("new-a"));
        assert_eq!(o.get("c/b").unwrap(), None, "пустое значение — пароль удалён");
        assert_eq!(o.get("c/x").unwrap(), None);
    }
```

В `commands/connections.rs` (`mod tests`) обновить хелпер `input` (поле `tls_passwords: None`) и добавить:

```rust
    use crate::kafka::config::{SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD};
    use crate::storage::connections::{CertFormat, ClientCert, TlsConfig};

    fn mtls(mut p: ConnectionProfile, format: CertFormat, ca_format: CertFormat) -> ConnectionProfile {
        let abs = if cfg!(windows) { "C:\\c\\c" } else { "/c/c" };
        p.security = SecurityConfig::Ssl {
            tls: TlsConfig {
                ca_location: Some(abs.into()),
                ca_format,
                verify_hostname: true,
                client_cert: Some(ClientCert {
                    format,
                    certificate_path: abs.into(),
                    key_path: Some(abs.into()),
                    alias: None,
                }),
            },
        };
        p
    }

    fn tls_input(profile: ConnectionProfile, key: Option<&str>, store: Option<&str>, trust: Option<&str>) -> ConnectionInput {
        ConnectionInput {
            profile,
            sasl_password: None,
            tls_passwords: Some(TlsPasswordsInput {
                key_password: key.map(String::from),
                keystore_password: store.map(String::from),
                truststore_password: trust.map(String::from),
            }),
        }
    }

    #[test]
    fn tls_passwords_set_keep_and_clear() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        let p = || mtls(sample_profile("a"), CertFormat::Jks, CertFormat::Jks);
        let get = |st: &AppState, f: &str| st.secrets.get(&secret_key("a", f)).unwrap();
        save_connection_blocking(&st, tls_input(p(), Some("k"), Some("s"), Some("t"))).unwrap();
        assert_eq!(get(&st, SSL_KEY_PASSWORD).as_deref(), Some("k"));
        // null — оставить, "" — удалить.
        save_connection_blocking(&st, tls_input(p(), None, Some(""), None)).unwrap();
        assert_eq!(get(&st, SSL_KEY_PASSWORD).as_deref(), Some("k"));
        assert_eq!(get(&st, SSL_KEYSTORE_PASSWORD), None);
        assert_eq!(get(&st, SSL_TRUSTSTORE_PASSWORD).as_deref(), Some("t"));
    }

    #[test]
    fn unused_tls_passwords_are_deleted() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        save_connection_blocking(&st, tls_input(mtls(sample_profile("a"), CertFormat::Jks, CertFormat::Jks), Some("k"), Some("s"), Some("t"))).unwrap();
        // PEM-клиент и PEM-CA: пароль хранилища и truststore больше не нужны.
        save_connection_blocking(&st, tls_input(mtls(sample_profile("a"), CertFormat::Pem, CertFormat::Pem), None, None, None)).unwrap();
        let get = |f: &str| st.secrets.get(&secret_key("a", f)).unwrap();
        assert_eq!(get(SSL_KEY_PASSWORD).as_deref(), Some("k"));
        assert_eq!(get(SSL_KEYSTORE_PASSWORD), None);
        assert_eq!(get(SSL_TRUSTSTORE_PASSWORD), None);
        // PLAINTEXT — удаляются все пароли TLS.
        save_connection_blocking(&st, input(sample_profile("a"), None)).unwrap();
        assert_eq!(get(SSL_KEY_PASSWORD), None);
    }
```

В `state.rs` (`mod tests`):

```rust
    #[test]
    fn delete_connection_removes_all_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        state.connections.upsert(sample_profile("a")).unwrap();
        for f in crate::secrets::CONNECTION_SECRET_FIELDS {
            state.secrets.set(&crate::secrets::secret_key("a", f), "x").unwrap();
        }
        state.delete_connection_full("a").unwrap();
        for f in crate::secrets::CONNECTION_SECRET_FIELDS {
            assert_eq!(state.secrets.get(&crate::secrets::secret_key("a", f)).unwrap(), None, "{f}");
        }
    }
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib secrets commands::connections state`
Expected: FAIL.

- [ ] **Step 4: Реализация секретов и сохранения**

`secrets.rs`:

```rust
use std::sync::Arc;

use crate::kafka::config::{SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD};

/// Все секреты подключения в keychain (для удаления и миграции).
pub const CONNECTION_SECRET_FIELDS: [&str; 4] =
    [SASL_PASSWORD, SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE_PASSWORD];

/// Несохранённые значения из формы поверх keychain (проверка подключения до сохранения).
pub struct OverlayStore {
    pub base: Arc<dyn SecretStore>,
    pub overrides: HashMap<String, String>,
}

impl SecretStore for OverlayStore {
    fn get(&self, key: &str) -> AppResult<Option<String>> {
        match self.overrides.get(key) {
            Some(v) if v.is_empty() => Ok(None),
            Some(v) => Ok(Some(v.clone())),
            None => self.base.get(key),
        }
    }
    fn set(&self, _: &str, _: &str) -> AppResult<()> {
        Err(AppError::internal("OverlayStore только для чтения"))
    }
    fn delete(&self, _: &str) -> AppResult<()> {
        Err(AppError::internal("OverlayStore только для чтения"))
    }
}
```

`commands/connections.rs`:

```rust
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsPasswordsInput {
    #[serde(default)]
    pub key_password: Option<String>,
    #[serde(default)]
    pub keystore_password: Option<String>,
    #[serde(default)]
    pub truststore_password: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInput {
    pub profile: ConnectionProfile,
    #[serde(default)]
    pub sasl_password: Option<String>,
    #[serde(default)]
    pub tls_passwords: Option<TlsPasswordsInput>,
}

/// Что делать с секретом при сохранении.
enum SecretOp {
    Keep,
    Set(String),
    Delete,
}

/// Секреты профиля: используемые поля — по данным формы, неиспользуемые — удаляются.
fn secret_ops(
    profile: &ConnectionProfile,
    sasl_password: Option<&str>,
    pw: Option<&TlsPasswordsInput>,
) -> Vec<(&'static str, SecretOp)> {
    use crate::storage::connections::{CertFormat, SecurityConfig};
    let tls = match &profile.security {
        SecurityConfig::Ssl { tls } | SecurityConfig::SaslSsl { tls, .. } => Some(tls),
        _ => None,
    };
    let tls_op = |used: bool, v: Option<&String>| match (used, v) {
        (false, _) => SecretOp::Delete,
        (true, None) => SecretOp::Keep,
        (true, Some(s)) if s.is_empty() => SecretOp::Delete,
        (true, Some(s)) => SecretOp::Set(s.clone()),
    };
    let client = tls.and_then(|t| t.client_cert.as_ref()).map(|c| c.format);
    let ca_store = tls.is_some_and(|t| {
        t.ca_format != CertFormat::Pem && t.ca_location.as_deref().is_some_and(|s| !s.trim().is_empty())
    });
    vec![
        (
            SASL_PASSWORD,
            if !profile.security.uses_sasl() {
                SecretOp::Delete
            } else {
                match sasl_password.filter(|p| !p.is_empty()) {
                    Some(p) => SecretOp::Set(p.to_string()),
                    None => SecretOp::Keep,
                }
            },
        ),
        (
            SSL_KEY_PASSWORD,
            tls_op(matches!(client, Some(CertFormat::Pem | CertFormat::Jks)), pw.and_then(|p| p.key_password.as_ref())),
        ),
        (
            SSL_KEYSTORE_PASSWORD,
            tls_op(matches!(client, Some(CertFormat::Pkcs12 | CertFormat::Jks)), pw.and_then(|p| p.keystore_password.as_ref())),
        ),
        (SSL_TRUSTSTORE_PASSWORD, tls_op(ca_store, pw.and_then(|p| p.truststore_password.as_ref()))),
    ]
}
```

В `save_connection_blocking` блок с `secret_result` заменить на:

```rust
    let ops = secret_ops(&saved, sasl_password.as_deref(), tls_passwords.as_ref());
    let secret_result = ops.into_iter().try_for_each(|(field, op)| {
        let key = secret_key(&saved.id, field);
        match op {
            SecretOp::Keep => Ok(()),
            SecretOp::Set(v) => st.secrets.set(&key, &v),
            SecretOp::Delete => st.secrets.delete(&key),
        }
    });
```

В начале `save_connection_blocking` вход разбирается деструктуризацией вместо `let mut profile = input.profile;`:

```rust
    let ConnectionInput { profile: mut profile, sasl_password, tls_passwords } = input;
```

`test_connection`: пароли из формы накладываются на keychain:

```rust
#[tauri::command]
pub async fn test_connection(state: State<'_, AppState>, input: ConnectionInput) -> AppResult<TestReport> {
    let id = input.profile.id.clone();
    let mut overrides = HashMap::new();
    if let Some(pw) = input.sasl_password.as_ref().filter(|p| !p.is_empty()) {
        overrides.insert(secret_key(&id, SASL_PASSWORD), pw.clone());
    }
    if let Some(t) = &input.tls_passwords {
        for (field, v) in [
            (SSL_KEY_PASSWORD, &t.key_password),
            (SSL_KEYSTORE_PASSWORD, &t.keystore_password),
            (SSL_TRUSTSTORE_PASSWORD, &t.truststore_password),
        ] {
            if let Some(v) = v {
                overrides.insert(secret_key(&id, field), v.clone());
            }
        }
    }
    let secrets: Arc<dyn SecretStore> = Arc::new(OverlayStore { base: state.secrets.clone(), overrides });
    let profile = input.profile;
    blocking(move || Ok(connectivity::test_connection(&profile, secrets.as_ref()))).await
}
```

`state.rs`, `delete_connection_full`: вместо одного `secrets.delete(...SASL_PASSWORD)`:

```rust
        for field in crate::secrets::CONNECTION_SECRET_FIELDS {
            self.secrets.delete(&crate::secrets::secret_key(id, field))?;
        }
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`
Expected: PASS.

- [ ] **Step 5: Сертификаты на шаге «config» проверки подключения** — в `connectivity.rs`, `test_connection`, после `parse_bootstrap`:

```rust
    // Файлы сертификатов, пароли и SASL проверяются до сети: ошибка относится к конфигурации.
    if let Err(e) = crate::kafka::config::build_client_config(p, secrets) {
        return failed(steps, "config", e);
    }
```

Тест в `connectivity.rs` (`mod tests`):

```rust
    #[test]
    fn certificate_errors_fail_config_step() {
        use crate::storage::connections::{CertFormat, ClientCert, SecurityConfig, TlsConfig};
        let mut p = sample_profile("a");
        p.security = SecurityConfig::Ssl {
            tls: TlsConfig {
                ca_location: None,
                ca_format: CertFormat::Pem,
                verify_hostname: true,
                client_cert: Some(ClientCert {
                    format: CertFormat::Pem,
                    certificate_path: if cfg!(windows) { "C:\\nope.pem".into() } else { "/nope.pem".into() },
                    key_path: Some(if cfg!(windows) { "C:\\nope.key".into() } else { "/nope.key".into() }),
                    alias: None,
                }),
            },
        };
        let r = test_connection(&p, &MemoryStore::default());
        assert!(!r.ok);
        let last = r.steps.last().unwrap();
        assert_eq!(last.name, "config");
        assert_eq!(last.error.as_ref().unwrap().code, ErrorCode::InvalidInput);
    }
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::connectivity`
Expected: PASS.

- [ ] **Step 6: Понятные ошибки TLS — тесты** — в `error.rs` (`mod tests`):

```rust
    #[test]
    fn tls_errors_get_specific_messages() {
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut);
        for (client, expected) in [
            ("ssl://h:9095/bootstrap: SSL handshake failed: error:0A000086:SSL routines::certificate verify failed: broker certificate could not be verified", "не подписан доверенным CA"),
            ("ssl://h:9095/bootstrap: SSL handshake failed: certificate verify failed: Hostname mismatch", "Имя хоста"),
            ("ssl://h:9095/bootstrap: SSL handshake failed: error:0A00045C:SSL routines::tlsv13 alert certificate required", "не принял клиентский сертификат"),
            ("ssl://h:9095/bootstrap: SSL handshake failed: sslv3 alert bad certificate", "не принял клиентский сертификат"),
            ("ssl://h:9095/bootstrap: SSL handshake failed: something new", "Ошибка TLS-соединения"),
        ] {
            let app = from_kafka(&err, Some(client));
            assert_eq!(app.code, ErrorCode::TlsError, "{client}");
            assert!(app.message.contains(expected), "{client} → {}", app.message);
        }
    }
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib error::`
Expected: FAIL.

- [ ] **Step 7: Реализация** — в `error.rs`:

```rust
/// Уточнение TLS-ошибки по тексту librdkafka/OpenSSL. Имя хоста проверяется раньше общего
/// «certificate verify failed», в который оно вложено.
fn tls_message(text: &str) -> &'static str {
    let t = text.to_lowercase();
    if t.contains("hostname mismatch") || t.contains("ip address mismatch") {
        "Имя хоста брокера не совпадает с его сертификатом"
    } else if t.contains("certificate verify failed") {
        "Сертификат брокера не подписан доверенным CA — проверьте «Доверенный CA»"
    } else if t.contains("certificate required")
        || t.contains("bad certificate")
        || t.contains("unknown ca")
        || t.contains("certificate unknown")
    {
        "Брокер не принял клиентский сертификат"
    } else {
        "Ошибка TLS-соединения"
    }
}
```

В `from_kafka` вычислить `details` до `message` и для `ErrorCode::TlsError` взять `tls_message(&details)`.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`
Expected: PASS.

- [ ] **Step 8: Интеграционный тест** — `src-tauri/tests/it_mtls.rs`:

```rust
mod common;

use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::config::{SSL_KEY_PASSWORD, SSL_KEYSTORE_PASSWORD};
use kafka_explorer_lib::kafka::connectivity::test_connection;
use kafka_explorer_lib::secrets::{MemoryStore, SecretStore, secret_key};
use kafka_explorer_lib::storage::connections::{
    CertFormat, ClientCert, ConnectionProfile, SecurityConfig, TlsConfig,
};

fn profile(bs: &str, client: Option<ClientCert>) -> ConnectionProfile {
    let mut p = common::plaintext_profile(bs);
    p.security = SecurityConfig::Ssl {
        tls: TlsConfig {
            ca_location: Some(common::cert_path("ca.pem")),
            ca_format: CertFormat::Pem,
            verify_hostname: true,
            client_cert: client,
        },
    };
    p
}

fn cert(format: CertFormat, file: &str, key: Option<&str>) -> Option<ClientCert> {
    Some(ClientCert {
        format,
        certificate_path: common::cert_path(file),
        key_path: key.map(common::cert_path),
        alias: None,
    })
}

#[test]
fn connects_with_each_certificate_format() {
    let Some(bs) = common::mtls_bootstrap() else { return };
    let secrets = MemoryStore::default();
    secrets.set(&secret_key("it", SSL_KEYSTORE_PASSWORD), "storepass").unwrap();
    secrets.set(&secret_key("it", SSL_KEY_PASSWORD), "keypass").unwrap();
    for client in [
        cert(CertFormat::Pem, "client.pem", Some("client.key")),
        cert(CertFormat::Pem, "client.pem", Some("client-enc.key")),
        cert(CertFormat::Pkcs12, "client.p12", None),
        cert(CertFormat::Jks, "client-keypass.jks", None),
    ] {
        let r = test_connection(&profile(&bs, client.clone()), &secrets);
        assert!(r.ok, "{client:?}: {r:?}");
    }
}

#[test]
fn broker_rejects_missing_client_certificate() {
    let Some(bs) = common::mtls_bootstrap() else { return };
    let r = test_connection(&profile(&bs, None), &MemoryStore::default());
    assert!(!r.ok);
    let e = r.steps.last().unwrap().error.clone().unwrap();
    // TLS 1.3: отказ брокера приходит алертом после рукопожатия или разрывом соединения.
    assert!(
        matches!(e.code, ErrorCode::TlsError | ErrorCode::ConnectionFailed),
        "{e:?}"
    );
}
```

Run: `source "$HOME/.cargo/env" && KE_IT_MTLS_BOOTSTRAP=localhost:9095 cargo test --manifest-path src-tauri/Cargo.toml --test it_mtls`
Expected: 2 passed. Если `broker_rejects_missing_client_certificate` даёт другой код, записать фактический текст в отчёт и добавить его признак в `tls_message`. Например, `connection reset` при `SSL handshake` — это `TlsError` с сообщением «Брокер не принял клиентский сертификат».

- [ ] **Step 9: Полный прогон**

Run: `source "$HOME/.cargo/env" && KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 KE_IT_MTLS_BOOTSTRAP=localhost:9095 cargo test --manifest-path src-tauri/Cargo.toml --lib --tests && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && cargo fmt --manifest-path src-tauri/Cargo.toml --check`
Expected: всё зелёное.

- [ ] **Step 10: Commit**

```bash
git add src-tauri
git commit -m "feat(tls): клиентские сертификаты в подключении, пароли TLS в keychain, понятные ошибки рукопожатия

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Сертификаты в форме подключения

**Files:**
- Modify: `src/api/types.ts`, `src/api/client.ts`, `src/api/client.test.ts`
- Modify: `src/components/connections/profile.ts`, `profile.test.ts`
- Create: `src/components/connections/TlsSection.tsx`
- Modify: `src/components/connections/ConnectionForm.tsx`, `ConnectionForm.test.tsx`
- Modify: `src/styles.css`

**Interfaces:**
- Consumes: команды `save_connection` / `test_connection` с `tlsPasswords` (Task 5).
- Produces (TS):
  ```ts
  export type CertFormat = "pem" | "pkcs12" | "jks";
  export interface ClientCert { format: CertFormat; certificatePath: string; keyPath: string | null; alias: string | null }
  export interface TlsConfig { caLocation: string | null; caFormat: CertFormat; verifyHostname: boolean; clientCert: ClientCert | null }
  export interface TlsPasswordsInput { keyPassword: string | null; keystorePassword: string | null; truststorePassword: string | null }
  api.saveConnection(profile, saslPassword?: string, tlsPasswords?: TlsPasswordsInput)
  api.testConnection(profile, saslPassword?: string, tlsPasswords?: TlsPasswordsInput)
  ```
- Produces: `formatFromPath(path: string): CertFormat | null`, `newClientCert(): ClientCert` в `profile.ts`. `<TlsSection tls onChange secrets onSecrets isNew />`, `type SecretField = { text: string; clear: boolean }`, `secretValue(f: SecretField): string | null` (`clear` → `""`, текст → текст, иначе `null`).

- [ ] **Step 1: Типы и клиент** — в `src/api/types.ts` заменить `TlsConfig` на типы из Interfaces (плюс `CertFormat`, `ClientCert`, `TlsPasswordsInput`). В `src/api/client.ts`:

```ts
  saveConnection: (profile: ConnectionProfile, saslPassword?: string, tlsPasswords?: TlsPasswordsInput) =>
    call<ConnectionProfile>("save_connection", {
      input: { profile, saslPassword: saslPassword ?? null, tlsPasswords: tlsPasswords ?? null },
    }),
  testConnection: (profile: ConnectionProfile, saslPassword?: string, tlsPasswords?: TlsPasswordsInput) =>
    call<TestReport>("test_connection", {
      input: { profile, saslPassword: saslPassword ?? null, tlsPasswords: tlsPasswords ?? null },
    }),
```

Тест в `client.test.ts`:

```ts
  it("saveConnection передаёт пароли TLS", async () => {
    invoke.mockResolvedValue({});
    const tls = { keyPassword: null, keystorePassword: "s", truststorePassword: "" };
    await api.saveConnection({ id: "a" } as ConnectionProfile, undefined, tls);
    expect(invoke).toHaveBeenCalledWith("save_connection", {
      input: { profile: { id: "a" }, saslPassword: null, tlsPasswords: tls },
    });
  });
```

(Импорт `ConnectionProfile` — из `./types`, если его ещё нет.)

- [ ] **Step 2: Хелперы профиля — тесты** — в `profile.test.ts`:

```ts
import { formatFromPath, newClientCert, withProtocol } from "./profile";

describe("сертификаты", () => {
  it("формат по расширению", () => {
    expect(formatFromPath("/a/client.P12")).toBe("pkcs12");
    expect(formatFromPath("C:\\certs\\c.pfx")).toBe("pkcs12");
    expect(formatFromPath("/a/keystore.jks")).toBe("jks");
    expect(formatFromPath("/a/tls.crt")).toBe("pem");
    expect(formatFromPath("/a/tls.key")).toBe("pem");
    expect(formatFromPath("/a/ca.pem")).toBe("pem");
    expect(formatFromPath("/a/cert")).toBeNull();
  });

  it("SSL по умолчанию — PEM без клиентского сертификата", () => {
    const sec = withProtocol({ protocol: "PLAINTEXT" }, "SSL");
    expect(sec).toEqual({ protocol: "SSL", tls: { caLocation: null, caFormat: "pem", verifyHostname: true, clientCert: null } });
    expect(newClientCert()).toEqual({ format: "pem", certificatePath: "", keyPath: "", alias: null });
  });
});
```

Run: `pnpm vitest run src/components/connections/profile.test.ts`
Expected: FAIL.

- [ ] **Step 3: Хелперы профиля — реализация** — в `profile.ts`:

```ts
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
```

Run: `pnpm vitest run src/components/connections/profile.test.ts`
Expected: PASS.

- [ ] **Step 4: Тесты формы** — в `ConnectionForm.test.tsx`:
  - в начало (до импорта формы) добавить мок диалога:
    ```ts
    vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn() }));
    ```
    и импорт `import { open as openDialog } from "@tauri-apps/plugin-dialog";`;
  - в существующем тесте SASL_SSL ожидаемый `tls` заменить на `{ caLocation: null, caFormat: "pem", verifyHostname: true, clientCert: null }`, а вызов — на `toHaveBeenCalledWith(expect.objectContaining({…}), "secret", { keyPassword: null, keystorePassword: null, truststorePassword: null })`.
  - новые тесты:

```ts
  it("клиентский сертификат PKCS#12: выбор файла, формат по расширению, пароль хранилища", async () => {
    vi.mocked(openDialog).mockResolvedValue("/certs/client.p12");
    vi.mocked(api.saveConnection).mockResolvedValue({ ...newProfile(), id: "n", name: "m" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.type(screen.getByLabelText("Имя"), "m");
    await user.selectOptions(screen.getByLabelText("Протокол"), "SSL");
    await user.click(screen.getByLabelText("Клиентский сертификат (mTLS)"));
    await user.click(screen.getByRole("button", { name: "Выбрать файл: Сертификат клиента" }));
    expect(screen.getByLabelText("Формат клиентского сертификата")).toHaveValue("pkcs12");
    expect(screen.queryByLabelText("Ключ клиента")).not.toBeInTheDocument();
    await user.type(screen.getByLabelText("Пароль хранилища клиента"), "storepass");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        security: {
          protocol: "SSL",
          tls: {
            caLocation: null,
            caFormat: "pem",
            verifyHostname: true,
            clientCert: { format: "pkcs12", certificatePath: "/certs/client.p12", keyPath: "", alias: null },
          },
        },
      }),
      undefined,
      { keyPassword: null, keystorePassword: "storepass", truststorePassword: null },
    );
  });

  it("PEM: поля сертификата и ключа, пароль ключа необязателен; у полей видимые подписи", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Протокол"), "SSL");
    await user.click(screen.getByLabelText("Клиентский сертификат (mTLS)"));
    for (const label of ["Сертификат клиента", "Ключ клиента", "Пароль ключа (если зашифрован)", "Доверенный CA"]) {
      expect(screen.getByText(label, { selector: "label" })).toBeVisible();
    }
  });

  it("существующий профиль: сохранённый пароль можно удалить", async () => {
    vi.mocked(api.saveConnection).mockResolvedValue({ ...newProfile(), id: "c1", name: "x" });
    const initial = {
      ...newProfile(),
      id: "c1",
      name: "x",
      security: {
        protocol: "SSL" as const,
        tls: {
          caLocation: "/ca.jks",
          caFormat: "jks" as const,
          verifyHostname: true,
          clientCert: { format: "jks" as const, certificatePath: "/c.jks", keyPath: null, alias: null },
        },
      },
    };
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    await user.click(screen.getByLabelText("Удалить сохранённый: Пароль ключа (если отличается)"));
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveConnection).toHaveBeenCalledWith(expect.anything(), undefined, {
      keyPassword: "",
      keystorePassword: null,
      truststorePassword: null,
    });
  });
```

Run: `pnpm vitest run src/components/connections`
Expected: FAIL.

- [ ] **Step 5: `TlsSection`** — создать `src/components/connections/TlsSection.tsx`:

```tsx
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
```

- [ ] **Step 6: Подключить в форму** — в `ConnectionForm.tsx`:
  - состояние `const [tlsSecrets, setTlsSecrets] = useState<TlsSecrets>(emptyTlsSecrets);`;
  - хелпер:
    ```ts
    const tlsPasswords = (): TlsPasswordsInput | undefined =>
      "tls" in profile.security
        ? { keyPassword: secretValue(tlsSecrets.key), keystorePassword: secretValue(tlsSecrets.keystore), truststorePassword: secretValue(tlsSecrets.truststore) }
        : undefined;
    ```
  - `api.testConnection(current(), password || undefined, tlsPasswords())` и `api.saveConnection(current(), password || undefined, tlsPasswords())`;
  - блок `{"tls" in sec && (<>…CA и hostname…</>)}` заменить на:
    ```tsx
        {"tls" in sec && (
          <TlsSection
            tls={sec.tls}
            onChange={(tls) => update({ security: { ...sec, tls } })}
            secrets={tlsSecrets}
            onSecrets={setTlsSecrets}
            isNew={isNew}
          />
        )}
    ```

В `styles.css`:

```css
.path-field { display: flex; gap: 6px; align-items: center; }
.path-field > input:not([type="checkbox"]) { flex: 1; min-width: 0; }
label.inline { display: inline-flex; gap: 4px; align-items: center; white-space: nowrap; color: var(--muted); }
```

Run: `pnpm vitest run && pnpm build`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src
git commit -m "feat(ui): клиентский сертификат и доверенный CA в форме подключения

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Идентификаторы продукта и корпоративная политика

**Files:**
- Create: `src-tauri/src/product.rs`
- Modify: `src-tauri/src/secrets.rs` (имя сервиса keychain из `product`)
- Create: `src-tauri/src/policy/mod.rs`, `src-tauri/src/policy/sources.rs`
- Modify: `src-tauri/Cargo.toml` (`plist`, `winreg` для Windows)
- Modify: `src-tauri/src/state.rs` (`policy: Arc<Policy>`, `with_policy`)
- Create: `src-tauri/src/commands/license.rs` (пока только `get_policy_status`)
- Modify: `src-tauri/src/commands/mod.rs`, `src-tauri/src/lib.rs`

**Interfaces:**
- Produces (`product`):
  ```rust
  pub const PRODUCT_NAME: &str = "Kafka Explorer";
  pub const IDENTIFIER: &str = "dev.kafkaexplorer.app";   // совпадает с tauri.conf.json
  pub const KEYCHAIN_SERVICE: &str = "kafka-explorer";
  pub const POLICY_VENDOR: &str = "KafkaExplorer";
  pub const POLICY_PRODUCT: &str = "KafkaExplorer";
  ```
  M3b меняет эти значения при переименовании. Весь остальной код берёт их отсюда.
- Produces (`policy`):
  ```rust
  pub enum PolicyValue { Bool(bool), Int(i64), Str(String), Other(String) }
  pub enum RawPolicy { Absent, Values(BTreeMap<String, PolicyValue>), Unreadable(String) }
  #[derive(Serialize)] #[serde(rename_all = "camelCase")]
  pub struct PolicyStatus { pub managed: bool, pub source: Option<String>, pub updates_enabled: bool,
                            pub update_url: Option<String>, pub has_license_key: bool, pub errors: Vec<String> }
  pub struct Policy { pub status: PolicyStatus, pub license_key: Option<String> }   // ключ не сериализуется
  pub fn resolve(source: &str, raw: RawPolicy) -> Policy;
  // policy::sources
  pub fn load() -> Policy;                       // источник текущей ОС
  pub fn read_json_file(path: &Path) -> RawPolicy;
  pub fn read_plist_file(path: &Path) -> RawPolicy;
  #[cfg(windows)] pub fn read_registry(subkey: &str) -> RawPolicy;
  ```
- Produces: `AppState.policy: Arc<Policy>` (по умолчанию `Policy::default()`: не управляется, обновления включены), `AppState::with_policy(self, Policy) -> Self`. Task 9 дополняет `with_policy` передачей `license_key` в менеджер лицензий.
- Produces: команда `get_policy_status() -> PolicyStatus`.

- [ ] **Step 1: `product.rs` с тестом**

```rust
//! Имя продукта и идентификаторы — в одном месте. Переименование (M3b) меняет только этот файл
//! и `tauri.conf.json`; тест ниже следит, чтобы они совпадали.

pub const PRODUCT_NAME: &str = "Kafka Explorer";
pub const IDENTIFIER: &str = "dev.kafkaexplorer.app";
pub const KEYCHAIN_SERVICE: &str = "kafka-explorer";
pub const POLICY_VENDOR: &str = "KafkaExplorer";
pub const POLICY_PRODUCT: &str = "KafkaExplorer";

#[cfg(test)]
mod tests {
    #[test]
    fn matches_tauri_conf() {
        let conf: serde_json::Value = serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        assert_eq!(conf["identifier"], super::IDENTIFIER);
        assert_eq!(conf["productName"], super::PRODUCT_NAME);
    }
}
```

`lib.rs`: `pub mod product;`. `secrets.rs`: удалить `const SERVICE` и использовать `crate::product::KEYCHAIN_SERVICE`.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib product`
Expected: PASS.

- [ ] **Step 2: Тесты `resolve`** — `src-tauri/src/policy/mod.rs` (сначала тесты):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn values(pairs: &[(&str, PolicyValue)]) -> RawPolicy {
        RawPolicy::Values(pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
    }

    #[test]
    fn absent_policy_is_unmanaged_with_updates() {
        let p = resolve("/etc/x/policy.json", RawPolicy::Absent);
        assert!(!p.status.managed);
        assert!(p.status.updates_enabled);
        assert_eq!(p.license_key, None);
    }

    #[test]
    fn values_are_applied() {
        let p = resolve(
            "src",
            values(&[
                ("updatesEnabled", PolicyValue::Bool(false)),
                ("updateUrl", PolicyValue::Str("https://mirror.corp/latest.json".into())),
                ("licenseKey", PolicyValue::Str(" abc.def ".into())),
            ]),
        );
        assert!(p.status.managed);
        assert_eq!(p.status.source.as_deref(), Some("src"));
        assert!(!p.status.updates_enabled);
        assert_eq!(p.status.update_url.as_deref(), Some("https://mirror.corp/latest.json"));
        assert!(p.status.has_license_key);
        assert_eq!(p.license_key.as_deref(), Some("abc.def"));
        assert!(p.status.errors.is_empty());
    }

    #[test]
    fn registry_dword_is_bool() {
        let p = resolve("src", values(&[("updatesEnabled", PolicyValue::Int(0))]));
        assert!(!p.status.updates_enabled);
        let p = resolve("src", values(&[("updatesEnabled", PolicyValue::Int(1))]));
        assert!(p.status.updates_enabled);
    }

    #[test]
    fn broken_policy_disables_updates() {
        let p = resolve("src", RawPolicy::Unreadable("plist: unexpected EOF".into()));
        assert!(p.status.managed);
        assert!(!p.status.updates_enabled);
        assert!(p.status.errors[0].contains("unexpected EOF"));
        // Неверный тип или адрес — тоже «безопасно закрыто», даже если updatesEnabled = true.
        for bad in [
            values(&[("updatesEnabled", PolicyValue::Str("yes".into()))]),
            values(&[("updatesEnabled", PolicyValue::Bool(true)), ("updateUrl", PolicyValue::Str("ftp://x".into()))]),
        ] {
            let p = resolve("src", bad);
            assert!(!p.status.updates_enabled);
            assert_eq!(p.status.errors.len(), 1, "{:?}", p.status.errors);
        }
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let p = resolve("src", values(&[("futureKey", PolicyValue::Bool(true))]));
        assert!(p.status.managed);
        assert!(p.status.updates_enabled);
        assert!(p.status.errors.is_empty());
    }

    #[test]
    fn license_key_is_not_serialized() {
        let p = resolve("src", values(&[("licenseKey", PolicyValue::Str("secret.key".into()))]));
        let json = serde_json::to_string(&p.status).unwrap();
        assert!(!json.contains("secret.key"));
        assert!(json.contains("\"hasLicenseKey\":true"));
    }
}
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib policy`
Expected: FAIL.

- [ ] **Step 3: Реализация `resolve`** — начало `policy/mod.rs`:

```rust
//! Корпоративная политика (спец. M3 §5): читается один раз при запуске, приоритетнее настроек пользователя.

pub mod sources;

use std::collections::BTreeMap;

use serde::Serialize;

pub const KEY_UPDATES_ENABLED: &str = "updatesEnabled";
pub const KEY_UPDATE_URL: &str = "updateUrl";
pub const KEY_LICENSE_KEY: &str = "licenseKey";

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyValue {
    Bool(bool),
    Int(i64),
    Str(String),
    /// Значение неподдерживаемого типа (описание типа).
    Other(String),
}

impl PolicyValue {
    fn describe(&self) -> String {
        match self {
            PolicyValue::Bool(b) => format!("логическое {b}"),
            PolicyValue::Int(i) => format!("число {i}"),
            PolicyValue::Str(s) => format!("строка «{s}»"),
            PolicyValue::Other(t) => format!("значение типа {t}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RawPolicy {
    Absent,
    Values(BTreeMap<String, PolicyValue>),
    Unreadable(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyStatus {
    pub managed: bool,
    pub source: Option<String>,
    pub updates_enabled: bool,
    pub update_url: Option<String>,
    pub has_license_key: bool,
    pub errors: Vec<String>,
}

impl Default for PolicyStatus {
    fn default() -> Self {
        Self {
            managed: false,
            source: None,
            updates_enabled: true,
            update_url: None,
            has_license_key: false,
            errors: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Policy {
    pub status: PolicyStatus,
    /// Ключ организации; в интерфейс не отдаётся.
    pub license_key: Option<String>,
}

/// Политика, которую задал администратор, но которую не удалось понять, выключает обновления:
/// раз политика есть, администратор хотел ограничений.
pub fn resolve(source: &str, raw: RawPolicy) -> Policy {
    let map = match raw {
        RawPolicy::Absent => return Policy::default(),
        RawPolicy::Unreadable(e) => {
            return Policy {
                status: PolicyStatus {
                    managed: true,
                    source: Some(source.to_string()),
                    updates_enabled: false,
                    errors: vec![format!("Политика не читается: {e}")],
                    ..PolicyStatus::default()
                },
                license_key: None,
            };
        }
        RawPolicy::Values(map) => map,
    };
    let mut status = PolicyStatus { managed: true, source: Some(source.to_string()), ..PolicyStatus::default() };
    let mut license_key = None;
    let mut broken = false;
    for (key, value) in map {
        match (key.as_str(), value) {
            (KEY_UPDATES_ENABLED, PolicyValue::Bool(b)) => status.updates_enabled = b,
            (KEY_UPDATES_ENABLED, PolicyValue::Int(i @ (0 | 1))) => status.updates_enabled = i == 1,
            (KEY_UPDATE_URL, PolicyValue::Str(u)) if u.starts_with("https://") || u.starts_with("http://") => {
                status.update_url = Some(u)
            }
            (KEY_LICENSE_KEY, PolicyValue::Str(s)) if !s.trim().is_empty() => {
                license_key = Some(s.trim().to_string());
                status.has_license_key = true;
            }
            (KEY_UPDATES_ENABLED | KEY_UPDATE_URL | KEY_LICENSE_KEY, other) => {
                broken = true;
                status.errors.push(format!("{key}: недопустимое {}", other.describe()));
            }
            (unknown, _) => tracing::warn!("неизвестный ключ политики «{unknown}» проигнорирован"),
        }
    }
    if broken {
        status.updates_enabled = false;
    }
    Policy { status, license_key }
}
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib policy`
Expected: PASS (кроме тестов `sources`, которых ещё нет).

- [ ] **Step 4: Источники — тесты** — `src-tauri/src/policy/sources.rs` (сначала тесты):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::PolicyValue;

    #[test]
    fn json_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.json");
        assert_eq!(read_json_file(&path), RawPolicy::Absent);
        std::fs::write(&path, r#"{"updatesEnabled": false, "updateUrl": "https://m/l.json", "x": [1]}"#).unwrap();
        let RawPolicy::Values(v) = read_json_file(&path) else { panic!() };
        assert_eq!(v["updatesEnabled"], PolicyValue::Bool(false));
        assert_eq!(v["updateUrl"], PolicyValue::Str("https://m/l.json".into()));
        assert!(matches!(v["x"], PolicyValue::Other(_)));
        std::fs::write(&path, "{broken").unwrap();
        assert!(matches!(read_json_file(&path), RawPolicy::Unreadable(_)));
        std::fs::write(&path, "[1, 2]").unwrap();
        assert!(matches!(read_json_file(&path), RawPolicy::Unreadable(_)));
    }

    #[test]
    fn plist_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.plist");
        assert_eq!(read_plist_file(&path), RawPolicy::Absent);
        std::fs::write(
            &path,
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>updatesEnabled</key><false/>
  <key>licenseKey</key><string>a.b</string>
  <key>n</key><integer>3</integer>
</dict></plist>"#,
        )
        .unwrap();
        let RawPolicy::Values(v) = read_plist_file(&path) else { panic!() };
        assert_eq!(v["updatesEnabled"], PolicyValue::Bool(false));
        assert_eq!(v["licenseKey"], PolicyValue::Str("a.b".into()));
        assert_eq!(v["n"], PolicyValue::Int(3));
        std::fs::write(&path, "not a plist").unwrap();
        assert!(matches!(read_plist_file(&path), RawPolicy::Unreadable(_)));
    }

    #[test]
    fn load_on_this_machine_does_not_panic() {
        // На машине разработчика политики обычно нет; проверяем только, что чтение не падает.
        let _ = load();
    }
}
```

`Cargo.toml`:

```toml
plist = "1"

[target.'cfg(windows)'.dependencies]
winreg = "0.55"
```

(Если `winreg 0.55` недоступна, взять актуальную 0.x и поправить импорты по её документации.)

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib policy::sources`
Expected: FAIL.

- [ ] **Step 5: Источники — реализация** — начало `sources.rs`:

```rust
//! Где ОС хранит политику администратора. Пользовательских уровней нет: политику задаёт только администратор.

use std::collections::BTreeMap;
use std::path::Path;

use super::{Policy, PolicyValue, RawPolicy, resolve};
use crate::product::{IDENTIFIER, POLICY_PRODUCT, POLICY_VENDOR};

/// Политика текущей ОС. Вызывается один раз при запуске.
pub fn load() -> Policy {
    #[cfg(target_os = "macos")]
    {
        let path = format!("/Library/Managed Preferences/{IDENTIFIER}.plist");
        resolve(&path, read_plist_file(Path::new(&path)))
    }
    #[cfg(windows)]
    {
        let sub = format!(r"Software\Policies\{POLICY_VENDOR}\{POLICY_PRODUCT}");
        resolve(&format!(r"HKLM\{sub}"), read_registry(&sub))
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let path = format!("/etc/{IDENTIFIER}/policy.json");
        resolve(&path, read_json_file(Path::new(&path)))
    }
}

pub fn read_json_file(path: &Path) -> RawPolicy {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return RawPolicy::Absent,
        Err(e) => return RawPolicy::Unreadable(e.to_string()),
    };
    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => return RawPolicy::Unreadable(e.to_string()),
    };
    let Some(obj) = value.as_object() else {
        return RawPolicy::Unreadable("ожидается JSON-объект".into());
    };
    RawPolicy::Values(
        obj.iter()
            .map(|(k, v)| {
                let pv = match v {
                    serde_json::Value::Bool(b) => PolicyValue::Bool(*b),
                    serde_json::Value::String(s) => PolicyValue::Str(s.clone()),
                    serde_json::Value::Number(n) if n.is_i64() => PolicyValue::Int(n.as_i64().unwrap_or_default()),
                    other => PolicyValue::Other(json_type(other).into()),
                };
                (k.clone(), pv)
            })
            .collect(),
    )
}

fn json_type(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Number(_) => "дробное число",
        serde_json::Value::Array(_) => "массив",
        serde_json::Value::Object(_) => "объект",
        _ => "неизвестный",
    }
}

pub fn read_plist_file(path: &Path) -> RawPolicy {
    if !path.exists() {
        return RawPolicy::Absent;
    }
    let value = match plist::Value::from_file(path) {
        Ok(v) => v,
        Err(e) => return RawPolicy::Unreadable(e.to_string()),
    };
    let Some(dict) = value.as_dictionary() else {
        return RawPolicy::Unreadable("ожидается словарь (dict)".into());
    };
    RawPolicy::Values(
        dict.iter()
            .map(|(k, v)| {
                let pv = match v {
                    plist::Value::Boolean(b) => PolicyValue::Bool(*b),
                    plist::Value::String(s) => PolicyValue::Str(s.clone()),
                    plist::Value::Integer(i) => match i.as_signed() {
                        Some(n) => PolicyValue::Int(n),
                        None => PolicyValue::Other("большое целое".into()),
                    },
                    _ => PolicyValue::Other("неподдерживаемый тип plist".into()),
                };
                (k.clone(), pv)
            })
            .collect::<BTreeMap<_, _>>(),
    )
}

#[cfg(windows)]
pub fn read_registry(subkey: &str) -> RawPolicy {
    use winreg::RegKey;
    use winreg::enums::{HKEY_LOCAL_MACHINE, REG_DWORD, REG_EXPAND_SZ, REG_SZ};
    use winreg::types::FromRegValue;

    let key = match RegKey::predef(HKEY_LOCAL_MACHINE).open_subkey(subkey) {
        Ok(k) => k,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return RawPolicy::Absent,
        Err(e) => return RawPolicy::Unreadable(e.to_string()),
    };
    let mut map = BTreeMap::new();
    for item in key.enum_values() {
        let (name, value) = match item {
            Ok(v) => v,
            Err(e) => return RawPolicy::Unreadable(e.to_string()),
        };
        let parsed = match value.vtype {
            REG_DWORD => u32::from_reg_value(&value).map(|d| PolicyValue::Int(i64::from(d))),
            REG_SZ | REG_EXPAND_SZ => String::from_reg_value(&value).map(PolicyValue::Str),
            other => Ok(PolicyValue::Other(format!("{other:?}"))),
        };
        match parsed {
            Ok(v) => {
                map.insert(name, v);
            }
            Err(e) => return RawPolicy::Unreadable(e.to_string()),
        }
    }
    RawPolicy::Values(map)
}
```

Неиспользуемые на текущей ОС импорты (`POLICY_VENDOR` на macOS и т. п.) пометить `#[cfg_attr(...)]`/`#[allow(unused_imports)]` или вынести в соответствующий блок `cfg`, чтобы clippy с `-D warnings` проходил на всех ОС.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib policy`
Expected: PASS.

- [ ] **Step 6: Состояние и команда** — в `state.rs`:
  - поле `pub policy: Arc<crate::policy::Policy>,`, в `new` — `policy: Arc::new(crate::policy::Policy::default()),`;
  - метод:
    ```rust
    /// Политика администратора (читается при запуске в lib.rs).
    pub fn with_policy(mut self, policy: crate::policy::Policy) -> Self {
        self.policy = Arc::new(policy);
        self
    }
    ```

`src-tauri/src/commands/license.rs`:

```rust
use tauri::State;

use crate::error::AppResult;
use crate::policy::PolicyStatus;
use crate::state::AppState;

#[tauri::command]
pub fn get_policy_status(state: State<'_, AppState>) -> AppResult<PolicyStatus> {
    Ok(state.policy.status.clone())
}
```

`commands/mod.rs`: `pub mod license;`. `lib.rs`: в `setup` —
`app.manage(AppState::new(&dir, Arc::new(KeyringStore))?.with_policy(crate::policy::sources::load()));`, плюс `pub mod policy;`; в `generate_handler!` — `commands::license::get_policy_status`.

В `setup`, после загрузки, записать в лог: `tracing::info!(managed = …, updates = …, "политика")`. Ключ лицензии в лог не пишется.

- [ ] **Step 7: Полный прогон и commit**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && cargo fmt --manifest-path src-tauri/Cargo.toml --check`
Expected: зелёное.

```bash
git add src-tauri
git commit -m "feat(policy): корпоративная политика из plist, реестра и /etc; идентификаторы продукта в одном месте

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Формат лицензии и утилита выдачи

**Files:**
- Create: `crates/license-format/Cargo.toml`, `crates/license-format/src/lib.rs`
- Create: `crates/license-format/testdata/dev-signing.key`, `dev-public.key` (через утилиту, Step 4)
- Create: `tools/issue-license/Cargo.toml`, `tools/issue-license/src/main.rs`
- Create: `src-tauri/license-public.key` (= dev-public.key на время разработки)
- Modify: `.gitignore`, `.github/workflows/ci.yml`

**Interfaces:**
- Produces (`license_format`):
  ```rust
  pub const FORMAT_VERSION: u32 = 1;
  #[serde(rename_all = "lowercase")] pub enum Tier { Pro }
  #[serde(rename_all = "lowercase")] pub enum Kind { Personal, Organization }
  pub struct Payload { pub v: u32, pub id: String, pub tier: Tier, pub kind: Kind, pub licensee: String,
                       pub issued: NaiveDate, pub expires: Option<NaiveDate>, pub seats: Option<u32> }
  pub enum LicenseError { Malformed(String), BadSignature, UnsupportedVersion(u32) }   // impl Display (по-русски)
  pub fn sign(payload: &Payload, key: &SigningKey) -> String;
  pub fn verify(token: &str, key: &VerifyingKey) -> Result<Payload, LicenseError>;
  pub fn encode_verifying_key(k: &VerifyingKey) -> String;  pub fn parse_verifying_key(s: &str) -> Result<VerifyingKey, LicenseError>;
  pub fn encode_signing_key(k: &SigningKey) -> String;      pub fn parse_signing_key(s: &str) -> Result<SigningKey, LicenseError>;
  pub use ed25519_dalek::{SigningKey, VerifyingKey};
  #[cfg(feature = "dev-keys")] pub mod dev { pub fn signing_key() -> SigningKey; pub fn verifying_key() -> VerifyingKey;
      pub fn pro_token(licensee: &str, expires: Option<NaiveDate>) -> String; }
  ```
  Токен: `base64url(JSON payload) "." base64url(подпись 64 байта)`, base64url без `=`. Ключи: base64url от 32 байт.
- Produces: `issue-license keygen --out <dir>`, `issue-license issue --key <file> --kind personal|organization --licensee <имя> [--expires YYYY-MM-DD] [--seats N]`, `issue-license verify --public <file> <token>`.

- [ ] **Step 1: Crate — тесты** — `crates/license-format/Cargo.toml`:

```toml
[package]
name = "license-format"
version = "0.1.0"
edition = "2024"
publish = false

[features]
dev-keys = []

[dependencies]
base64 = "0.22"
chrono = { version = "0.4", default-features = false, features = ["std", "serde"] }
ed25519-dalek = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

`crates/license-format/src/lib.rs` — тесты:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn payload() -> Payload {
        Payload {
            v: FORMAT_VERSION,
            id: "lic-1".into(),
            tier: Tier::Pro,
            kind: Kind::Organization,
            licensee: "ACME".into(),
            issued: NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(),
            expires: NaiveDate::from_ymd_opt(2027, 3, 31),
            seats: Some(500),
        }
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let k = key(1);
        let token = sign(&payload(), &k);
        assert_eq!(token.matches('.').count(), 1);
        assert!(!token.contains('='));
        assert_eq!(verify(&token, &k.verifying_key()).unwrap(), payload());
        assert_eq!(verify(&format!("  {token}\n"), &k.verifying_key()).unwrap(), payload());
    }

    #[test]
    fn foreign_key_and_tampering_fail() {
        let token = sign(&payload(), &key(1));
        assert_eq!(verify(&token, &key(2).verifying_key()), Err(LicenseError::BadSignature));
        let (p, s) = token.split_once('.').unwrap();
        let mut json = String::from_utf8(B64.decode(p).unwrap()).unwrap();
        json = json.replace("ACME", "EVIL");
        let forged = format!("{}.{s}", B64.encode(json));
        assert_eq!(verify(&forged, &key(1).verifying_key()), Err(LicenseError::BadSignature));
    }

    #[test]
    fn malformed_tokens() {
        let vk = key(1).verifying_key();
        for t in ["", "nodot", "@@@.@@@", "e30.AAAA"] {
            assert!(matches!(verify(t, &vk), Err(LicenseError::Malformed(_))), "{t}");
        }
    }

    #[test]
    fn newer_format_version_is_reported() {
        let k = key(1);
        let json = br#"{"v":2,"id":"x"}"#;
        let token = format!("{}.{}", B64.encode(json), B64.encode(k.sign(json).to_bytes()));
        assert_eq!(verify(&token, &k.verifying_key()), Err(LicenseError::UnsupportedVersion(2)));
    }

    #[test]
    fn keys_roundtrip_through_text() {
        let k = key(7);
        let sk = parse_signing_key(&format!("{}\n", encode_signing_key(&k))).unwrap();
        assert_eq!(sk.to_bytes(), k.to_bytes());
        let vk = parse_verifying_key(&encode_verifying_key(&k.verifying_key())).unwrap();
        assert_eq!(vk, k.verifying_key());
        assert!(parse_verifying_key("short").is_err());
    }

    #[test]
    fn payload_json_shape() {
        let v = serde_json::to_value(payload()).unwrap();
        assert_eq!(v["tier"], "pro");
        assert_eq!(v["kind"], "organization");
        assert_eq!(v["expires"], "2027-03-31");
    }
}
```

Run: `cargo test --manifest-path crates/license-format/Cargo.toml`
Expected: FAIL.

- [ ] **Step 2: Crate — реализация** — начало `lib.rs`:

```rust
//! Формат лицензионного ключа: base64url(JSON) "." base64url(подпись Ed25519).
//! Общий для приложения (проверка) и утилиты issue-license (выдача).

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
pub use chrono::NaiveDate;
use ed25519_dalek::{Signature, Signer, Verifier};
pub use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Pro,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Personal,
    Organization,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payload {
    pub v: u32,
    pub id: String,
    pub tier: Tier,
    pub kind: Kind,
    pub licensee: String,
    pub issued: NaiveDate,
    pub expires: Option<NaiveDate>,
    pub seats: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LicenseError {
    Malformed(String),
    BadSignature,
    UnsupportedVersion(u32),
}

impl std::fmt::Display for LicenseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LicenseError::Malformed(m) => write!(f, "ключ повреждён ({m})"),
            LicenseError::BadSignature => write!(f, "подпись ключа не подтверждена — ключ выдан не нами или изменён"),
            LicenseError::UnsupportedVersion(v) => {
                write!(f, "ключ версии {v} требует более новой версии приложения")
            }
        }
    }
}

impl std::error::Error for LicenseError {}

fn malformed(m: &str) -> LicenseError {
    LicenseError::Malformed(m.to_string())
}

pub fn sign(payload: &Payload, key: &SigningKey) -> String {
    let json = serde_json::to_vec(payload).expect("Payload сериализуется");
    let sig = key.sign(&json);
    format!("{}.{}", B64.encode(&json), B64.encode(sig.to_bytes()))
}

pub fn verify(token: &str, key: &VerifyingKey) -> Result<Payload, LicenseError> {
    let (p, s) = token.trim().split_once('.').ok_or_else(|| malformed("нет разделителя «.»"))?;
    let json = B64.decode(p).map_err(|_| malformed("данные не в base64url"))?;
    let sig = B64.decode(s).map_err(|_| malformed("подпись не в base64url"))?;
    let sig = Signature::from_slice(&sig).map_err(|_| malformed("подпись неверной длины"))?;
    key.verify(&json, &sig).map_err(|_| LicenseError::BadSignature)?;
    let value: serde_json::Value = serde_json::from_slice(&json).map_err(|_| malformed("данные не JSON"))?;
    let v = value.get("v").and_then(serde_json::Value::as_u64).ok_or_else(|| malformed("нет версии"))?;
    if v != u64::from(FORMAT_VERSION) {
        return Err(LicenseError::UnsupportedVersion(v as u32));
    }
    serde_json::from_value(value).map_err(|e| LicenseError::Malformed(e.to_string()))
}

fn key_bytes(s: &str) -> Result<[u8; 32], LicenseError> {
    B64.decode(s.trim())
        .ok()
        .and_then(|b| <[u8; 32]>::try_from(b).ok())
        .ok_or_else(|| malformed("ключ должен быть base64url от 32 байт"))
}

pub fn encode_verifying_key(k: &VerifyingKey) -> String {
    B64.encode(k.as_bytes())
}

pub fn parse_verifying_key(s: &str) -> Result<VerifyingKey, LicenseError> {
    VerifyingKey::from_bytes(&key_bytes(s)?).map_err(|_| malformed("неверный открытый ключ"))
}

pub fn encode_signing_key(k: &SigningKey) -> String {
    B64.encode(k.to_bytes())
}

pub fn parse_signing_key(s: &str) -> Result<SigningKey, LicenseError> {
    Ok(SigningKey::from_bytes(&key_bytes(s)?))
}

/// Ключи разработки: подписывают тестовые лицензии. В сборку приложения не попадают
/// (feature включается только в dev-dependencies).
#[cfg(feature = "dev-keys")]
pub mod dev {
    use super::*;

    const SIGNING: &str = include_str!("../testdata/dev-signing.key");
    const PUBLIC: &str = include_str!("../testdata/dev-public.key");

    pub fn signing_key() -> SigningKey {
        parse_signing_key(SIGNING).expect("dev-signing.key")
    }

    pub fn verifying_key() -> VerifyingKey {
        parse_verifying_key(PUBLIC).expect("dev-public.key")
    }

    pub fn pro_token(licensee: &str, expires: Option<NaiveDate>) -> String {
        sign(
            &Payload {
                v: FORMAT_VERSION,
                id: "dev".into(),
                tier: Tier::Pro,
                kind: Kind::Personal,
                licensee: licensee.into(),
                issued: NaiveDate::from_ymd_opt(2026, 1, 1).expect("дата"),
                expires,
                seats: None,
            },
            &signing_key(),
        )
    }
}
```

Run: `cargo test --manifest-path crates/license-format/Cargo.toml`
Expected: PASS.

- [ ] **Step 3: Утилита** — `tools/issue-license/Cargo.toml`:

```toml
[package]
name = "issue-license"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
license-format = { path = "../../crates/license-format" }
ed25519-dalek = { version = "2", features = ["rand_core"] }
rand = "0.8"
chrono = { version = "0.4", default-features = false, features = ["std", "clock"] }
clap = { version = "4", features = ["derive"] }

[dev-dependencies]
tempfile = "3"
```

`tools/issue-license/src/main.rs`:

```rust
//! Утилита владельца: генерация ключей подписи, выдача и проверка лицензий.
//! Закрытый ключ хранится только у владельца и никогда не коммитится.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use license_format::{
    FORMAT_VERSION, Kind, NaiveDate, Payload, SigningKey, Tier, encode_signing_key, encode_verifying_key,
    parse_signing_key, parse_verifying_key, sign, verify,
};

#[derive(Parser)]
#[command(about = "Выдача лицензий Pro")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum KindArg {
    Personal,
    Organization,
}

#[derive(Subcommand)]
enum Command {
    /// Создать пару ключей: <out>/license-signing.key (секрет) и <out>/license-public.key.
    Keygen {
        #[arg(long)]
        out: PathBuf,
    },
    /// Выдать лицензию и напечатать ключ.
    Issue {
        #[arg(long)]
        key: PathBuf,
        #[arg(long, value_enum)]
        kind: KindArg,
        #[arg(long)]
        licensee: String,
        /// Последний день действия, YYYY-MM-DD. Без него — бессрочно.
        #[arg(long)]
        expires: Option<NaiveDate>,
        #[arg(long)]
        seats: Option<u32>,
    },
    /// Проверить ключ и напечатать его содержимое.
    Verify {
        #[arg(long)]
        public: PathBuf,
        token: String,
    },
}

fn keygen(out: &Path) -> Result<String, String> {
    let signing = out.join("license-signing.key");
    let public = out.join("license-public.key");
    if signing.exists() || public.exists() {
        return Err(format!("{} уже содержит ключи — не перезаписываю", out.display()));
    }
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let key = SigningKey::generate(&mut rand::rngs::OsRng);
    std::fs::write(&signing, encode_signing_key(&key) + "\n").map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&signing, std::fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    }
    std::fs::write(&public, encode_verifying_key(&key.verifying_key()) + "\n").map_err(|e| e.to_string())?;
    Ok(format!(
        "Закрытый ключ: {} (храните в секрете, не коммитьте)\nОткрытый ключ: {} (скопируйте в src-tauri/license-public.key)",
        signing.display(),
        public.display()
    ))
}

fn issue(key: &Path, kind: KindArg, licensee: &str, expires: Option<NaiveDate>, seats: Option<u32>, today: NaiveDate) -> Result<String, String> {
    let text = std::fs::read_to_string(key).map_err(|e| format!("{}: {e}", key.display()))?;
    let key = parse_signing_key(&text).map_err(|e| e.to_string())?;
    if licensee.trim().is_empty() {
        return Err("--licensee не может быть пустым".into());
    }
    if expires.is_some_and(|d| d < today) {
        return Err("--expires в прошлом".into());
    }
    let payload = Payload {
        v: FORMAT_VERSION,
        id: format!("{}-{:08x}", today.format("%Y%m%d"), rand::random::<u32>()),
        tier: Tier::Pro,
        kind: match kind {
            KindArg::Personal => Kind::Personal,
            KindArg::Organization => Kind::Organization,
        },
        licensee: licensee.trim().to_string(),
        issued: today,
        expires,
        seats,
    };
    Ok(sign(&payload, &key))
}

fn verify_cmd(public: &Path, token: &str) -> Result<String, String> {
    let text = std::fs::read_to_string(public).map_err(|e| format!("{}: {e}", public.display()))?;
    let key = parse_verifying_key(&text).map_err(|e| e.to_string())?;
    let payload = verify(token, &key).map_err(|e| e.to_string())?;
    Ok(format!("Подпись верна.\n{}", describe(&payload)))
}

fn describe(p: &Payload) -> String {
    format!(
        "id: {}\nтариф: pro\nтип: {:?}\nвладелец: {}\nвыдан: {}\nдействует до: {}\nмест: {}",
        p.id,
        p.kind,
        p.licensee,
        p.issued,
        p.expires.map_or("бессрочно".to_string(), |d| d.to_string()),
        p.seats.map_or("—".to_string(), |s| s.to_string()),
    )
}

fn main() {
    let cli = Cli::parse();
    let today = chrono::Local::now().date_naive();
    let result = match cli.command {
        Command::Keygen { out } => keygen(&out),
        Command::Issue { key, kind, licensee, expires, seats } => issue(&key, kind, &licensee, expires, seats, today),
        Command::Verify { public, token } => verify_cmd(&public, &token),
    };
    match result {
        Ok(out) => println!("{out}"),
        Err(e) => {
            eprintln!("Ошибка: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keygen_issue_verify() {
        let dir = tempfile::tempdir().unwrap();
        keygen(dir.path()).unwrap();
        assert!(keygen(dir.path()).is_err(), "не перезаписывает");
        let today = NaiveDate::from_ymd_opt(2026, 10, 16).unwrap();
        let token = issue(
            &dir.path().join("license-signing.key"),
            KindArg::Organization,
            "ACME",
            NaiveDate::from_ymd_opt(2027, 3, 31),
            Some(500),
            today,
        )
        .unwrap();
        let out = verify_cmd(&dir.path().join("license-public.key"), &token).unwrap();
        assert!(out.contains("ACME") && out.contains("2027-03-31") && out.contains("500"), "{out}");
    }

    #[test]
    fn rejects_past_expiry_and_empty_licensee() {
        let dir = tempfile::tempdir().unwrap();
        keygen(dir.path()).unwrap();
        let key = dir.path().join("license-signing.key");
        let today = NaiveDate::from_ymd_opt(2026, 10, 16).unwrap();
        assert!(issue(&key, KindArg::Personal, "a@b", NaiveDate::from_ymd_opt(2026, 1, 1), None, today).is_err());
        assert!(issue(&key, KindArg::Personal, "  ", None, None, today).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn signing_key_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        keygen(dir.path()).unwrap();
        let mode = std::fs::metadata(dir.path().join("license-signing.key")).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
```

Run: `cargo test --manifest-path tools/issue-license/Cargo.toml`
Expected: PASS.

- [ ] **Step 4: Ключи разработки**

Run:
```bash
cargo run -q --manifest-path tools/issue-license/Cargo.toml -- keygen --out crates/license-format/testdata
mv crates/license-format/testdata/license-signing.key crates/license-format/testdata/dev-signing.key
mv crates/license-format/testdata/license-public.key crates/license-format/testdata/dev-public.key
cp crates/license-format/testdata/dev-public.key src-tauri/license-public.key
```

Создать `crates/license-format/testdata/README.md`:

```markdown
Ключи разработки: подписывают тестовые лицензии и вшиты в dev-сборки.
Перед первым релизом владелец генерирует настоящую пару (`issue-license keygen`)
и заменяет `src-tauri/license-public.key`; CI релиза не пропустит dev-ключ (M3b).
```

`.gitignore` — добавить:

```
# Закрытые ключи лицензий (кроме тестовых dev-ключей)
license-signing.key
tools/issue-license/target
crates/license-format/target
```

Run: `cargo test --manifest-path crates/license-format/Cargo.toml --all-features`
Expected: PASS. Модуль `dev` компилируется.

- [ ] **Step 5: CI** — в `.github/workflows/ci.yml`, job `linux`, после `cargo clippy`:

```yaml
      - name: Лицензии (формат и утилита)
        run: |
          cargo test --manifest-path crates/license-format/Cargo.toml --all-features
          cargo test --manifest-path tools/issue-license/Cargo.toml
          cargo clippy --manifest-path crates/license-format/Cargo.toml --all-features -- -D warnings
          cargo clippy --manifest-path tools/issue-license/Cargo.toml -- -D warnings
      - name: Закрытые ключи лицензий не в репозитории
        run: |
          if git ls-files | grep -E '(^|/)license-signing\.key$'; then
            echo "::error::в репозитории закрытый ключ лицензий"; exit 1
          fi
```

- [ ] **Step 6: Commit**

```bash
git add crates tools src-tauri/license-public.key .gitignore .github/workflows/ci.yml
git commit -m "feat(license): формат ключа Ed25519 и утилита issue-license

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Лицензия в приложении и ограничения Pro

**Files:**
- Modify: `src-tauri/Cargo.toml` (`license-format` в зависимостях и, с `dev-keys`, в dev-dependencies)
- Create: `src-tauri/src/license/mod.rs`
- Modify: `src-tauri/src/error.rs` (`ErrorCode::ProRequired`)
- Modify: `src-tauri/src/state.rs` (`license`, `with_policy`)
- Modify: `src-tauri/src/commands/license.rs` (команды лицензии)
- Modify: `src-tauri/src/commands/consume.rs` (`require_pro`)
- Modify: `src-tauri/src/commands/connections.rs` (prod требует Pro)
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/tests/it_consume.rs` (Pro-лицензия в тесте)

**Interfaces:**
- Consumes: `license_format::{verify, Kind, NaiveDate, VerifyingKey, parse_verifying_key}`, `license_format::dev::*` (только в тестах); `Policy.license_key` (Task 7).
- Produces:
  ```rust
  #[serde(rename_all = "lowercase")] pub enum Tier { Free, Pro }
  #[serde(rename_all = "lowercase")] pub enum LicenseState { None, Valid, Expiring, Expired, Invalid }
  #[serde(rename_all = "lowercase")] pub enum LicenseSource { Policy, User }
  #[derive(Serialize)] #[serde(rename_all = "camelCase")]
  pub struct LicenseStatus { pub tier: Tier, pub state: LicenseState, pub source: Option<LicenseSource>,
      pub licensee: Option<String>, pub kind: Option<Kind>, pub expires: Option<NaiveDate>,
      pub days_left: Option<i64>, pub error: Option<String> }
  pub const EXPIRY_WARNING_DAYS: i64 = 14;
  pub fn evaluate(token: Option<&str>, source: Option<LicenseSource>, key: &VerifyingKey, today: NaiveDate) -> LicenseStatus;
  pub struct LicenseManager;  // open(dir, key), embedded_key(), set_policy_key, status, status_on, set_user_key, remove_user_key, require_pro
  ```
  JSON: `{"tier":"pro","state":"expiring","source":"user","licensee":"a@b","kind":"personal","expires":"2026-10-20","daysLeft":4,"error":null}`.
- Produces: `ErrorCode::ProRequired` → `"PRO_REQUIRED"`, `retryable: false`.
- Produces: команды `get_license_status() -> LicenseStatus`, `set_license_key(key: String) -> LicenseStatus`, `remove_license_key() -> LicenseStatus`.
- Produces: `AppState.license: Arc<LicenseManager>`. `AppState::with_policy` передаёт `policy.license_key` в `license.set_policy_key`.

- [ ] **Step 1: Зависимости** — `src-tauri/Cargo.toml`:

```toml
[dependencies]
license-format = { path = "../crates/license-format" }

[dev-dependencies]
license-format = { path = "../crates/license-format", features = ["dev-keys"] }
```

- [ ] **Step 2: Тесты лицензии** — `src-tauri/src/license/mod.rs` (сначала тесты):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use license_format::dev;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn evaluate_states() {
        let key = dev::verifying_key();
        let today = d(2026, 10, 16);
        assert_eq!(evaluate(None, None, &key, today).tier, Tier::Free);
        assert_eq!(evaluate(None, None, &key, today).state, LicenseState::None);

        let forever = dev::pro_token("a@b", None);
        let s = evaluate(Some(&forever), Some(LicenseSource::User), &key, today);
        assert_eq!((s.tier, s.state), (Tier::Pro, LicenseState::Valid));
        assert_eq!(s.licensee.as_deref(), Some("a@b"));

        let soon = dev::pro_token("a@b", Some(d(2026, 10, 29)));
        let s = evaluate(Some(&soon), Some(LicenseSource::User), &key, today);
        assert_eq!((s.tier, s.state, s.days_left), (Tier::Pro, LicenseState::Expiring, Some(13)));

        // Последний день действия включителен.
        let last_day = dev::pro_token("a@b", Some(today));
        assert_eq!(evaluate(Some(&last_day), None, &key, today).tier, Tier::Pro);

        let past = dev::pro_token("a@b", Some(d(2026, 10, 15)));
        let s = evaluate(Some(&past), None, &key, today);
        assert_eq!((s.tier, s.state), (Tier::Free, LicenseState::Expired));
        assert_eq!(s.expires, Some(d(2026, 10, 15)));

        let s = evaluate(Some("garbage"), None, &key, today);
        assert_eq!((s.tier, s.state), (Tier::Free, LicenseState::Invalid));
        assert!(s.error.unwrap().contains("повреждён"));
    }

    fn manager(dir: &std::path::Path) -> LicenseManager {
        LicenseManager::open(dir, dev::verifying_key())
    }

    #[test]
    fn user_key_is_persisted_and_removable() {
        let dir = tempfile::tempdir().unwrap();
        let m = manager(dir.path());
        assert_eq!(m.status().tier, Tier::Free);
        assert_eq!(m.require_pro().unwrap_err().code, ErrorCode::ProRequired);
        let st = m.set_user_key(&dev::pro_token("a@b", None)).unwrap();
        assert_eq!((st.tier, st.source), (Tier::Pro, Some(LicenseSource::User)));
        assert!(m.require_pro().is_ok());
        assert_eq!(manager(dir.path()).status().tier, Tier::Pro, "ключ сохранён на диск");
        assert_eq!(m.remove_user_key().unwrap().tier, Tier::Free);
        assert_eq!(manager(dir.path()).status().tier, Tier::Free);
    }

    #[test]
    fn invalid_or_expired_keys_are_not_saved() {
        let dir = tempfile::tempdir().unwrap();
        let m = manager(dir.path());
        assert_eq!(m.set_user_key("garbage").unwrap_err().code, ErrorCode::InvalidInput);
        let past = dev::pro_token("a@b", Some(d(2020, 1, 1)));
        let e = m.set_user_key(&past).unwrap_err();
        assert!(e.message.contains("истёк"), "{}", e.message);
        assert!(!dir.path().join("license.json").exists());
    }

    #[test]
    fn policy_key_wins_and_locks_user_changes() {
        let dir = tempfile::tempdir().unwrap();
        let m = manager(dir.path());
        m.set_user_key(&dev::pro_token("user", None)).unwrap();
        m.set_policy_key(Some(dev::pro_token("ACME", None)));
        let st = m.status();
        assert_eq!(st.source, Some(LicenseSource::Policy));
        assert_eq!(st.licensee.as_deref(), Some("ACME"));
        assert_eq!(m.set_user_key(&dev::pro_token("x", None)).unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(m.remove_user_key().unwrap_err().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn expired_message_names_date() {
        let dir = tempfile::tempdir().unwrap();
        let m = manager(dir.path());
        m.set_policy_key(Some(dev::pro_token("ACME", Some(d(2020, 1, 1)))));
        let e = m.require_pro().unwrap_err();
        assert_eq!(e.code, ErrorCode::ProRequired);
        assert!(e.message.contains("2020-01-01"), "{}", e.message);
    }

    #[test]
    fn corrupt_license_file_does_not_block() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("license.json"), "{broken").unwrap();
        assert_eq!(manager(dir.path()).status().tier, Tier::Free);
    }

    #[test]
    fn embedded_key_parses() {
        let _ = LicenseManager::embedded_key();
    }

    #[test]
    fn status_json_shape() {
        let key = dev::verifying_key();
        let s = evaluate(Some(&dev::pro_token("a@b", Some(d(2026, 10, 20)))), Some(LicenseSource::User), &key, d(2026, 10, 16));
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["tier"], "pro");
        assert_eq!(v["state"], "expiring");
        assert_eq!(v["source"], "user");
        assert_eq!(v["kind"], "personal");
        assert_eq!(v["daysLeft"], 4);
    }
}
```

В `error.rs` в тест `mutation_codes_serialize_and_are_not_retryable` добавить пару `(ErrorCode::ProRequired, "PRO_REQUIRED")`.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib license error`
Expected: FAIL.

- [ ] **Step 3: Реализация** — `error.rs`: в `ErrorCode` добавить `ProRequired` (сериализуется в `PRO_REQUIRED`). `lib.rs`: `pub mod license;`. Начало `license/mod.rs`:

```rust
//! Лицензия Pro (спец. M3 §4): офлайн-ключ из политики организации или введённый пользователем.

use std::path::{Path, PathBuf};
use std::sync::RwLock;

use license_format::{Kind, NaiveDate, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};
use crate::storage::fs::{read_json_or_quarantine, write_json};
use crate::sync::RwLockExt;

pub const EXPIRY_WARNING_DAYS: i64 = 14;
const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Free,
    Pro,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LicenseState {
    None,
    Valid,
    Expiring,
    Expired,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LicenseSource {
    Policy,
    User,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LicenseStatus {
    pub tier: Tier,
    pub state: LicenseState,
    pub source: Option<LicenseSource>,
    pub licensee: Option<String>,
    pub kind: Option<Kind>,
    pub expires: Option<NaiveDate>,
    pub days_left: Option<i64>,
    pub error: Option<String>,
}

impl LicenseStatus {
    fn free(state: LicenseState, source: Option<LicenseSource>) -> Self {
        Self { tier: Tier::Free, state, source, licensee: None, kind: None, expires: None, days_left: None, error: None }
    }
}

pub fn evaluate(token: Option<&str>, source: Option<LicenseSource>, key: &VerifyingKey, today: NaiveDate) -> LicenseStatus {
    let Some(token) = token.map(str::trim).filter(|t| !t.is_empty()) else {
        return LicenseStatus::free(LicenseState::None, None);
    };
    match license_format::verify(token, key) {
        Err(e) => LicenseStatus { error: Some(e.to_string()), ..LicenseStatus::free(LicenseState::Invalid, source) },
        Ok(p) => {
            let days_left = p.expires.map(|d| (d - today).num_days());
            let state = match days_left {
                Some(d) if d < 0 => LicenseState::Expired,
                Some(d) if d < EXPIRY_WARNING_DAYS => LicenseState::Expiring,
                _ => LicenseState::Valid,
            };
            LicenseStatus {
                tier: if state == LicenseState::Expired { Tier::Free } else { Tier::Pro },
                state,
                source,
                licensee: Some(p.licensee),
                kind: Some(p.kind),
                expires: p.expires,
                days_left,
                error: None,
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LicenseFile {
    schema_version: u32,
    key: String,
}

pub struct LicenseManager {
    path: PathBuf,
    key: VerifyingKey,
    policy_key: RwLock<Option<String>>,
    user_key: RwLock<Option<String>>,
}

impl LicenseManager {
    /// Открытый ключ из `src-tauri/license-public.key`, вшитый при сборке.
    pub fn embedded_key() -> VerifyingKey {
        license_format::parse_verifying_key(include_str!("../../license-public.key"))
            .expect("license-public.key содержит неверный открытый ключ")
    }

    /// Повреждённый `license.json` не мешает запуску: он переименовывается, лицензия — Free.
    pub fn open(dir: &Path, key: VerifyingKey) -> Self {
        let path = dir.join("license.json");
        let user_key = match read_json_or_quarantine::<LicenseFile>(&path, SCHEMA_VERSION) {
            Ok(f) => f.map(|f| f.key),
            Err(e) => {
                tracing::warn!("лицензия не прочитана: {}", e.message);
                None
            }
        };
        Self { path, key, policy_key: RwLock::new(None), user_key: RwLock::new(user_key) }
    }

    pub fn set_policy_key(&self, key: Option<String>) {
        *self.policy_key.write_safe() = key;
    }

    pub fn status(&self) -> LicenseStatus {
        self.status_on(chrono::Local::now().date_naive())
    }

    pub fn status_on(&self, today: NaiveDate) -> LicenseStatus {
        if let Some(k) = self.policy_key.read_safe().as_deref() {
            return evaluate(Some(k), Some(LicenseSource::Policy), &self.key, today);
        }
        let user = self.user_key.read_safe();
        evaluate(user.as_deref(), user.as_ref().map(|_| LicenseSource::User), &self.key, today)
    }

    fn ensure_not_managed(&self) -> AppResult<()> {
        if self.policy_key.read_safe().is_some() {
            return Err(AppError::invalid("Лицензия управляется вашей организацией (корпоративная политика) — изменить её нельзя"));
        }
        Ok(())
    }

    /// Блокирующий вызов (запись файла) — из blocking-контекста.
    pub fn set_user_key(&self, token: &str) -> AppResult<LicenseStatus> {
        self.ensure_not_managed()?;
        let st = evaluate(Some(token), Some(LicenseSource::User), &self.key, chrono::Local::now().date_naive());
        match st.state {
            LicenseState::Invalid | LicenseState::None => {
                return Err(AppError::invalid(format!(
                    "Ключ недействителен: {}",
                    st.error.as_deref().unwrap_or("пустой ключ")
                )));
            }
            LicenseState::Expired => {
                return Err(AppError::invalid(format!(
                    "Срок действия ключа истёк {}",
                    st.expires.map(|d| d.to_string()).unwrap_or_default()
                )));
            }
            LicenseState::Valid | LicenseState::Expiring => {}
        }
        let token = token.trim().to_string();
        write_json(&self.path, &LicenseFile { schema_version: SCHEMA_VERSION, key: token.clone() })?;
        *self.user_key.write_safe() = Some(token);
        Ok(self.status())
    }

    pub fn remove_user_key(&self) -> AppResult<LicenseStatus> {
        self.ensure_not_managed()?;
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        *self.user_key.write_safe() = None;
        Ok(self.status())
    }

    /// Проверка в Rust: интерфейс только показывает замки.
    pub fn require_pro(&self) -> AppResult<()> {
        let st = self.status();
        if st.tier == Tier::Pro {
            return Ok(());
        }
        let message = match (st.state, st.expires) {
            (LicenseState::Expired, Some(d)) => format!("Срок лицензии Pro истёк {d} — функция доступна в Pro"),
            _ => "Функция доступна в Pro — активируйте лицензию на экране «Лицензия»".to_string(),
        };
        Err(AppError::new(ErrorCode::ProRequired, message))
    }
}

/// Для тестов: менеджер с dev-ключом и бессрочной Pro-лицензией.
#[cfg(test)]
pub fn pro_for_tests(dir: &Path) -> LicenseManager {
    let m = LicenseManager::open(dir, license_format::dev::verifying_key());
    m.set_user_key(&license_format::dev::pro_token("test", None)).expect("dev-лицензия");
    m
}
```

(Если `RwLockExt` в `crate::sync` называет методы иначе, используйте существующие имена: `read_safe` / `write_safe` уже применяются в `storage/connections.rs`.)

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib license error`
Expected: PASS.

- [ ] **Step 4: Состояние и команды** — `state.rs`:
  - поле `pub license: Arc<crate::license::LicenseManager>,`, в `new`: `license: Arc::new(crate::license::LicenseManager::open(data_dir, crate::license::LicenseManager::embedded_key())),`;
  - в `with_policy` перед `self.policy = …`: `self.license.set_policy_key(policy.license_key.clone());`.

Тест в `state.rs`:

```rust
    #[test]
    fn policy_license_key_reaches_manager() {
        let dir = tempfile::tempdir().unwrap();
        let mut st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        st.license = Arc::new(crate::license::LicenseManager::open(dir.path(), license_format::dev::verifying_key()));
        let policy = crate::policy::Policy {
            license_key: Some(license_format::dev::pro_token("ACME", None)),
            ..Default::default()
        };
        let st = st.with_policy(policy);
        assert_eq!(st.license.status().licensee.as_deref(), Some("ACME"));
    }
```

`commands/license.rs` — дополнить:

```rust
use super::blocking;
use crate::license::LicenseStatus;

#[tauri::command]
pub fn get_license_status(state: State<'_, AppState>) -> AppResult<LicenseStatus> {
    Ok(state.license.status())
}

#[tauri::command]
pub async fn set_license_key(state: State<'_, AppState>, key: String) -> AppResult<LicenseStatus> {
    let st = state.inner().clone();
    blocking(move || st.license.set_user_key(&key)).await
}

#[tauri::command]
pub async fn remove_license_key(state: State<'_, AppState>) -> AppResult<LicenseStatus> {
    let st = state.inner().clone();
    blocking(move || st.license.remove_user_key()).await
}
```

`lib.rs`: зарегистрировать три команды. В `setup` логировать `tracing::info!(tier = ?st.license.status().tier, "лицензия")`. Ключ лицензии в лог не попадает.

- [ ] **Step 5: Ограничения Pro — тесты** — в `commands/consume.rs` (`mod tests`):
  - в хелпере создания состояния после `AppState::new(...)` добавить `st.license = Arc::new(crate::license::pro_for_tests(dir.path()));` (`st` объявить `mut`), чтобы существующие тесты проверяли guard, а не лицензию;
  - новый тест:

```rust
    #[test]
    fn consumer_sessions_require_pro() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        st.connections.upsert(sample_profile("dev")).unwrap();
        let e = start_consumer_impl(&st, params("dev", "g"), NullSink, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::ProRequired);
        assert!(st.sessions.is_empty(), "сессия не зарегистрирована");
    }
```

В `commands/connections.rs` (`mod tests`):

```rust
    use crate::storage::connections::Environment;

    fn prod(mut p: ConnectionProfile) -> ConnectionProfile {
        p.environment = Environment::Prod;
        p
    }

    #[test]
    fn prod_environment_requires_pro_but_existing_prod_stays_editable() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        // Новый prod-профиль без лицензии — нельзя.
        let e = save_connection_blocking(&st, input(prod(sample_profile("a")), None)).unwrap_err();
        assert_eq!(e.code, ErrorCode::ProRequired);
        assert!(st.connections.get("a").is_err());
        // dev → prod — нельзя.
        save_connection_blocking(&st, input(sample_profile("b"), None)).unwrap();
        assert_eq!(save_connection_blocking(&st, input(prod(sample_profile("b")), None)).unwrap_err().code, ErrorCode::ProRequired);
        // Существующий prod-профиль (создан раньше) остаётся редактируемым и защищённым.
        st.connections.upsert(prod(sample_profile("c"))).unwrap();
        let mut renamed = prod(sample_profile("c"));
        renamed.name = "новое имя".into();
        save_connection_blocking(&st, input(renamed, None)).unwrap();
    }

    #[test]
    fn prod_environment_with_pro() {
        let (d, mut st) = state(Arc::new(MemoryStore::default()));
        st.license = Arc::new(crate::license::pro_for_tests(d.path()));
        save_connection_blocking(&st, input(prod(sample_profile("a")), None)).unwrap();
    }
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib commands`
Expected: FAIL.

- [ ] **Step 6: Ограничения Pro — реализация**
  - `start_consumer_impl`: первой строкой `st.license.require_pro()?;`.
  - `commit_offsets_impl`: первой строкой `st.license.require_pro()?;`.
  - `pause_partitions`, `resume_partitions`: первой строкой `state.license.require_pro()?;`.
  - `save_connection_blocking`, сразу после `let previous = …`:
    ```rust
    // Перевод в prod — функция Pro; уже существующие prod-профили остаются редактируемыми (и защищёнными guard).
    let becomes_prod = profile.environment == Environment::Prod
        && previous.as_ref().map(|p| p.environment) != Some(Environment::Prod);
    if becomes_prod {
        st.license.require_pro()?;
    }
    ```

`require_pro` только читает `RwLock`, поэтому вызывать его в async-командах безопасно.

`src-tauri/tests/it_consume.rs`: там, где создаётся `AppState` для `start_consumer_impl` (около строки 321), сделать состояние `mut` и выдать Pro:

```rust
        let license = kafka_explorer_lib::license::LicenseManager::open(dir.path(), license_format::dev::verifying_key());
        license.set_user_key(&license_format::dev::pro_token("it", None)).unwrap();
        st.license = Arc::new(license);
```

Run: `source "$HOME/.cargo/env" && KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 KE_IT_MTLS_BOOTSTRAP=localhost:9095 cargo test --manifest-path src-tauri/Cargo.toml --lib --tests && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && cargo fmt --manifest-path src-tauri/Cargo.toml --check`
Expected: всё зелёное.

- [ ] **Step 7: Commit**

```bash
git add src-tauri
git commit -m "feat(license): лицензия Pro в приложении; consumer-сессии и режим prod требуют Pro

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Интерфейс лицензии и замки Pro

**Files:**
- Modify: `src/api/types.ts`, `src/api/client.ts`, `src/api/client.test.ts`
- Create: `src/lib/useLicense.ts`, `src/components/common/ProLock.tsx`
- Create: `src/components/license/LicensePanel.tsx`, `LicensePanel.test.tsx`, `LicenseNotice.tsx`
- Create: `src/components/ProGating.test.tsx`
- Modify: `src/state/workspace.ts`, `src/components/TabContent.tsx`, `src/components/Layout.tsx`, `src/components/Sidebar.tsx`
- Modify: `src/components/topics/TopicDetails.tsx`, `src/components/groups/GroupDetails.tsx`, `src/components/consumer/ConsumerPanel.tsx`, `src/components/connections/ConnectionForm.tsx`, `src/components/common/ErrorBanner.tsx`
- Modify: `src/styles.css`

**Interfaces:**
- Consumes: команды `get_license_status`, `set_license_key(key)`, `remove_license_key`, `get_policy_status` (Task 7, 9).
- Produces (TS):
  ```ts
  export type Tier = "free" | "pro";
  export type LicenseState = "none" | "valid" | "expiring" | "expired" | "invalid";
  export interface LicenseStatus { tier: Tier; state: LicenseState; source: "policy" | "user" | null; licensee: string | null;
    kind: "personal" | "organization" | null; expires: string | null; daysLeft: number | null; error: string | null }
  export interface PolicyStatus { managed: boolean; source: string | null; updatesEnabled: boolean; updateUrl: string | null;
    hasLicenseKey: boolean; errors: string[] }
  ErrorCode += "PRO_REQUIRED"
  api.getLicenseStatus(), api.setLicenseKey(key), api.removeLicenseKey(), api.getPolicyStatus()
  ```
- Produces: `useLicense(): { status: LicenseStatus | undefined; locked: boolean }`. `locked` — статус загружен и тариф не Pro. Пока статус не загружен или не получен, интерфейс не блокируется: проверку всё равно делает Rust. `PRO_HINT` — текст подсказки. `<ProLock />` — значок «🔒 Pro».
- Produces: `TabSpec += { kind: "license" }` (id `license`).

- [ ] **Step 1: Типы, клиент, хук** — типы из Interfaces в `types.ts`. В `client.ts`:

```ts
  getLicenseStatus: () => call<LicenseStatus>("get_license_status"),
  setLicenseKey: (key: string) => call<LicenseStatus>("set_license_key", { key }),
  removeLicenseKey: () => call<LicenseStatus>("remove_license_key"),
  getPolicyStatus: () => call<PolicyStatus>("get_policy_status"),
```

Тест в `client.test.ts`:

```ts
  it("setLicenseKey передаёт ключ", async () => {
    invoke.mockResolvedValue({ tier: "pro" });
    await api.setLicenseKey("a.b");
    expect(invoke).toHaveBeenCalledWith("set_license_key", { key: "a.b" });
  });
```

`src/lib/useLicense.ts`:

```ts
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";

export const PRO_HINT = "Доступно в Pro — активируйте лицензию на экране «Лицензия»";

/** locked — статус известен и тариф не Pro. Пока статус не загружен, интерфейс не блокируется: проверку делает Rust. */
export function useLicense() {
  const q = useQuery({ queryKey: ["license"], queryFn: api.getLicenseStatus, staleTime: 60_000 });
  const status = q.data;
  return { status, locked: status !== undefined && status.tier !== "pro" };
}
```

`src/components/common/ProLock.tsx`:

```tsx
import { PRO_HINT } from "../../lib/useLicense";

export function ProLock() {
  return (
    <span className="pro-lock" title={PRO_HINT}>
      🔒 Pro
    </span>
  );
}
```

- [ ] **Step 2: Тесты замков** — `src/components/ProGating.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { LicenseStatus } from "../api/types";
import { renderWithClient } from "../test/render";
import { newProfile } from "./connections/profile";

const FREE: LicenseStatus = { tier: "free", state: "none", source: null, licensee: null, kind: null, expires: null, daysLeft: null, error: null };
const PRO: LicenseStatus = { ...FREE, tier: "pro", state: "valid", source: "user", licensee: "a@b", kind: "personal" };

vi.mock("../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../api/client")>();
  return {
    ...orig,
    api: {
      getLicenseStatus: vi.fn(),
      listConnections: vi.fn().mockResolvedValue([{ ...newProfile(), id: "c", name: "local" }]),
    },
  };
});

import { api } from "../api/client";
import { ConnectionForm } from "./connections/ConnectionForm";
import { Sidebar } from "./Sidebar";

beforeEach(() => vi.clearAllMocks());

describe("Free / Pro", () => {
  it("Free: consumer-сессия в дереве недоступна, рядом замок", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    renderWithClient(<Sidebar />);
    const link = await screen.findByRole("button", { name: /Consumer-сессия/ });
    await vi.waitFor(() => expect(link).toBeDisabled());
    expect(link).toHaveAttribute("title", expect.stringContaining("Pro"));
    expect(screen.getByText("Лицензия: Free")).toBeInTheDocument();
  });

  it("Pro: consumer-сессия доступна", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(PRO);
    renderWithClient(<Sidebar />);
    expect(await screen.findByText("Лицензия: Pro")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Consumer-сессия/ })).toBeEnabled();
  });

  it("Free: окружение prod нельзя выбрать для нового подключения", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    const option = await screen.findByRole("option", { name: "prod (Pro)" });
    await vi.waitFor(() => expect(option).toBeDisabled());
  });

  it("Free: существующий prod-профиль остаётся prod", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={{ ...newProfile(), id: "p", name: "p", environment: "prod" }} tabId="t" />);
    expect(screen.getByLabelText("Окружение")).toHaveValue("prod");
    await user.selectOptions(screen.getByLabelText("Окружение"), "dev");
    await user.selectOptions(screen.getByLabelText("Окружение"), "prod");
    expect(screen.getByLabelText("Окружение")).toHaveValue("prod");
  });
});
```

Run: `pnpm vitest run src/components/ProGating.test.tsx`
Expected: FAIL.

- [ ] **Step 3: Замки** —
  - `Sidebar.tsx`: `const { status, locked } = useLicense();`.
    Кнопка «Consumer-сессия»: `disabled={locked}`, `title={locked ? PRO_HINT : undefined}`, после текста `{locked && <> <ProLock /></>}`.
    Внизу `<aside>` добавить:
    ```tsx
      <div className="sidebar-footer">
        <button type="button" className="link" onClick={() => open({ kind: "license" })}>
          Лицензия: {status ? (status.tier === "pro" ? "Pro" : "Free") : "…"}
        </button>
      </div>
    ```
  - `TopicDetails.tsx` («Читать как группа») и `GroupDetails.tsx` («Читать как участник»): к условию `disabled` добавить `|| locked`. В `title` после причины read-only вставить `: locked ? PRO_HINT`, после текста кнопки — `{locked && <> <ProLock /></>}`.
  - `ConsumerPanel.tsx`: после всех хуков, перед проверкой `connection.readOnly`:
    ```tsx
      if (locked && !running) {
        return (
          <div className="page">
            <p className="warn">Consumer-сессии доступны в Pro.</p>
            <button type="button" className="link" onClick={() => openTab({ kind: "license" })}>
              Открыть «Лицензия»
            </button>
          </div>
        );
      }
    ```
    (`openTab` — `useWorkspace((s) => s.open)`; если в панели уже есть переменная `open` с другим смыслом, назовите иначе.)
  - `ConnectionForm.tsx`: `const { locked } = useLicense();`. В `<select id="cf-env">` опции рендерить так:
    ```tsx
          {ENVIRONMENTS.map((env) => {
            const proOnly = env === "prod" && locked && initial.environment !== "prod";
            return (
              <option key={env} value={env} disabled={proOnly}>
                {proOnly ? "prod (Pro)" : env}
              </option>
            );
          })}
    ```
  - `ErrorBanner.tsx`: при `e.code === "PRO_REQUIRED"` рядом с «Повторить» показать кнопку «Открыть «Лицензия»» (`useWorkspace((s) => s.open)({ kind: "license" })`).
  - `styles.css`:
    ```css
    .pro-lock { font-size: 11px; color: var(--muted); white-space: nowrap; }
    .sidebar-footer { padding: 8px 12px; border-top: 1px solid var(--border); }
    .notice { display: flex; gap: 8px; align-items: center; padding: 6px 12px; border-bottom: 1px solid var(--warn); background: color-mix(in srgb, var(--warn) 10%, transparent); }
    ```

Run: `pnpm vitest run src/components/ProGating.test.tsx`
Expected: PASS.

- [ ] **Step 4: Экран «Лицензия» — тесты** — `src/components/license/LicensePanel.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { LicenseStatus, PolicyStatus } from "../../api/types";
import { renderWithClient } from "../../test/render";

const FREE: LicenseStatus = { tier: "free", state: "none", source: null, licensee: null, kind: null, expires: null, daysLeft: null, error: null };
const PRO: LicenseStatus = { ...FREE, tier: "pro", state: "valid", source: "user", licensee: "a@b", kind: "personal", expires: "2027-03-31", daysLeft: 166 };
const UNMANAGED: PolicyStatus = { managed: false, source: null, updatesEnabled: true, updateUrl: null, hasLicenseKey: false, errors: [] };

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getLicenseStatus: vi.fn(),
      getPolicyStatus: vi.fn(),
      setLicenseKey: vi.fn(),
      removeLicenseKey: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { LicensePanel } from "./LicensePanel";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getPolicyStatus).mockResolvedValue(UNMANAGED);
});

describe("LicensePanel", () => {
  it("активация ключа", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    vi.mocked(api.setLicenseKey).mockResolvedValue(PRO);
    const user = userEvent.setup();
    renderWithClient(<LicensePanel />);
    expect(await screen.findByText("Free")).toBeInTheDocument();
    await user.type(screen.getByLabelText("Лицензионный ключ"), " a.b ");
    await user.click(screen.getByRole("button", { name: "Активировать" }));
    expect(api.setLicenseKey).toHaveBeenCalledWith("a.b");
    expect(await screen.findByText("Pro")).toBeInTheDocument();
    expect(screen.getByText("a@b")).toBeInTheDocument();
    expect(screen.getByText("2027-03-31")).toBeInTheDocument();
  });

  it("ошибка ключа показывается", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(FREE);
    vi.mocked(api.setLicenseKey).mockRejectedValue({ code: "INVALID_INPUT", message: "Ключ недействителен: подпись", retryable: false });
    const user = userEvent.setup();
    renderWithClient(<LicensePanel />);
    await user.type(await screen.findByLabelText("Лицензионный ключ"), "x.y");
    await user.click(screen.getByRole("button", { name: "Активировать" }));
    expect(await screen.findByText("Ключ недействителен: подпись")).toBeInTheDocument();
  });

  it("удаление ключа — с подтверждением на странице", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(PRO);
    vi.mocked(api.removeLicenseKey).mockResolvedValue(FREE);
    const user = userEvent.setup();
    renderWithClient(<LicensePanel />);
    await user.click(await screen.findByRole("button", { name: "Удалить ключ" }));
    expect(api.removeLicenseKey).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Да, удалить" }));
    expect(api.removeLicenseKey).toHaveBeenCalled();
  });

  it("лицензия организации: ввод недоступен", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue({ ...PRO, source: "policy", licensee: "ACME", kind: "organization" });
    vi.mocked(api.getPolicyStatus).mockResolvedValue({ ...UNMANAGED, managed: true, source: "/etc/x/policy.json", hasLicenseKey: true });
    renderWithClient(<LicensePanel />);
    expect(await screen.findByText("Лицензия управляется вашей организацией.")).toBeInTheDocument();
    expect(screen.queryByLabelText("Лицензионный ключ")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Удалить ключ" })).not.toBeInTheDocument();
  });

  it("истекает и истекла", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue({ ...PRO, state: "expiring", daysLeft: 3 });
    const { unmount } = renderWithClient(<LicensePanel />);
    expect(await screen.findByText(/истекает через 3 дн/)).toBeInTheDocument();
    unmount();
    vi.mocked(api.getLicenseStatus).mockResolvedValue({ ...PRO, tier: "free", state: "expired", expires: "2026-10-01", daysLeft: -15 });
    renderWithClient(<LicensePanel />);
    expect(await screen.findByText(/истекла 2026-10-01/)).toBeInTheDocument();
  });
});
```

Run: `pnpm vitest run src/components/license`
Expected: FAIL.

- [ ] **Step 5: `LicensePanel`** — `src/components/license/LicensePanel.tsx`:

```tsx
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
          <li>Режим prod: подтверждение именем подключения для любых изменений и маркировка окружений.</li>
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
```

`src/components/license/LicenseNotice.tsx` — полоса вверху рабочей области:

```tsx
import { useState } from "react";
import { useLicense } from "../../lib/useLicense";
import { useWorkspace } from "../../state/workspace";

export function LicenseNotice() {
  const { status } = useLicense();
  const open = useWorkspace((s) => s.open);
  const [hidden, setHidden] = useState(false);
  if (hidden || !status || (status.state !== "expiring" && status.state !== "expired")) return null;
  return (
    <div className="notice" role="status">
      <span>
        {status.state === "expiring"
          ? `Лицензия Pro истекает через ${status.daysLeft} дн.`
          : `Лицензия Pro истекла ${status.expires}: функции Pro отключены.`}
      </span>
      <button type="button" className="link" onClick={() => open({ kind: "license" })}>
        Подробнее
      </button>
      <button type="button" className="icon" aria-label="Скрыть уведомление" onClick={() => setHidden(true)}>
        ×
      </button>
    </div>
  );
}
```

- [ ] **Step 6: Вкладка** —
  - `workspace.ts`: в `TabSpec` добавить `| { kind: "license" }`, в `tabId` — `case "license": return "license";`. В `Layout.tsx`, `tabId` и других местах, где берётся `tab.connectionId`, использовать `"connectionId" in tab ? tab.connectionId : null` (так требует TypeScript). В `closeForConnection` то же самое.
  - `TabContent.tsx`: `case "license": return <LicensePanel />;`.
  - `Layout.tsx`: в `tabTitle` — `case "license": return "Лицензия";`. После `env-stripe` вставить `<LicenseNotice />`.

Run: `pnpm vitest run && pnpm build`
Expected: PASS. Старые тесты не меняются: при отсутствии `getLicenseStatus` в моке статус не загружается и замки не включаются.

- [ ] **Step 7: Commit**

```bash
git add src
git commit -m "feat(ui): экран «Лицензия», замки Pro и уведомление об окончании лицензии

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Документация, полная проверка и смоук M3a

**Files:**
- Modify: `README.md`
- Create: `docs/licensing.md`
- Create: `docs/smoke/m3a.md`

- [ ] **Step 1: README** — после раздела «Отправка и чтение как группа» добавить:

````markdown
## Сертификаты (SSL и mTLS)
- **Доверенный CA** — файл PEM, PKCS#12 или JKS (truststore). Пусто — системные корневые сертификаты.
- **Клиентский сертификат (mTLS)** — PEM (сертификат + ключ), PKCS#12 (`.p12`, `.pfx`) или JKS. Пароли необязательны и хранятся в системном хранилище секретов.
- Файлы читаются при каждом подключении: после ротации сертификата достаточно переподключиться.

Пример для кластера в Kubernetes с Istio / cert-manager (секрет с `tls.crt`, `tls.key`, `ca.crt`):
```bash
kubectl get secret kafka-client -o jsonpath='{.data.tls\.crt}' | base64 -d > ~/certs/tls.crt
kubectl get secret kafka-client -o jsonpath='{.data.tls\.key}' | base64 -d > ~/certs/tls.key
kubectl get secret kafka-client -o jsonpath='{.data.ca\.crt}' | base64 -d > ~/certs/ca.crt
```
Протокол `SSL`, доверенный CA — `ca.crt`, клиентский сертификат PEM — `tls.crt` и `tls.key`, пароли пустые.

Локальный стенд слушает mTLS на `localhost:9095` (сертификаты — `dev/certs/out`, см. `dev/certs/README.md`).

## Free и Pro
- **Free:** просмотр, топики и группы, Producer, все виды подключений (включая mTLS), «только чтение».
- **Pro:** consumer-сессии и режим prod (подтверждение именем подключения для любых изменений).
- Лицензия вводится на экране «Лицензия» (ссылка внизу дерева подключений) или раздаётся администратором через политику (`licenseKey`). Проверка офлайн.
- Выдача ключей — `docs/licensing.md`.
````

В раздел «Где хранятся данные» добавить строку: `- Лицензия: license.json (только если ключ введён вручную).`

- [ ] **Step 2: `docs/licensing.md`**

````markdown
# Лицензии: генерация ключей и выдача

Лицензионный ключ — подписанный Ed25519 текст: `base64url(JSON).base64url(подпись)`.
Приложение проверяет его офлайн открытым ключом из `src-tauri/license-public.key`.

## Один раз: пара ключей
```bash
cargo run --manifest-path tools/issue-license/Cargo.toml -- keygen --out ~/.kafka-explorer-license
cp ~/.kafka-explorer-license/license-public.key src-tauri/license-public.key
```
`license-signing.key` — секрет. Держите вне репозитория (менеджер паролей, зашифрованный диск) и сделайте резервную копию:
без него нельзя выдать новые ключи, а ключи, подписанные потерянным ключом, нельзя отозвать.
В репозитории лежит только тестовая пара `crates/license-format/testdata/dev-*` — релизная сборка с ней не собирается (M3b).

## Выдать ключ
```bash
# организации (раздаётся через политику licenseKey)
cargo run --manifest-path tools/issue-license/Cargo.toml -- issue --key ~/.kafka-explorer-license/license-signing.key \
  --kind organization --licensee "ACME Corp" --expires 2027-03-31 --seats 500
# человеку
cargo run --manifest-path tools/issue-license/Cargo.toml -- issue --key ~/.kafka-explorer-license/license-signing.key \
  --kind personal --licensee user@example.com --expires 2027-03-31
```
`--expires` — последний день действия (включительно). Без него ключ бессрочный.

## Проверить ключ
```bash
cargo run --manifest-path tools/issue-license/Cargo.toml -- verify --public src-tauri/license-public.key '<ключ>'
```

## Что происходит при окончании срока
За 14 дней — предупреждение. После — функции Pro отключаются, данные и существующие prod-профили (с подтверждениями) сохраняются.
````

- [ ] **Step 3: Чек-лист смоука** — `docs/smoke/m3a.md`:

````markdown
# Смоук M3a

Подготовка: `docker compose -f dev/docker-compose.yml up -d --force-recreate`, `pnpm tauri dev`.
Сертификаты — `dev/certs/out` (пароли: хранилища `storepass`, отдельного ключа `keypass`).

## mTLS
1. Новое подключение `mtls`, bootstrap `localhost:9095`, протокол `SSL`.
   Доверенный CA — «Выбрать…» → `ca.pem`. Клиентский сертификат включить, «Выбрать…» → `client.pem`
   (формат PEM определился сам), ключ — `client.key`, пароли пустые.
   «Проверить подключение» → все три шага ✅. Сохранить, открыть «Топики».
2. Ключ `client-enc.key` без пароля → шаг «Конфигурация» ❌ «ключ зашифрован — укажите пароль ключа».
   С паролем `keypass` → ✅.
3. Формат PKCS#12: `client.p12`, пароль хранилища `storepass` → ✅; пароль `nope` → «неверный пароль хранилища PKCS#12».
4. Формат JKS: `client-keypass.jks`, пароль хранилища `storepass`, пароль ключа пустой → «Неверный пароль ключа…»;
   пароль ключа `keypass` → ✅. Доверенный CA — `truststore.jks` с паролем `storepass` → ✅.
5. Выключить клиентский сертификат → ❌ на шаге Kafka с понятным текстом (TLS_ERROR или CONNECTION_FAILED).
6. Сохранённый JKS-профиль: «удалить сохранённый» у пароля ключа → сохранить → «Проверить» ❌ (пароль ключа нужен).

## Лицензия
7. Внизу дерева «Лицензия: Free». «Consumer-сессия» в дереве и «Читать как группа» у топика — неактивны с замком и подсказкой.
   В форме подключения окружение «prod (Pro)» недоступно.
8. Выдать dev-ключ (`cargo run --manifest-path tools/issue-license/Cargo.toml -- issue --key crates/license-format/testdata/dev-signing.key --kind personal --licensee me@example.com --expires <дата через 10 дней>`),
   открыть «Лицензия» → вставить → «Активировать» → тариф Pro, предупреждение «истекает через 10 дн.», полоса вверху.
   Consumer-сессии и prod доступны. Перезапуск приложения — Pro сохраняется.
9. «Удалить ключ» → подтверждение → Free. Prod-профиль, созданный в п. 8, остаётся prod и по-прежнему требует ввода имени подключения.
10. Ключ с испорченным символом → «Ключ недействителен: подпись ключа не подтверждена…».

## Политика (macOS)
11. `sudo mkdir -p "/Library/Managed Preferences"`; создать `/Library/Managed Preferences/dev.kafkaexplorer.app.plist`
    с `licenseKey` (ключ из п. 8) и `updatesEnabled` = false (`sudo defaults write "/Library/Managed Preferences/dev.kafkaexplorer.app" licenseKey '<ключ>'`,
    `sudo defaults write "/Library/Managed Preferences/dev.kafkaexplorer.app" updatesEnabled -bool false`).
    Перезапуск → «Лицензия»: «управляется вашей организацией», поле ввода скрыто, блок «Политика организации» с путём.
12. Удалить файл (`sudo rm "/Library/Managed Preferences/dev.kafkaexplorer.app.plist"`), перезапуск → снова личная лицензия / Free.
````

- [ ] **Step 4: Полный прогон**

Run:
```bash
docker compose -f dev/docker-compose.yml up -d && scripts/wait-kafka.sh
source "$HOME/.cargo/env"
pnpm test && pnpm build && \
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && \
cargo fmt --manifest-path src-tauri/Cargo.toml --check && \
cargo test --manifest-path crates/license-format/Cargo.toml --all-features && \
cargo test --manifest-path tools/issue-license/Cargo.toml && \
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 KE_IT_MTLS_BOOTSTRAP=localhost:9095 \
  cargo test --manifest-path src-tauri/Cargo.toml --lib --tests
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -c '^it-'
```
Expected: всё зелёное; последняя команда печатает `0`.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/licensing.md docs/smoke/m3a.md
git commit -m "docs: сертификаты, Free/Pro, выдача лицензий, смоук M3a

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

После этого контроллер просит пользователя пройти `docs/smoke/m3a.md` и запустить CI (`gh workflow run ci`) на ветке с его согласия на push.
