# Kafka Explorer — M2b «Producer и consumer-сессии»: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить в Kafka Explorer отправку сообщений (одиночную, по шаблонам с плейсхолдерами, пакетную из N копий или файла JSON Lines/CSV) с историей и чтение топиков как участник consumer group: назначения, журнал ребалансов, ручной коммит, pause/resume.

**Architecture:** Одиночная отправка идёт через `FutureProducer` из пула (ключ «подключение + настройки», не больше 4 на подключение). Пакетная отправка и consumer-сессии — фоновые потоки с `SessionId` в общем `SessionRegistry`; события идут во фронтенд через `tauri::ipc::Channel`. Команды consumer-сессии (коммит, pause/resume) передаются в её поток через mpsc-канал, потому что consumer не `Sync`. Плейсхолдеры, кодирование payload, разбор файлов и ограничитель скорости — чистые функции с unit-тестами. Шаблоны и история — JSON-файлы через `storage::fs::write_json`.

**Tech Stack:** Rust stable (edition 2024), Tauri 2 + `tauri-plugin-dialog`, rdkafka 0.37, serde, `chrono` 0.4, `fastrand` 2, `csv` 1; React 19, TypeScript, TanStack Query 5, TanStack Virtual 3, Zustand 5, Vitest + Testing Library; Docker (confluentinc/cp-kafka:7.6.1).

**Спецификации:** `docs/superpowers/specs/2026-10-14-kafka-explorer-design.md` (§4.6, §4.7, §5), `docs/superpowers/specs/2026-10-15-kafka-explorer-m2-design.md` (§4).

**Начальное состояние:** `main` со слитым M2a (`docs/superpowers/plans/2026-10-15-kafka-explorer-m2a.md`): есть `guard`, `AppState::guarded`, `ConfirmDialog`, `needsConfirmation`, `useConnection`, `storage::fs::{read_json, write_json}`, `sync::MutexExt`, `KeyedPool`, `commands::with_pooled`, `kafka::groups::{TopicPartitionRef, OffsetTarget}`, `kafka::group_ops`, `tests/common::{TempTopic, TempGroup, join_group, commit_group_offsets}`.

## Global Constraints

- Платформы: macOS (arm64, x64), Windows x64, Linux x64; Kafka 2.8+ (ZooKeeper и KRaft), Redpanda, AWS MSK, Confluent Cloud.
- Вся работа с Kafka — в Rust; фронтенд не знает о librdkafka.
- Все команды возвращают `Result<T, AppError>`; `AppError { code, message, details?, retryable }` сериализуется в camelCase, `code` — в SCREAMING_SNAKE_CASE.
- **Мутирующие команды M2b первой строкой вызывают guard** (`AppState::guarded`): `produce`, `produce_batch`, `start_consumer`, `commit_offsets`. read-only → `READ_ONLY` (consumer-сессии в read-only запрещены целиком); `prod` без `confirm`, равного имени подключения, → `CONFIRMATION_REQUIRED`.
- Форматы key/value при отправке в M2: `string`, `json` (валидируется в Rust), `hex`, `base64`, `null`; значения заголовков — `string` или `hex`. SR-форматы — M3.
- Плейсхолдеры рендерятся в Rust: `{{uuid}}`, `{{now}}` (ISO-8601 UTC с миллисекундами), `{{nowMillis}}`, `{{seq}}`, `{{randomInt min max}}`, `{{randomString n}}`, `{{env NAME}}`. Неизвестный плейсхолдер или неверные аргументы → `INVALID_INPUT` до отправки.
- Настройки продюсера: `acks` (`0` / `1` / `all`), compression (`none` / `gzip` / `snappy` / `lz4` / `zstd`), idempotence (требует `acks = all`).
- История: последние 100 успешно отправленных сообщений на подключение в `history/<conn-id>.json`, после подстановки плейсхолдеров; значения больше 64 КБ усекаются с флагом `truncated` — такие записи повторить нельзя.
- Шаблоны: `templates.json` (`schemaVersion: 1`), на уровне подключения или глобально (`connectionId: null`).
- Пакетная отправка: до 1 000 000 копий; источник-файл читается потоково по пути; прогресс — раз в 100 мс; в событии — первые 20 ошибок с номером строки; отмена — `stop_session`.
- Consumer-сессия: `enable.auto.commit` по умолчанию выключен; сгенерированный group.id — `kafka-explorer-<user>-<6 hex>`; коммит «до выбранного» = `offset + 1` партиции сообщения; остановка — корректный выход из группы (`unsubscribe` + закрытие консьюмера).
- Данные из Rust во фронтенд идут батчами: до 200 сообщений или раз в 100 мс; буфер сообщений в UI — не больше 100 000.
- Ни одна операция не блокирует UI: блокирующие вызовы librdkafka — в `spawn_blocking` или отдельном потоке; у каждой сетевой операции есть таймаут.
- JSON-файлы пишутся через `storage::fs::write_json`; блокировки — `lock_safe` / `read_safe` / `write_safe`.
- Тексты UI — по-русски в коде; подтверждения — `ConfirmDialog` внутри страницы; для read-only кнопки изменений скрыты.
- Нет телеметрии и удалённого контента.

---

## Структура файлов M2b

```
src-tauri/
├── Cargo.toml                       # + chrono, fastrand, csv, tauri-plugin-dialog
├── capabilities/default.json        # + dialog:allow-open
├── src/
│   ├── lib.rs, state.rs             # новые хранилища, пул producer'ов, контролы consumer'ов
│   ├── browse/format.rs             # усечение до копирования (бэклог)
│   ├── browse/session.rs            # RecoveryMonitor вынесен в kafka/recovery.rs
│   ├── kafka/
│   │   ├── recovery.rs (NEW)        # RecoveryMonitor, is_transient
│   │   ├── context.rs               # журнал ребалансов
│   │   ├── pool.rs                  # KeyedPool::ids / evict_where
│   │   └── producer.rs (NEW)        # ProducerSettings, пул, одиночная отправка
│   ├── produce/ (NEW)
│   │   ├── mod.rs, payload.rs       # ProduceMessage, кодирование
│   │   ├── template.rs              # плейсхолдеры
│   │   ├── source.rs                # JSON Lines / CSV
│   │   ├── rate.rs                  # ограничитель скорости
│   │   └── batch.rs                 # пакетная сессия
│   ├── consume/ (NEW)
│   │   ├── mod.rs, types.rs         # параметры, события, group.id
│   │   ├── control.rs               # команды в поток сессии
│   │   └── session.rs               # цикл consumer-сессии
│   ├── storage/history.rs (NEW), storage/templates.rs (NEW)
│   └── commands/produce.rs (NEW), commands/consume.rs (NEW), commands/connections.rs
└── tests/it_produce.rs (NEW), tests/it_batch.rs (NEW), tests/it_consume.rs (NEW)

src/
├── api/types.ts, api/client.ts
├── state/workspace.ts               # вкладки producer / consumer
├── components/
│   ├── Layout.tsx, Sidebar.tsx, TabContent.tsx
│   ├── browse/state.ts              # appendMessages (бэклог)
│   ├── browse/FilterFields.tsx (NEW), browse/params.ts (buildFilterSpec), browse/BrowseForm.tsx
│   ├── browse/MessageDetails.tsx    # «Отправить повторно»
│   ├── topics/TopicDetails.tsx      # кнопки «Отправить сообщение», «Читать как группа»
│   ├── producer/ (NEW)
│   │   ├── form.ts, ProducerPanel.tsx, HeadersEditor.tsx
│   │   ├── TemplatesBar.tsx, HistoryList.tsx, BatchPanel.tsx
│   └── consumer/ (NEW)
│       ├── form.ts, state.ts, useConsumerSession.ts
│       ├── ConsumerForm.tsx, AssignmentPanel.tsx, ConsumerPanel.tsx
```

**Как запускать тесты** (из корня репозитория):
- Unit-тесты Rust: `cargo test --manifest-path src-tauri/Cargo.toml --lib <фильтр>`
- Интеграционные тесты Rust (нужен Docker): `docker compose -f dev/docker-compose.yml up -d`, затем `KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --test <имя>`. Без переменных окружения интеграционные тесты пропускаются.
- Фронтенд: `pnpm vitest run <путь>`
- Линт: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`

**Ветка:** `feature/m2b` от `main` (после слияния M2a).

---

### Task 1: Декодер усекает данные до копирования

Бэклог M1: `decode` копирует и кодирует весь payload, а потом обрезает текст до 1 МиБ — для больших сообщений это лишние аллокации.

**Files:**
- Modify: `src-tauri/src/browse/format.rs`

**Interfaces:**
- Produces: поведение `decode(bytes, format) -> DecodedData` не меняется (`text` ≤ `MAX_TEXT_BYTES`, `size` — размер исходных байтов, `truncated`), но копируется и кодируется только та часть входа, что попадёт в `text`; JSON валидируется по всему входу без копирования.

- [ ] **Step 1: Тесты** — в `mod tests` файла `format.rs` добавить:

```rust
    #[test]
    fn hex_and_base64_encode_only_the_visible_prefix() {
        let big = vec![0xabu8; MAX_TEXT_BYTES * 3];
        let h = decode(&big, DataFormat::Hex);
        assert!(h.truncated);
        assert!(h.text.len() <= MAX_TEXT_BYTES);
        assert_eq!(h.text.len() % 2, 0, "hex не режется посреди байта");
        assert_eq!(h.size, big.len());
        let b = decode(&big, DataFormat::Base64);
        assert!(b.truncated);
        assert!(b.text.len() <= MAX_TEXT_BYTES);
        assert_eq!(b.text.len() % 4, 0, "base64 — целыми группами");
    }

    #[test]
    fn json_validity_is_checked_on_whole_input() {
        let mut v = b"{\"a\":\"".to_vec();
        v.extend(std::iter::repeat_n(b'x', MAX_TEXT_BYTES * 2));
        v.extend(b"\"}");
        let d = decode(&v, DataFormat::Json);
        assert!(d.truncated);
        assert!(d.error.is_none(), "весь вход — валидный JSON");
        assert!(d.text.len() <= MAX_TEXT_BYTES);
    }

    #[test]
    fn invalid_utf8_tail_is_cut_cleanly() {
        let mut v = vec![b'a'; MAX_TEXT_BYTES - 1];
        v.extend("я".as_bytes()); // 2-байтовый символ пересекает границу
        v.extend(vec![b'b'; 10]);
        let d = decode(&v, DataFormat::String);
        assert!(d.truncated);
        assert_eq!(d.text.len(), MAX_TEXT_BYTES - 1);
        assert!(!d.text.contains('\u{FFFD}'), "символ на границе не превращается в �");
    }
```

- [ ] **Step 2: Зафиксировать поведение**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib browse::format`
Expected: PASS — это изменение производительности, а не поведения: новые тесты фиксируют контракт (длины, границы символов, валидация JSON по всему входу) и должны остаться зелёными после рефакторинга.

- [ ] **Step 3: Реализация** — в `format.rs` заменить тело `decode` и добавить хелперы:

```rust
pub fn decode(bytes: &[u8], format: DataFormat) -> DecodedData {
    let resolved = if format == DataFormat::Auto {
        detect(bytes)
    } else {
        format
    };
    let (text, truncated, error) = match resolved {
        DataFormat::String => {
            let (t, cut) = text_prefix(bytes, MAX_TEXT_BYTES);
            (t, cut, None)
        }
        DataFormat::Json => {
            // Валидность — по всему входу (без копирования), в text — только видимая часть.
            let error = serde_json::from_slice::<serde::de::IgnoredAny>(bytes)
                .err()
                .map(|e| format!("Невалидный JSON: {e}"));
            let (t, cut) = text_prefix(bytes, MAX_TEXT_BYTES);
            (t, cut, error)
        }
        DataFormat::Hex => {
            let take = bytes.len().min(MAX_TEXT_BYTES / 2);
            (hex::encode(&bytes[..take]), take < bytes.len(), None)
        }
        DataFormat::Base64 => {
            let take = bytes.len().min(MAX_TEXT_BYTES / 4 * 3);
            (
                base64::engine::general_purpose::STANDARD.encode(&bytes[..take]),
                take < bytes.len(),
                None,
            )
        }
        DataFormat::Auto => unreachable!("detect никогда не возвращает Auto"),
    };
    DecodedData {
        format: resolved,
        text,
        size: bytes.len(),
        truncated,
        error,
    }
}

/// Текст из первых `max` байтов: граница не режет UTF-8-символ, невалидные байты — `�`.
fn text_prefix(bytes: &[u8], max: usize) -> (String, bool) {
    if bytes.len() <= max {
        return (String::from_utf8_lossy(bytes).into_owned(), false);
    }
    let mut cut = max;
    // Не начинать хвост с продолжающего байта UTF-8 (10xxxxxx): откатываемся к началу символа.
    while cut > 0 && (bytes[cut] & 0b1100_0000) == 0b1000_0000 {
        cut -= 1;
    }
    (String::from_utf8_lossy(&bytes[..cut]).into_owned(), true)
}
```
Функцию `truncate` удалить (больше не используется).

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib browse`
Expected: PASS (включая прежний `truncates_on_char_boundary`).

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/browse/format.rs
git commit -m "perf(browse): декодер усекает payload до копирования и кодирования"
```

---

### Task 2: Буфер сообщений без склейки на пакет

Бэклог M1: редьюсер на каждый пакет склеивает весь буфер с пакетом и только потом обрезает. Общий хелпер понадобится и consumer-сессиям.

**Files:**
- Modify: `src/components/browse/state.ts`, `src/components/browse/state.test.ts`

**Interfaces:**
- Produces: `appendMessages(buffer: BrowsedMessage[], batch: BrowsedMessage[], limit: number): BrowsedMessage[]` — последние `limit` сообщений; при пакете ≥ `limit` старый буфер не копируется; если пакет пуст — возвращается тот же массив.

- [ ] **Step 1: Тесты** — в `state.test.ts`:

```ts
import { appendMessages } from "./state";

describe("appendMessages", () => {
  it("держит последние limit сообщений", () => {
    expect(appendMessages([msg(1), msg(2)], [msg(3)], 5).map((m) => m.offset)).toEqual([1, 2, 3]);
    expect(appendMessages([msg(1), msg(2)], [msg(3), msg(4)], 3).map((m) => m.offset)).toEqual([2, 3, 4]);
  });

  it("большой пакет вытесняет весь буфер", () => {
    const batch = [msg(5), msg(6), msg(7), msg(8)];
    expect(appendMessages([msg(1)], batch, 3).map((m) => m.offset)).toEqual([6, 7, 8]);
  });

  it("пустой пакет не создаёт новый массив", () => {
    const buf = [msg(1)];
    expect(appendMessages(buf, [], 3)).toBe(buf);
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/browse/state.test.ts`
Expected: FAIL — `appendMessages` не экспортирована.

- [ ] **Step 3: Реализация** — в `state.ts`:

```ts
/** Кольцевой буфер: одна аллокация на пакет, без промежуточной склейки всего буфера. */
export function appendMessages(buffer: BrowsedMessage[], batch: BrowsedMessage[], limit: number): BrowsedMessage[] {
  if (batch.length === 0) return buffer;
  if (batch.length >= limit) return batch.slice(batch.length - limit);
  const keep = Math.min(buffer.length, limit - batch.length);
  const out = buffer.slice(buffer.length - keep);
  for (const m of batch) out.push(m);
  return out;
}
```
и в `browseReducer` ветку `messages` заменить на:
```ts
        case "messages":
          return { ...s, messages: appendMessages(s.messages, e.messages, s.limit) };
```

- [ ] **Step 4: Тесты проходят**

Run: `pnpm vitest run src/components/browse`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/components/browse
git commit -m "perf(browse): буфер сообщений без склейки на каждый пакет"
```

---

### Task 3: Общий монитор переподключения с короткой пробой

Бэклог M1: проба восстановления блокирует цикл browse до 2 с (задержка реакции на «Остановить»). Монитор понадобится consumer-сессиям — выносим его в `kafka/recovery.rs`.

**Files:**
- Create: `src-tauri/src/kafka/recovery.rs`
- Modify: `src-tauri/src/kafka/mod.rs` (`pub mod recovery;`)
- Modify: `src-tauri/src/browse/session.rs`

**Interfaces:**
- Produces (`kafka::recovery`):
  - `pub const RECOVERY_WINDOW: Duration` (60 с), `pub const PROBE_INTERVAL: Duration` (5 с), `pub const PROBE_TIMEOUT: Duration` (**500 мс**)
  - `pub fn is_transient(err: &KafkaError) -> bool`
  - `pub struct RecoveryMonitor` (`Default`) с `pub fn on_transient(&mut self, now: Instant, err: KafkaError) -> bool`, `pub fn on_progress(&mut self) -> bool`, `pub fn should_probe(&self, now: Instant) -> bool`, `pub fn on_probe(&mut self, now: Instant, ok: bool) -> bool`, `pub fn expired(&self, now: Instant) -> Option<(Instant, &KafkaError)>`

- [ ] **Step 1: Перенос** — создать `src-tauri/src/kafka/recovery.rs` и перенести в него из `browse/session.rs` **без изменений логики**:
  - константы `RECOVERY_WINDOW`, `PROBE_INTERVAL`, `PROBE_TIMEOUT` (сделать `pub`);
  - функцию `is_transient` (сделать `pub`);
  - структуры `Degraded`, `RecoveryMonitor` и `impl RecoveryMonitor` (структуру и методы сделать `pub`);
  - из `mod tests` файла `session.rs` — хелперы `secs`, `transient` и все тесты `monitor_*` (в новый `#[cfg(test)] mod tests` с `use super::*;`, `use rdkafka::types::RDKafkaErrorCode;`).

Импорты нового файла:
```rust
use std::time::{Duration, Instant};

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
```

В `browse/session.rs` удалить перенесённое и импортировать:
```rust
use crate::kafka::recovery::{PROBE_TIMEOUT, RecoveryMonitor, is_transient};
```

- [ ] **Step 2: Тест короткой пробы** — в `mod tests` файла `recovery.rs`:

```rust
    #[test]
    fn probe_is_short_enough_for_responsive_cancel() {
        // Цикл сессии проверяет отмену между пробами: проба не должна держать его дольше 0,5 с.
        assert!(PROBE_TIMEOUT <= Duration::from_millis(500));
        assert!(PROBE_TIMEOUT < PROBE_INTERVAL);
    }
```

- [ ] **Step 3: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::recovery`
Expected: FAIL — `probe_is_short_enough_for_responsive_cancel` (PROBE_TIMEOUT = 2 с).

- [ ] **Step 4: Реализация** — в `recovery.rs`:

```rust
/// Проба метаданных во время деградации. Короткая: цикл сессии проверяет отмену между пробами,
/// а частота проб задаётся PROBE_INTERVAL, так что длинный таймаут ничего не даёт.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);
```

- [ ] **Step 5: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS; тесты `monitor_*` теперь в `kafka::recovery::tests`.
Run (Docker): `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_browse`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src-tauri/src
git commit -m "refactor(kafka): общий монитор переподключения, проба 500 мс"
```

---

### Task 4: Сообщение для отправки и кодирование payload

**Files:**
- Create: `src-tauri/src/produce/mod.rs`, `src-tauri/src/produce/payload.rs`
- Modify: `src-tauri/src/lib.rs` (`pub mod produce;`)

**Interfaces:**
- Produces (`produce::payload`, все DTO — serde camelCase, `Serialize + Deserialize`):
  - `enum PayloadFormat { String, Json, Hex, Base64, Null }` (JSON — строчными)
  - `struct PayloadInput { pub format: PayloadFormat, pub text: String }` (`text` по умолчанию пуст)
  - `enum HeaderFormat { String, Hex }` (по умолчанию `String`)
  - `struct HeaderInput { pub name: String, pub value: String, pub format: HeaderFormat }`
  - `struct ProduceMessage { pub topic: String, pub partition: Option<i32>, pub key: PayloadInput, pub value: PayloadInput, pub headers: Vec<HeaderInput> }`
  - `struct EncodedMessage { pub topic: String, pub partition: Option<i32>, pub key: Option<Vec<u8>>, pub value: Option<Vec<u8>>, pub headers: Vec<(String, Vec<u8>)> }`
  - `fn encode_payload(p: &PayloadInput, what: &str) -> AppResult<Option<Vec<u8>>>` — `what` («ключ»/«значение») входит в текст ошибки
  - `fn encode_message(m: &ProduceMessage) -> AppResult<EncodedMessage>` — топик не пуст, партиция ≥ 0, имена заголовков не пусты

- [ ] **Step 1: Тесты** — создать `src-tauri/src/produce/payload.rs` с тестами, `src-tauri/src/produce/mod.rs` с `pub mod payload;`, добавить `pub mod produce;` в `lib.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn p(format: PayloadFormat, text: &str) -> PayloadInput {
        PayloadInput { format, text: text.into() }
    }

    #[test]
    fn encodes_each_format() {
        assert_eq!(encode_payload(&p(PayloadFormat::String, "привет"), "значение").unwrap(), Some("привет".as_bytes().to_vec()));
        assert_eq!(encode_payload(&p(PayloadFormat::Json, " {\"a\": 1} "), "значение").unwrap(), Some(b" {\"a\": 1} ".to_vec()));
        assert_eq!(encode_payload(&p(PayloadFormat::Hex, "de ad\nBE EF"), "ключ").unwrap(), Some(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(encode_payload(&p(PayloadFormat::Base64, " QUI= "), "ключ").unwrap(), Some(b"AB".to_vec()));
        assert_eq!(encode_payload(&p(PayloadFormat::Null, "ignored"), "ключ").unwrap(), None);
        assert_eq!(encode_payload(&p(PayloadFormat::String, ""), "ключ").unwrap(), Some(vec![]));
    }

    #[test]
    fn invalid_payloads_are_invalid_input() {
        for (f, t) in [(PayloadFormat::Json, "{oops"), (PayloadFormat::Hex, "abc"), (PayloadFormat::Hex, "zz"), (PayloadFormat::Base64, "%%%")] {
            let e = encode_payload(&p(f, t), "значение").unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{f:?} {t}");
            assert!(e.message.contains("значение"), "{}", e.message);
        }
    }

    fn msg() -> ProduceMessage {
        ProduceMessage {
            topic: "t".into(),
            partition: None,
            key: p(PayloadFormat::String, "k"),
            value: p(PayloadFormat::Json, "{}"),
            headers: vec![
                HeaderInput { name: "trace".into(), value: "abc".into(), format: HeaderFormat::String },
                HeaderInput { name: "bin".into(), value: "00ff".into(), format: HeaderFormat::Hex },
            ],
        }
    }

    #[test]
    fn encodes_message_with_headers() {
        let e = encode_message(&msg()).unwrap();
        assert_eq!(e.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(e.headers, vec![("trace".to_string(), b"abc".to_vec()), ("bin".to_string(), vec![0x00, 0xff])]);
    }

    #[test]
    fn message_validation() {
        let mut m = msg();
        m.topic = " ".into();
        assert_eq!(encode_message(&m).unwrap_err().code, ErrorCode::InvalidInput);
        let mut m = msg();
        m.partition = Some(-1);
        assert_eq!(encode_message(&m).unwrap_err().code, ErrorCode::InvalidInput);
        let mut m = msg();
        m.headers[0].name = "".into();
        assert_eq!(encode_message(&m).unwrap_err().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn json_shape() {
        let m: ProduceMessage = serde_json::from_value(serde_json::json!({
            "topic": "t",
            "key": {"format": "null"},
            "value": {"format": "string", "text": "v"},
            "headers": [{"name": "h", "value": "1"}]
        }))
        .unwrap();
        assert_eq!(m.partition, None);
        assert_eq!(m.key.format, PayloadFormat::Null);
        assert_eq!(m.headers[0].format, HeaderFormat::String);
        assert_eq!(serde_json::to_value(PayloadFormat::Base64).unwrap(), "base64");
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib produce::payload`
Expected: FAIL — `cannot find function encode_payload`.

- [ ] **Step 3: Реализация** — над тестами в `payload.rs`:

```rust
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadFormat {
    String,
    Json,
    Hex,
    Base64,
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadInput {
    pub format: PayloadFormat,
    #[serde(default)]
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HeaderFormat {
    #[default]
    String,
    Hex,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderInput {
    pub name: String,
    pub value: String,
    #[serde(default)]
    pub format: HeaderFormat,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceMessage {
    pub topic: String,
    /// None — партицию выбирает партиционер librdkafka.
    #[serde(default)]
    pub partition: Option<i32>,
    pub key: PayloadInput,
    pub value: PayloadInput,
    #[serde(default)]
    pub headers: Vec<HeaderInput>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EncodedMessage {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    hex::decode(&compact).map_err(|e| e.to_string())
}

pub fn encode_payload(p: &PayloadInput, what: &str) -> AppResult<Option<Vec<u8>>> {
    let bad = |e: String| AppError::invalid(format!("Некорректное {what}: {e}"));
    Ok(match p.format {
        PayloadFormat::Null => None,
        PayloadFormat::String => Some(p.text.as_bytes().to_vec()),
        PayloadFormat::Json => {
            serde_json::from_str::<serde::de::IgnoredAny>(&p.text)
                .map_err(|e| bad(format!("невалидный JSON: {e}")))?;
            Some(p.text.as_bytes().to_vec())
        }
        PayloadFormat::Hex => Some(decode_hex(&p.text).map_err(|e| bad(format!("невалидный hex: {e}")))?),
        PayloadFormat::Base64 => Some(
            base64::engine::general_purpose::STANDARD
                .decode(p.text.trim())
                .map_err(|e| bad(format!("невалидный base64: {e}")))?,
        ),
    })
}

pub fn encode_message(m: &ProduceMessage) -> AppResult<EncodedMessage> {
    if m.topic.trim().is_empty() {
        return Err(AppError::invalid("Не указан топик"));
    }
    if let Some(p) = m.partition
        && p < 0
    {
        return Err(AppError::invalid("Номер партиции не может быть отрицательным"));
    }
    let mut headers = Vec::with_capacity(m.headers.len());
    for h in &m.headers {
        if h.name.trim().is_empty() {
            return Err(AppError::invalid("Пустое имя заголовка"));
        }
        let value = match h.format {
            HeaderFormat::String => h.value.as_bytes().to_vec(),
            HeaderFormat::Hex => decode_hex(&h.value).map_err(|e| {
                AppError::invalid(format!("Заголовок «{}»: невалидный hex: {e}", h.name))
            })?,
        };
        headers.push((h.name.clone(), value));
    }
    Ok(EncodedMessage {
        topic: m.topic.trim().to_string(),
        partition: m.partition,
        key: encode_payload(&m.key, "ключ")?,
        value: encode_payload(&m.value, "значение")?,
        headers,
    })
}
```

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib produce`
Expected: PASS, 5 тестов.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src
git commit -m "feat(produce): сообщение для отправки и кодирование форматов"
```

---

### Task 5: Плейсхолдеры шаблонов

**Files:**
- Modify: `src-tauri/Cargo.toml` (зависимости `chrono`, `fastrand`)
- Create: `src-tauri/src/produce/template.rs`
- Modify: `src-tauri/src/produce/mod.rs` (`pub mod template;`)

**Interfaces:**
- Consumes: `ProduceMessage`, `PayloadInput`, `PayloadFormat`
- Produces:
  - `struct RenderContext<'a> { pub seq: u64, pub now: chrono::DateTime<chrono::Utc>, pub rng: &'a mut fastrand::Rng, pub env: &'a dyn Fn(&str) -> Option<String> }`
  - `fn render(template: &str, ctx: &mut RenderContext) -> AppResult<String>`
  - `fn render_message(m: &ProduceMessage, ctx: &mut RenderContext) -> AppResult<ProduceMessage>` — рендерит `key.text`, `value.text` (кроме формата `null`) и значения заголовков; `{{seq}}` у всех полей одного сообщения одинаков
  - `fn system_env(name: &str) -> Option<String>` — `std::env::var(name).ok()`

- [ ] **Step 1: Зависимости** — в `src-tauri/Cargo.toml` в `[dependencies]`:

```toml
chrono = { version = "0.4", default-features = false, features = ["clock", "std"] }
fastrand = "2"
```

- [ ] **Step 2: Тесты** — создать `src-tauri/src/produce/template.rs` с тестами, добавить `pub mod template;` в `produce/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::produce::payload::{HeaderFormat, HeaderInput, PayloadFormat, PayloadInput};
    use chrono::TimeZone;

    fn env(name: &str) -> Option<String> {
        (name == "TEAM").then(|| "qa".to_string())
    }

    fn with_ctx<T>(seq: u64, f: impl FnOnce(&mut RenderContext) -> T) -> T {
        let mut rng = fastrand::Rng::with_seed(42);
        let mut ctx = RenderContext {
            seq,
            now: chrono::Utc.with_ymd_and_hms(2026, 10, 15, 12, 30, 0).unwrap(),
            rng: &mut rng,
            env: &env,
        };
        f(&mut ctx)
    }

    #[test]
    fn simple_placeholders() {
        let out = with_ctx(7, |c| render("#{{seq}} at {{now}} ({{nowMillis}}) team={{env TEAM}}", c)).unwrap();
        assert_eq!(out, "#7 at 2026-10-15T12:30:00.000Z (1792067400000) team=qa");
        assert_eq!(with_ctx(0, |c| render("без плейсхолдеров", c)).unwrap(), "без плейсхолдеров");
        assert_eq!(with_ctx(0, |c| render("{{ seq }}", c)).unwrap(), "0", "пробелы внутри скобок допустимы");
    }

    #[test]
    fn random_placeholders_respect_arguments() {
        for _ in 0..50 {
            let n: i64 = with_ctx(0, |c| render("{{randomInt -5 5}}", c)).unwrap().parse().unwrap();
            assert!((-5..=5).contains(&n));
        }
        let s = with_ctx(0, |c| render("{{randomString 12}}", c)).unwrap();
        assert_eq!(s.len(), 12);
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
        let u = with_ctx(0, |c| render("{{uuid}}", c)).unwrap();
        assert_eq!(uuid::Uuid::parse_str(&u).unwrap().get_version_num(), 4);
        // Один и тот же seed — одинаковый результат (детерминированность тестов и предпросмотра).
        assert_eq!(u, with_ctx(0, |c| render("{{uuid}}", c)).unwrap());
    }

    #[test]
    fn errors_are_invalid_input() {
        for bad in [
            "{{unknown}}",
            "{{randomInt 5}}",
            "{{randomInt 9 1}}",
            "{{randomInt a b}}",
            "{{randomString}}",
            "{{randomString 100001}}",
            "{{env}}",
            "{{env MISSING_VAR}}",
            "{{seq",
        ] {
            let e = with_ctx(0, |c| render(bad, c)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
        }
    }

    #[test]
    fn message_rendering_shares_seq_and_skips_null() {
        let m = ProduceMessage {
            topic: "t".into(),
            partition: None,
            key: PayloadInput { format: PayloadFormat::String, text: "k-{{seq}}".into() },
            value: PayloadInput { format: PayloadFormat::Null, text: "{{unknown}}".into() },
            headers: vec![HeaderInput { name: "n".into(), value: "{{seq}}".into(), format: HeaderFormat::String }],
        };
        let r = with_ctx(3, |c| render_message(&m, c)).unwrap();
        assert_eq!(r.key.text, "k-3");
        assert_eq!(r.headers[0].value, "3");
        assert_eq!(r.value.text, "{{unknown}}", "null-значение не рендерится");
    }
}
```

- [ ] **Step 3: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib produce::template`
Expected: FAIL — `cannot find struct RenderContext`.

- [ ] **Step 4: Реализация** — над тестами в `template.rs`:

```rust
use chrono::{DateTime, SecondsFormat, Utc};

use crate::error::{AppError, AppResult};
use crate::produce::payload::{PayloadFormat, ProduceMessage};

const MAX_RANDOM_STRING: usize = 100_000;

pub struct RenderContext<'a> {
    /// Номер сообщения в пакете (0 для одиночной отправки).
    pub seq: u64,
    pub now: DateTime<Utc>,
    pub rng: &'a mut fastrand::Rng,
    pub env: &'a dyn Fn(&str) -> Option<String>,
}

pub fn system_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn invalid(msg: String) -> AppError {
    AppError::invalid(msg)
}

fn eval(expr: &str, ctx: &mut RenderContext) -> AppResult<String> {
    let mut parts = expr.split_whitespace();
    let name = parts.next().unwrap_or("");
    let args: Vec<&str> = parts.collect();
    let arity = |n: usize| -> AppResult<()> {
        if args.len() == n {
            Ok(())
        } else {
            Err(invalid(format!("{{{{{expr}}}}}: ожидается аргументов — {n}")))
        }
    };
    match name {
        "uuid" => {
            arity(0)?;
            let mut bytes = [0u8; 16];
            ctx.rng.fill(&mut bytes);
            Ok(uuid::Builder::from_random_bytes(bytes).into_uuid().to_string())
        }
        "now" => {
            arity(0)?;
            Ok(ctx.now.to_rfc3339_opts(SecondsFormat::Millis, true))
        }
        "nowMillis" => {
            arity(0)?;
            Ok(ctx.now.timestamp_millis().to_string())
        }
        "seq" => {
            arity(0)?;
            Ok(ctx.seq.to_string())
        }
        "randomInt" => {
            arity(2)?;
            let parse = |s: &str| {
                s.parse::<i64>()
                    .map_err(|_| invalid(format!("{{{{{expr}}}}}: «{s}» — не целое число")))
            };
            let (min, max) = (parse(args[0])?, parse(args[1])?);
            if min > max {
                return Err(invalid(format!("{{{{{expr}}}}}: min больше max")));
            }
            Ok(ctx.rng.i64(min..=max).to_string())
        }
        "randomString" => {
            arity(1)?;
            let n: usize = args[0]
                .parse()
                .ok()
                .filter(|n| *n <= MAX_RANDOM_STRING)
                .ok_or_else(|| invalid(format!("{{{{{expr}}}}}: длина — число от 0 до {MAX_RANDOM_STRING}")))?;
            Ok((0..n).map(|_| ctx.rng.alphanumeric()).collect())
        }
        "env" => {
            arity(1)?;
            (ctx.env)(args[0]).ok_or_else(|| invalid(format!("Переменная окружения {} не задана", args[0])))
        }
        _ => Err(invalid(format!("Неизвестный плейсхолдер {{{{{expr}}}}}"))),
    }
}

/// Подставляет плейсхолдеры `{{…}}`. Экранирования нет: литерал `{{` в тексте недопустим.
pub fn render(template: &str, ctx: &mut RenderContext) -> AppResult<String> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find("}}")
            .ok_or_else(|| invalid("Незакрытый плейсхолдер: нет «}}»".to_string()))?;
        out.push_str(&eval(after[..end].trim(), ctx)?);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

pub fn render_message(m: &ProduceMessage, ctx: &mut RenderContext) -> AppResult<ProduceMessage> {
    let mut r = m.clone();
    if r.key.format != PayloadFormat::Null {
        r.key.text = render(&m.key.text, ctx)?;
    }
    if r.value.format != PayloadFormat::Null {
        r.value.text = render(&m.value.text, ctx)?;
    }
    for h in &mut r.headers {
        h.value = render(&h.value, ctx)?;
    }
    Ok(r)
}
```

- [ ] **Step 5: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib produce`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src-tauri/Cargo.toml src-tauri/Cargo.lock src-tauri/src/produce
git commit -m "feat(produce): плейсхолдеры шаблонов"
```

---

### Task 6: Пул producer'ов и одиночная отправка

**Files:**
- Modify: `src-tauri/src/kafka/pool.rs` (`KeyedPool::ids`, `KeyedPool::evict_where`)
- Create: `src-tauri/src/kafka/producer.rs`
- Modify: `src-tauri/src/kafka/mod.rs` (`pub mod producer;`)
- Modify: `src-tauri/src/state.rs` (поле `producers`, метод `evict_connection`)
- Modify: `src-tauri/src/commands/connections.rs` (`st.pool.evict(…)` → `st.evict_connection(…)` во всех местах)
- Create: `src-tauri/tests/it_produce.rs`

**Interfaces:**
- Consumes: `KeyedPool`, `build_client_config`, `CapturingContext`, `EncodedMessage`, `errors::item_error`
- Produces:
  - `KeyedPool::ids(&self) -> Vec<String>`, `KeyedPool::evict_where(&self, pred: impl Fn(&str) -> bool)`
  - `enum Acks { Zero, One, All }` (JSON `"0"`, `"1"`, `"all"`), `enum Compression { None, Gzip, Snappy, Lz4, Zstd }` (строчными)
  - `struct ProducerSettings { pub acks: Acks, pub compression: Compression, pub idempotence: bool }` (camelCase, `Default` = acks all, none, false); `fn validate(&self) -> AppResult<()>`; `fn apply(&self, cfg: &mut ClientConfig)`; `fn pool_key(&self) -> String`
  - `struct ProducerHandle { pub producer: FutureProducer<CapturingContext>, pub ctx: CapturingContext, pub timeout: Duration }`, `fn ProducerHandle::create(p: &ConnectionProfile, secrets: &dyn SecretStore, s: &ProducerSettings) -> AppResult<Self>`
  - `struct ProducerPool` (`Default`): `fn get(&self, p, secrets, s) -> AppResult<Arc<ProducerHandle>>` (не больше `MAX_PRODUCERS_PER_CONNECTION = 4`), `fn evict_connection(&self, connection_id: &str)`
  - `struct ProduceResult { pub partition: i32, pub offset: i64, pub timestamp: i64 }` (Serialize + Deserialize camelCase)
  - `async fn send(h: &ProducerHandle, m: &EncodedMessage) -> AppResult<ProduceResult>` — timestamp сообщения (CreateTime) = текущее время, возвращается в результате
  - `AppState::producers: Arc<ProducerPool>`, `fn AppState::evict_connection(&self, id: &str)` — вытесняет и admin/consumer-handles, и producer'ы

- [ ] **Step 1: Тесты пула** — в `mod tests` файла `kafka/pool.rs`:

```rust
    #[test]
    fn keyed_pool_lists_and_evicts_by_predicate() {
        let pool = KeyedPool::<u32>::default();
        for id in ["a|1", "a|2", "b|1"] {
            pool.get_or_create(id, || Ok(0)).unwrap();
        }
        let mut ids = pool.ids();
        ids.sort();
        assert_eq!(ids, ["a|1", "a|2", "b|1"]);
        pool.evict_where(|id| id.starts_with("a|"));
        assert_eq!(pool.ids(), ["b|1"]);
    }
```

И unit-тесты настроек — создать `src-tauri/src/kafka/producer.rs` с тестами, добавить `pub mod producer;` в `kafka/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn settings_json_and_config() {
        let s: ProducerSettings =
            serde_json::from_value(serde_json::json!({"acks": "1", "compression": "zstd", "idempotence": false})).unwrap();
        let mut cfg = ClientConfig::new();
        s.apply(&mut cfg);
        assert_eq!(cfg.get("acks"), Some("1"));
        assert_eq!(cfg.get("compression.type"), Some("zstd"));
        assert_eq!(cfg.get("enable.idempotence"), Some("false"));
        assert_eq!(s.pool_key(), "acks=1;compression=zstd;idempotence=false");
        assert_eq!(serde_json::to_value(ProducerSettings::default()).unwrap(), serde_json::json!({"acks": "all", "compression": "none", "idempotence": false}));
    }

    #[test]
    fn idempotence_requires_acks_all() {
        let s = ProducerSettings { acks: Acks::One, compression: Compression::None, idempotence: true };
        assert_eq!(s.validate().unwrap_err().code, ErrorCode::InvalidInput);
        assert!(ProducerSettings { acks: Acks::All, ..s }.validate().is_ok());
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::pool kafka::producer`
Expected: FAIL — нет `ids`, `ProducerSettings`.

- [ ] **Step 3: `KeyedPool`** — в `impl<T> KeyedPool<T>` (`pool.rs`):

```rust
    pub fn ids(&self) -> Vec<String> {
        self.inner.lock_safe().items.keys().cloned().collect()
    }

    /// Вытесняет все элементы, чьи id удовлетворяют предикату (освобождение — вне мьютекса).
    pub fn evict_where(&self, pred: impl Fn(&str) -> bool) {
        let removed: Vec<Arc<T>> = {
            let mut st = self.inner.lock_safe();
            let ids: Vec<String> = st.items.keys().filter(|id| pred(id)).cloned().collect();
            ids.iter()
                .filter_map(|id| {
                    *st.generations.entry(id.clone()).or_insert(0) += 1;
                    st.items.remove(id)
                })
                .collect()
        };
        drop(removed);
    }
```

- [ ] **Step 4: `producer.rs`** — над тестами:

```rust
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, from_kafka};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::item_error;
use crate::kafka::pool::KeyedPool;
use crate::produce::payload::EncodedMessage;
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;

pub const MAX_PRODUCERS_PER_CONNECTION: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Acks {
    #[serde(rename = "0")]
    Zero,
    #[serde(rename = "1")]
    One,
    #[default]
    #[serde(rename = "all")]
    All,
}

impl Acks {
    fn as_str(self) -> &'static str {
        match self {
            Acks::Zero => "0",
            Acks::One => "1",
            Acks::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Compression {
    fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Snappy => "snappy",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducerSettings {
    pub acks: Acks,
    pub compression: Compression,
    pub idempotence: bool,
}

impl ProducerSettings {
    pub fn validate(&self) -> AppResult<()> {
        if self.idempotence && self.acks != Acks::All {
            return Err(AppError::invalid("Идемпотентность требует acks = all"));
        }
        Ok(())
    }

    pub fn apply(&self, cfg: &mut ClientConfig) {
        cfg.set("acks", self.acks.as_str())
            .set("compression.type", self.compression.as_str())
            .set("enable.idempotence", if self.idempotence { "true" } else { "false" });
    }

    pub fn pool_key(&self) -> String {
        format!(
            "acks={};compression={};idempotence={}",
            self.acks.as_str(),
            self.compression.as_str(),
            self.idempotence
        )
    }
}

pub struct ProducerHandle {
    pub producer: FutureProducer<CapturingContext>,
    pub ctx: CapturingContext,
    pub timeout: Duration,
}

impl ProducerHandle {
    pub fn create(p: &ConnectionProfile, secrets: &dyn SecretStore, s: &ProducerSettings) -> AppResult<Self> {
        s.validate()?;
        let mut cfg = build_client_config(p, secrets)?;
        s.apply(&mut cfg);
        cfg.set("message.timeout.ms", p.request_timeout_ms.to_string());
        let ctx = CapturingContext::default();
        let producer = cfg
            .create_with_context(ctx.clone())
            .map_err(|e| from_kafka(&e, None))?;
        Ok(Self {
            producer,
            ctx,
            timeout: Duration::from_millis(p.request_timeout_ms),
        })
    }
}

/// Producer'ы по ключу «подключение|настройки».
#[derive(Default)]
pub struct ProducerPool {
    inner: KeyedPool<ProducerHandle>,
}

impl ProducerPool {
    pub fn get(
        &self,
        p: &ConnectionProfile,
        secrets: &dyn SecretStore,
        s: &ProducerSettings,
    ) -> AppResult<Arc<ProducerHandle>> {
        let prefix = format!("{}|", p.id);
        let key = format!("{prefix}{}", s.pool_key());
        let existing = self.inner.ids();
        let of_connection = existing.iter().filter(|id| id.starts_with(&prefix)).count();
        if !existing.contains(&key) && of_connection >= MAX_PRODUCERS_PER_CONNECTION {
            // Редкий случай (много разных настроек): проще пересоздать, чем вести LRU.
            self.evict_connection(&p.id);
        }
        self.inner.get_or_create(&key, || ProducerHandle::create(p, secrets, s))
    }

    pub fn evict_connection(&self, connection_id: &str) {
        let prefix = format!("{connection_id}|");
        self.inner.evict_where(|id| id.starts_with(&prefix));
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceResult {
    pub partition: i32,
    pub offset: i64,
    /// CreateTime, проставленный при отправке (мс).
    pub timestamp: i64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn owned_headers(headers: &[(String, Vec<u8>)]) -> OwnedHeaders {
    headers.iter().fold(OwnedHeaders::new(), |acc, (k, v)| {
        acc.insert(Header {
            key: k,
            value: Some(v.as_slice()),
        })
    })
}

/// Отправка с ожиданием подтверждения не дольше таймаута запроса.
pub async fn send(h: &ProducerHandle, m: &EncodedMessage) -> AppResult<ProduceResult> {
    let timestamp = now_ms();
    let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(&m.topic)
        .timestamp(timestamp)
        .headers(owned_headers(&m.headers));
    if let Some(k) = &m.key {
        record = record.key(k.as_slice());
    }
    if let Some(v) = &m.value {
        record = record.payload(v.as_slice());
    }
    if let Some(p) = m.partition {
        record = record.partition(p);
    }
    let started = Instant::now();
    match h.producer.send(record, Timeout::After(h.timeout)).await {
        Ok((partition, offset)) => Ok(ProduceResult {
            partition,
            offset,
            timestamp,
        }),
        Err((e, _)) => {
            let cause = h.ctx.last_error_since(started);
            Err(match &e {
                KafkaError::MessageProduction(code) => {
                    item_error(*code, cause.as_deref().unwrap_or(""), "Не удалось отправить сообщение")
                }
                _ => from_kafka(&e, cause.as_deref()),
            })
        }
    }
}
```

- [ ] **Step 5: `AppState`** — в `src-tauri/src/state.rs`: поле `pub producers: Arc<crate::kafka::producer::ProducerPool>,` (в `new` — `Arc::new(Default::default())`) и метод:

```rust
    /// Вытесняет все клиенты подключения: admin/consumer-handles и producer'ы.
    pub fn evict_connection(&self, id: &str) {
        self.pool.evict(id);
        self.producers.evict_connection(id);
    }
```
В `evict_if_client_broken` заменить `self.pool.evict(id)` на `self.evict_connection(id)`. В `src-tauri/src/commands/connections.rs` заменить все `st.pool.evict(` на `st.evict_connection(` (в `save_connection_blocking`, `delete_connection`, `disconnect`).

- [ ] **Step 6: Unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`
Expected: PASS.

- [ ] **Step 7: Интеграционный тест** — создать `src-tauri/tests/it_produce.rs`:

```rust
mod common;

use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::producer::{ProducerHandle, ProducerSettings, send};
use kafka_explorer_lib::produce::payload::EncodedMessage;
use kafka_explorer_lib::secrets::MemoryStore;

fn message(topic: &str, partition: Option<i32>) -> EncodedMessage {
    EncodedMessage {
        topic: topic.into(),
        partition,
        key: Some(b"k1".to_vec()),
        value: Some(br#"{"n":1}"#.to_vec()),
        headers: vec![("trace".into(), b"abc".to_vec())],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sends_and_reports_partition_offset() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-produce", 2).await;
    let h = ProducerHandle::create(&common::plaintext_profile(&bs), &MemoryStore::default(), &ProducerSettings::default()).unwrap();
    let r1 = send(&h, &message(&topic, Some(1))).await.unwrap();
    let r2 = send(&h, &message(&topic, Some(1))).await.unwrap();
    assert_eq!((r1.partition, r1.offset), (1, 0));
    assert_eq!((r2.partition, r2.offset), (1, 1));
    assert!(r1.timestamp > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_partition_is_reported() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-produce-bad", 1).await;
    let mut p = common::plaintext_profile(&bs);
    p.request_timeout_ms = 5_000;
    let h = ProducerHandle::create(&p, &MemoryStore::default(), &ProducerSettings::default()).unwrap();
    let e = send(&h, &message(&topic, Some(7))).await.unwrap_err();
    assert_ne!(e.code, ErrorCode::Internal, "{e:?}");
}
```

- [ ] **Step 8: Тесты проходят**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_produce && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS. Если `missing_partition_is_reported` получает `INTERNAL`, выведите `details` ошибки и добавьте код librdkafka (ожидается `UnknownPartition`) в ветку `InvalidInput` функции `item_error` с unit-тестом.

- [ ] **Step 9: Commit**

```bash
git add src-tauri/src src-tauri/tests/it_produce.rs
git commit -m "feat(kafka): пул producer'ов и одиночная отправка"
```

---

### Task 7: Хранилища истории и шаблонов

**Files:**
- Create: `src-tauri/src/storage/history.rs`, `src-tauri/src/storage/templates.rs`
- Modify: `src-tauri/src/storage/mod.rs` (`pub mod history; pub mod templates;`)
- Modify: `src-tauri/src/state.rs` (поля `history`, `templates`)
- Modify: `src-tauri/src/commands/connections.rs` (удаление подключения чистит историю и его шаблоны)

**Interfaces:**
- Consumes: `storage::fs::{read_json, write_json}`, `sync::{MutexExt, RwLockExt}`, `ProduceMessage`, `ProducerSettings`, `ProduceResult`
- Produces:
  - `const HISTORY_LIMIT: usize = 100`, `const MAX_HISTORY_TEXT: usize = 64 * 1024`
  - `struct HistoryEntry { pub id: String, pub sent_at: i64, pub message: ProduceMessage, pub settings: ProducerSettings, pub result: ProduceResult, pub truncated: bool }` (serde camelCase)
  - `fn HistoryEntry::new(message: ProduceMessage, settings: ProducerSettings, result: ProduceResult) -> Self` (id — uuid, `sent_at` = `result.timestamp`)
  - `fn truncate_for_history(m: &mut ProduceMessage) -> bool` — усечь `key.text`, `value.text` и значения заголовков до 64 КБ по границе символа; true, если что-то усечено
  - `struct HistoryStore`: `fn open(data_dir: &Path) -> AppResult<Self>`, `fn list(&self, connection_id: &str) -> AppResult<Vec<HistoryEntry>>` (новые первыми), `fn append(&self, connection_id: &str, entry: HistoryEntry) -> AppResult<()>`, `fn delete_connection(&self, connection_id: &str) -> AppResult<()>`
  - `struct Template { pub id: String, pub name: String, pub connection_id: Option<String>, pub message: ProduceMessage, pub settings: ProducerSettings }` (serde camelCase; `id` по умолчанию пуст)
  - `struct TemplateStore`: `fn open(data_dir: &Path) -> AppResult<Self>`, `fn list(&self, connection_id: &str) -> Vec<Template>` (глобальные + этого подключения, по имени), `fn save(&self, t: Template) -> AppResult<Template>` (пустой id → новый uuid), `fn delete(&self, id: &str) -> AppResult<()>`, `fn delete_for_connection(&self, connection_id: &str) -> AppResult<()>`
  - `AppState::history: Arc<HistoryStore>`, `AppState::templates: Arc<TemplateStore>`

- [ ] **Step 1: Тесты истории** — создать `src-tauri/src/storage/history.rs` с тестами:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::kafka::producer::{ProduceResult, ProducerSettings};
    use crate::produce::payload::{PayloadFormat, PayloadInput};

    fn entry(n: i64, value: &str) -> HistoryEntry {
        HistoryEntry::new(
            ProduceMessage {
                topic: "t".into(),
                partition: None,
                key: PayloadInput { format: PayloadFormat::Null, text: String::new() },
                value: PayloadInput { format: PayloadFormat::String, text: value.into() },
                headers: vec![],
            },
            ProducerSettings::default(),
            ProduceResult { partition: 0, offset: n, timestamp: n },
        )
    }

    #[test]
    fn keeps_last_100_newest_first_per_connection() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        for n in 0..105 {
            store.append("c1", entry(n, "v")).unwrap();
        }
        store.append("c2", entry(999, "v")).unwrap();
        let reopened = HistoryStore::open(dir.path()).unwrap();
        let list = reopened.list("c1").unwrap();
        assert_eq!(list.len(), HISTORY_LIMIT);
        assert_eq!(list[0].result.offset, 104);
        assert_eq!(list[99].result.offset, 5);
        assert_eq!(reopened.list("c2").unwrap().len(), 1);
        assert!(reopened.list("none").unwrap().is_empty());
        reopened.delete_connection("c1").unwrap();
        assert!(reopened.list("c1").unwrap().is_empty());
    }

    #[test]
    fn large_values_are_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        store.append("c", entry(1, &"я".repeat(MAX_HISTORY_TEXT))).unwrap();
        let e = &store.list("c").unwrap()[0];
        assert!(e.truncated);
        assert!(e.message.value.text.len() <= MAX_HISTORY_TEXT);
        store.append("c", entry(2, "short")).unwrap();
        assert!(!store.list("c").unwrap()[0].truncated);
    }

    #[test]
    fn rejects_unsafe_connection_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(dir.path()).unwrap();
        assert_eq!(store.list("../x").unwrap_err().code, ErrorCode::InvalidInput);
    }
}
```

- [ ] **Step 2: Тесты шаблонов** — создать `src-tauri/src/storage/templates.rs` с тестами, добавить оба модуля в `storage/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::produce::payload::{PayloadFormat, PayloadInput};

    fn template(name: &str, conn: Option<&str>) -> Template {
        Template {
            id: String::new(),
            name: name.into(),
            connection_id: conn.map(String::from),
            message: ProduceMessage {
                topic: "orders".into(),
                partition: None,
                key: PayloadInput { format: PayloadFormat::String, text: "{{uuid}}".into() },
                value: PayloadInput { format: PayloadFormat::Json, text: "{\"n\":{{seq}}}".into() },
                headers: vec![],
            },
            settings: ProducerSettings::default(),
        }
    }

    #[test]
    fn save_list_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemplateStore::open(dir.path()).unwrap();
        let g = store.save(template("глобальный", None)).unwrap();
        assert!(!g.id.is_empty());
        let a = store.save(template("a", Some("c1"))).unwrap();
        store.save(template("b", Some("c2"))).unwrap();
        let reopened = TemplateStore::open(dir.path()).unwrap();
        let names: Vec<String> = reopened.list("c1").into_iter().map(|t| t.name).collect();
        assert_eq!(names, ["a", "глобальный"]);
        let mut renamed = a.clone();
        renamed.name = "a2".into();
        reopened.save(renamed).unwrap();
        assert_eq!(reopened.list("c1").len(), 2, "сохранение с тем же id заменяет шаблон");
        reopened.delete(&a.id).unwrap();
        assert_eq!(reopened.delete(&a.id).unwrap_err().code, ErrorCode::NotFound);
        reopened.delete_for_connection("c2").unwrap();
        assert_eq!(reopened.list("c2").len(), 1, "остался только глобальный");
    }

    #[test]
    fn name_is_required() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemplateStore::open(dir.path()).unwrap();
        assert_eq!(store.save(template(" ", None)).unwrap_err().code, ErrorCode::InvalidInput);
    }
}
```

- [ ] **Step 3: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib storage`
Expected: FAIL — нет `HistoryStore`, `TemplateStore`.

- [ ] **Step 4: Реализация истории** — над тестами в `history.rs`:

```rust
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::kafka::producer::{ProduceResult, ProducerSettings};
use crate::produce::payload::ProduceMessage;
use crate::storage::fs::{read_json, write_json};
use crate::sync::MutexExt;

pub const HISTORY_LIMIT: usize = 100;
pub const MAX_HISTORY_TEXT: usize = 64 * 1024;
const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    pub id: String,
    pub sent_at: i64,
    pub message: ProduceMessage,
    pub settings: ProducerSettings,
    pub result: ProduceResult,
    /// Значения усечены до 64 КБ — повторить такую запись нельзя.
    pub truncated: bool,
}

impl HistoryEntry {
    pub fn new(message: ProduceMessage, settings: ProducerSettings, result: ProduceResult) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            sent_at: result.timestamp,
            message,
            settings,
            result,
            truncated: false,
        }
    }
}

fn cut(s: &mut String) -> bool {
    if s.len() <= MAX_HISTORY_TEXT {
        return false;
    }
    let mut at = MAX_HISTORY_TEXT;
    while !s.is_char_boundary(at) {
        at -= 1;
    }
    s.truncate(at);
    true
}

pub fn truncate_for_history(m: &mut ProduceMessage) -> bool {
    let mut any = cut(&mut m.key.text);
    any |= cut(&mut m.value.text);
    for h in &mut m.headers {
        any |= cut(&mut h.value);
    }
    any
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryFile {
    schema_version: u32,
    entries: Vec<HistoryEntry>,
}

/// История отправленных сообщений: по файлу на подключение (`history/<id>.json`).
pub struct HistoryStore {
    dir: PathBuf,
    // Сериализует read-modify-write файлов истории.
    lock: Mutex<()>,
}

impl HistoryStore {
    pub fn open(data_dir: &Path) -> AppResult<Self> {
        let dir = data_dir.join("history");
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            lock: Mutex::new(()),
        })
    }

    fn path(&self, connection_id: &str) -> AppResult<PathBuf> {
        let safe = !connection_id.is_empty()
            && connection_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !safe {
            return Err(AppError::invalid("Некорректный id подключения"));
        }
        Ok(self.dir.join(format!("{connection_id}.json")))
    }

    fn read(&self, path: &Path) -> AppResult<Vec<HistoryEntry>> {
        Ok(read_json::<HistoryFile>(path)?
            .map(|f| f.entries)
            .unwrap_or_default())
    }

    pub fn list(&self, connection_id: &str) -> AppResult<Vec<HistoryEntry>> {
        let path = self.path(connection_id)?;
        let _g = self.lock.lock_safe();
        self.read(&path)
    }

    pub fn append(&self, connection_id: &str, mut entry: HistoryEntry) -> AppResult<()> {
        let path = self.path(connection_id)?;
        entry.truncated = truncate_for_history(&mut entry.message);
        let _g = self.lock.lock_safe();
        let mut entries = self.read(&path)?;
        entries.insert(0, entry);
        entries.truncate(HISTORY_LIMIT);
        write_json(
            &path,
            &HistoryFile {
                schema_version: SCHEMA_VERSION,
                entries,
            },
        )
    }

    pub fn delete_connection(&self, connection_id: &str) -> AppResult<()> {
        let path = self.path(connection_id)?;
        let _g = self.lock.lock_safe();
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
```

- [ ] **Step 5: Реализация шаблонов** — над тестами в `templates.rs`:

```rust
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::kafka::producer::ProducerSettings;
use crate::produce::payload::ProduceMessage;
use crate::storage::fs::{read_json, write_json};
use crate::sync::RwLockExt;

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Template {
    #[serde(default)]
    pub id: String,
    pub name: String,
    /// None — глобальный шаблон (виден во всех подключениях).
    #[serde(default)]
    pub connection_id: Option<String>,
    pub message: ProduceMessage,
    #[serde(default)]
    pub settings: ProducerSettings,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TemplatesFile {
    schema_version: u32,
    templates: Vec<Template>,
}

pub struct TemplateStore {
    path: PathBuf,
    cache: RwLock<Vec<Template>>,
}

impl TemplateStore {
    pub fn open(data_dir: &Path) -> AppResult<Self> {
        let path = data_dir.join("templates.json");
        let templates = match read_json::<TemplatesFile>(&path)? {
            Some(f) if f.schema_version > SCHEMA_VERSION => {
                return Err(AppError::internal(format!(
                    "templates.json создан более новой версией приложения (schemaVersion {})",
                    f.schema_version
                )));
            }
            Some(f) => f.templates,
            None => Vec::new(),
        };
        Ok(Self {
            path,
            cache: RwLock::new(templates),
        })
    }

    pub fn list(&self, connection_id: &str) -> Vec<Template> {
        let mut out: Vec<Template> = self
            .cache
            .read_safe()
            .iter()
            .filter(|t| t.connection_id.as_deref().is_none_or(|c| c == connection_id))
            .cloned()
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn persist(&self, list: &[Template]) -> AppResult<()> {
        write_json(
            &self.path,
            &TemplatesFile {
                schema_version: SCHEMA_VERSION,
                templates: list.to_vec(),
            },
        )
    }

    pub fn save(&self, mut t: Template) -> AppResult<Template> {
        t.name = t.name.trim().to_string();
        if t.name.is_empty() {
            return Err(AppError::invalid("Укажите имя шаблона"));
        }
        if t.id.is_empty() {
            t.id = uuid::Uuid::new_v4().to_string();
        }
        let mut cache = self.cache.write_safe();
        let mut next = cache.clone();
        match next.iter_mut().find(|x| x.id == t.id) {
            Some(slot) => *slot = t.clone(),
            None => next.push(t.clone()),
        }
        self.persist(&next)?;
        *cache = next;
        Ok(t)
    }

    pub fn delete(&self, id: &str) -> AppResult<()> {
        let mut cache = self.cache.write_safe();
        if !cache.iter().any(|t| t.id == id) {
            return Err(AppError::not_found("Шаблон не найден"));
        }
        let next: Vec<Template> = cache.iter().filter(|t| t.id != id).cloned().collect();
        self.persist(&next)?;
        *cache = next;
        Ok(())
    }

    pub fn delete_for_connection(&self, connection_id: &str) -> AppResult<()> {
        let mut cache = self.cache.write_safe();
        let next: Vec<Template> = cache
            .iter()
            .filter(|t| t.connection_id.as_deref() != Some(connection_id))
            .cloned()
            .collect();
        if next.len() != cache.len() {
            self.persist(&next)?;
            *cache = next;
        }
        Ok(())
    }
}
```

- [ ] **Step 6: `AppState`** — в `state.rs` добавить поля и открыть хранилища в `new`:

```rust
    pub history: Arc<crate::storage::history::HistoryStore>,
    pub templates: Arc<crate::storage::templates::TemplateStore>,
```
```rust
            history: Arc::new(crate::storage::history::HistoryStore::open(data_dir)?),
            templates: Arc::new(crate::storage::templates::TemplateStore::open(data_dir)?),
```

В `commands/connections.rs`, в `delete_connection` после удаления секрета:
```rust
        st.history.delete_connection(&id)?;
        st.templates.delete_for_connection(&id)?;
```

- [ ] **Step 7: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`
Expected: PASS (5 новых тестов `storage`).

- [ ] **Step 8: Commit**

```bash
git add src-tauri/src
git commit -m "feat(storage): история отправки и шаблоны сообщений"
```

---

### Task 8: Команды Producer

**Files:**
- Create: `src-tauri/src/commands/produce.rs`
- Modify: `src-tauri/src/commands/mod.rs` (`pub mod produce;`)
- Modify: `src-tauri/src/lib.rs` (регистрация)
- Modify: `src-tauri/tests/it_produce.rs`

**Interfaces:**
- Consumes: `AppState::{guarded, producers, history, templates, evict_if_client_broken}`, `render_message`, `system_env`, `encode_message`, `producer::send`, `blocking`
- Produces — Tauri-команды:
  - `produce(connectionId, message: ProduceMessage, settings: ProducerSettings, confirm) -> ProduceResult` (guard; плейсхолдеры с `seq = 0`; при успехе — запись в историю; ошибка записи истории не отменяет результат)
  - `list_history(connectionId) -> HistoryEntry[]`
  - `list_templates(connectionId) -> Template[]`, `save_template(template: Template) -> Template`, `delete_template(id) -> ()`
  - `produce_impl(st: &AppState, connection_id, message, settings, confirm) -> AppResult<ProduceResult>`

- [ ] **Step 1: Тесты** — создать `src-tauri/src/commands/produce.rs` с тестами, добавить `pub mod produce;` в `commands/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::ErrorCode;
    use crate::produce::payload::{PayloadFormat, PayloadInput};
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{Environment, sample_profile};

    fn state() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        for (id, env, ro) in [("ro", Environment::Dev, true), ("prod", Environment::Prod, false), ("dev", Environment::Dev, false)] {
            let mut p = sample_profile(id);
            p.environment = env;
            p.read_only = ro;
            p.bootstrap_servers = "127.0.0.1:1".into();
            st.connections.upsert(p).unwrap();
        }
        (dir, st)
    }

    fn message(value: &str, format: PayloadFormat) -> ProduceMessage {
        ProduceMessage {
            topic: "t".into(),
            partition: None,
            key: PayloadInput { format: PayloadFormat::Null, text: String::new() },
            value: PayloadInput { format, text: value.into() },
            headers: vec![],
        }
    }

    #[tokio::test]
    async fn produce_is_guarded() {
        let (_d, st) = state();
        let ok = || message("v", PayloadFormat::String);
        let e = produce_impl(&st, "ro".into(), ok(), ProducerSettings::default(), Some("conn-ro".into())).await;
        assert_eq!(e.unwrap_err().code, ErrorCode::ReadOnly);
        let e = produce_impl(&st, "prod".into(), ok(), ProducerSettings::default(), None).await;
        assert_eq!(e.unwrap_err().code, ErrorCode::ConfirmationRequired);
    }

    #[tokio::test]
    async fn invalid_message_is_rejected_before_network() {
        let (_d, st) = state();
        for m in [message("{{nope}}", PayloadFormat::String), message("{oops", PayloadFormat::Json)] {
            let e = produce_impl(&st, "dev".into(), m, ProducerSettings::default(), None).await;
            assert_eq!(e.unwrap_err().code, ErrorCode::InvalidInput);
        }
        assert!(st.history.list("dev").unwrap().is_empty());
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib commands::produce`
Expected: FAIL — нет `produce_impl`.

- [ ] **Step 3: Реализация** — над тестами:

```rust
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::producer::{self, ProduceResult, ProducerSettings};
use crate::produce::payload::{ProduceMessage, encode_message};
use crate::produce::template::{RenderContext, render_message, system_env};
use crate::state::AppState;
use crate::storage::history::HistoryEntry;
use crate::storage::templates::Template;

#[tauri::command]
pub async fn produce(
    state: State<'_, AppState>,
    connection_id: String,
    message: ProduceMessage,
    settings: ProducerSettings,
    confirm: Option<String>,
) -> AppResult<ProduceResult> {
    produce_impl(state.inner(), connection_id, message, settings, confirm).await
}

pub async fn produce_impl(
    st: &AppState,
    connection_id: String,
    message: ProduceMessage,
    settings: ProducerSettings,
    confirm: Option<String>,
) -> AppResult<ProduceResult> {
    let profile = st.guarded(&connection_id, confirm.as_deref())?;
    settings.validate()?;
    let mut rng = fastrand::Rng::new();
    let rendered = render_message(
        &message,
        &mut RenderContext {
            seq: 0,
            now: chrono::Utc::now(),
            rng: &mut rng,
            env: &system_env,
        },
    )?;
    let encoded = encode_message(&rendered)?;

    let st2 = st.clone();
    let h = blocking(move || st2.producers.get(&profile, st2.secrets.as_ref(), &settings)).await?;
    let result = producer::send(&h, &encoded).await;
    // Освобождение producer'а и вытеснение при AUTH/TLS — на blocking-потоке.
    let (st3, id, err) = (st.clone(), connection_id.clone(), result.as_ref().err().cloned());
    let _ = blocking(move || {
        if let Some(e) = &err {
            st3.evict_if_client_broken(&id, e);
        }
        drop(h);
        Ok(())
    })
    .await;
    let result = result?;

    let (st4, r) = (st.clone(), result.clone());
    let saved = blocking(move || st4.history.append(&connection_id, HistoryEntry::new(rendered, settings, r))).await;
    if let Err(e) = saved {
        // Сообщение уже в Kafka: ошибка истории не должна выглядеть как ошибка отправки.
        tracing::warn!("не удалось записать историю отправки: {e}");
    }
    Ok(result)
}

#[tauri::command]
pub async fn list_history(state: State<'_, AppState>, connection_id: String) -> AppResult<Vec<HistoryEntry>> {
    let st = state.inner().clone();
    blocking(move || st.history.list(&connection_id)).await
}

#[tauri::command]
pub async fn list_templates(state: State<'_, AppState>, connection_id: String) -> AppResult<Vec<Template>> {
    let st = state.inner().clone();
    blocking(move || Ok(st.templates.list(&connection_id))).await
}

#[tauri::command]
pub async fn save_template(state: State<'_, AppState>, template: Template) -> AppResult<Template> {
    let st = state.inner().clone();
    blocking(move || st.templates.save(template)).await
}

#[tauri::command]
pub async fn delete_template(state: State<'_, AppState>, id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || st.templates.delete(&id)).await
}
```

Регистрация в `lib.rs` (после команд групп):
```rust
            commands::produce::produce,
            commands::produce::list_history,
            commands::produce::list_templates,
            commands::produce::save_template,
            commands::produce::delete_template,
```

- [ ] **Step 4: IT команды с историей** — в `tests/it_produce.rs`:

```rust
use std::sync::Arc;

use kafka_explorer_lib::commands::produce::produce_impl;
use kafka_explorer_lib::produce::payload::{PayloadFormat, PayloadInput, ProduceMessage};
use kafka_explorer_lib::state::AppState;

#[tokio::test(flavor = "multi_thread")]
async fn produce_command_renders_placeholders_and_records_history() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-produce-cmd", 1).await;
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
    st.connections.upsert(common::plaintext_profile(&bs)).unwrap();
    let m = ProduceMessage {
        topic: topic.to_string(),
        partition: None,
        key: PayloadInput { format: PayloadFormat::String, text: "key-{{seq}}".into() },
        value: PayloadInput { format: PayloadFormat::Json, text: r#"{"id":"{{uuid}}"}"#.into() },
        headers: vec![],
    };
    let r = produce_impl(&st, "it".into(), m, ProducerSettings::default(), None).await.unwrap();
    assert_eq!(r.offset, 0);
    let history = st.history.list("it").unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].message.key.text, "key-0");
    assert!(!history[0].message.value.text.contains("{{"), "в истории — уже подставленные значения");
}
```
`common::plaintext_profile` создаёт профиль с id `it`. `tempfile` уже в dev-dependencies.

- [ ] **Step 5: Тесты проходят**

Run:
```bash
cargo test --manifest-path src-tauri/Cargo.toml --lib commands::produce
KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_produce
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src-tauri/src src-tauri/tests/it_produce.rs
git commit -m "feat(commands): отправка сообщения, история и шаблоны"
```

---

### Task 9: Источники пакета и ограничитель скорости

**Files:**
- Modify: `src-tauri/Cargo.toml` (`csv = "1"`)
- Create: `src-tauri/src/produce/source.rs`, `src-tauri/src/produce/rate.rs`
- Modify: `src-tauri/src/produce/mod.rs` (`pub mod rate; pub mod source;`)

**Interfaces:**
- Consumes: `EncodedMessage`
- Produces:
  - `enum FileFormat { Jsonl, Csv }` (JSON `"jsonl"`, `"csv"`)
  - `struct SourceItem { pub line: u64, pub message: Result<EncodedMessage, String> }` — `line` — номер строки файла (1-based) или номер копии
  - `fn parse_jsonl_line(line: &str, topic: &str) -> Result<EncodedMessage, String>` — `{"key": …, "value": …, "headers": {имя: строка}, "partition": n}`; строковые key/value — как есть, объекты/массивы/числа — компактный JSON, `null`/нет поля — без значения
  - `fn read_jsonl<R: BufRead + Send + 'static>(reader: R, topic: String) -> impl Iterator<Item = SourceItem> + Send` — пустые строки пропускаются
  - `fn read_csv<R: Read + Send + 'static>(reader: R, topic: String) -> Result<impl Iterator<Item = SourceItem> + Send, String>` — обязательная колонка `value`; необязательные `key` (пусто → без ключа), `partition` (пусто → авто), `header.<имя>`
  - `fn open_file(path: &Path, format: FileFormat, topic: &str) -> AppResult<Box<dyn Iterator<Item = SourceItem> + Send>>`
  - `struct RateLimiter`: `fn new(per_sec: Option<u32>, now: Instant) -> Self`, `fn wait_time(&self, now: Instant) -> Duration`, `fn record(&mut self)`

- [ ] **Step 1: Тесты** — создать `src-tauri/src/produce/source.rs` с тестами:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn jsonl_line_variants() {
        let m = parse_jsonl_line(r#"{"key":"k1","value":{"n":1},"headers":{"trace":"t1"},"partition":2}"#, "t").unwrap();
        assert_eq!(m.topic, "t");
        assert_eq!(m.key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(m.value.as_deref(), Some(&br#"{"n":1}"#[..]));
        assert_eq!(m.headers, vec![("trace".to_string(), b"t1".to_vec())]);
        assert_eq!(m.partition, Some(2));
        let m = parse_jsonl_line(r#"{"value":"plain"}"#, "t").unwrap();
        assert_eq!((m.key, m.value.as_deref(), m.partition), (None, Some(&b"plain"[..]), None));
        let m = parse_jsonl_line(r#"{"key":null,"value":42}"#, "t").unwrap();
        assert_eq!(m.value.as_deref(), Some(&b"42"[..]));
        assert!(parse_jsonl_line("{oops", "t").unwrap_err().contains("JSON"));
        assert!(parse_jsonl_line(r#"{"value":"v","partition":-1}"#, "t").is_err());
        assert!(parse_jsonl_line(r#"[1,2]"#, "t").is_err());
    }

    #[test]
    fn jsonl_reader_numbers_lines_and_skips_blank() {
        let text = "{\"value\":\"a\"}\n\n{oops\n{\"value\":\"b\"}\n";
        let items: Vec<SourceItem> = read_jsonl(Cursor::new(text.to_string()), "t".into()).collect();
        assert_eq!(items.iter().map(|i| i.line).collect::<Vec<_>>(), [1, 3, 4]);
        assert!(items[0].message.is_ok() && items[1].message.is_err() && items[2].message.is_ok());
    }

    #[test]
    fn csv_reader_columns() {
        let text = "key,value,partition,header.trace\nk1,v1,0,t1\n,v2,,\n";
        let items: Vec<SourceItem> = read_csv(Cursor::new(text.to_string()), "t".into()).unwrap().collect();
        assert_eq!(items.len(), 2);
        let a = items[0].message.as_ref().unwrap();
        assert_eq!(items[0].line, 2);
        assert_eq!((a.key.as_deref(), a.value.as_deref(), a.partition), (Some(&b"k1"[..]), Some(&b"v1"[..]), Some(0)));
        assert_eq!(a.headers, vec![("trace".to_string(), b"t1".to_vec())]);
        let b = items[1].message.as_ref().unwrap();
        assert_eq!((b.key.as_deref(), b.partition), (None, None));
        assert!(b.headers.is_empty(), "пустое значение заголовка не добавляется");
    }

    #[test]
    fn csv_errors() {
        assert!(read_csv(Cursor::new("key\nk".to_string()), "t".into()).err().unwrap().contains("value"));
        let items: Vec<SourceItem> = read_csv(Cursor::new("value,partition\nv,abc\n".to_string()), "t".into()).unwrap().collect();
        assert!(items[0].message.as_ref().unwrap_err().contains("partition"));
    }
}
```

Создать `src-tauri/src/produce/rate.rs` с тестами и добавить модули в `produce/mod.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_never_waits() {
        let t = Instant::now();
        let mut r = RateLimiter::new(None, t);
        for _ in 0..1000 {
            r.record();
        }
        assert_eq!(r.wait_time(t), Duration::ZERO);
        assert_eq!(RateLimiter::new(Some(0), t).wait_time(t), Duration::ZERO);
    }

    #[test]
    fn spreads_messages_over_time() {
        let t = Instant::now();
        let mut r = RateLimiter::new(Some(10), t);
        assert_eq!(r.wait_time(t), Duration::ZERO, "первое — сразу");
        r.record();
        assert_eq!(r.wait_time(t), Duration::from_millis(100));
        assert_eq!(r.wait_time(t + Duration::from_millis(100)), Duration::ZERO);
        for _ in 0..9 {
            r.record();
        }
        assert_eq!(r.wait_time(t + Duration::from_millis(500)), Duration::from_millis(500));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib produce`
Expected: FAIL — нет `parse_jsonl_line`, `RateLimiter`.

- [ ] **Step 3: Зависимость** — в `src-tauri/Cargo.toml` в `[dependencies]`: `csv = "1"`.

- [ ] **Step 4: `rate.rs`** — над тестами:

```rust
use std::time::{Duration, Instant};

/// Равномерное ограничение скорости: k-е сообщение не раньше started + k / rate.
pub struct RateLimiter {
    per_sec: Option<u32>,
    started: Instant,
    sent: u64,
}

impl RateLimiter {
    pub fn new(per_sec: Option<u32>, now: Instant) -> Self {
        Self { per_sec, started: now, sent: 0 }
    }

    /// Сколько ждать перед следующим сообщением.
    pub fn wait_time(&self, now: Instant) -> Duration {
        match self.per_sec {
            None | Some(0) => Duration::ZERO,
            Some(rate) => {
                let due = self.started + Duration::from_secs_f64(self.sent as f64 / f64::from(rate));
                due.saturating_duration_since(now)
            }
        }
    }

    pub fn record(&mut self) {
        self.sent += 1;
    }
}
```

- [ ] **Step 5: `source.rs`** — над тестами:

```rust
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

use crate::error::{AppError, AppResult};
use crate::produce::payload::EncodedMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Jsonl,
    Csv,
}

#[derive(Debug)]
pub struct SourceItem {
    pub line: u64,
    pub message: Result<EncodedMessage, String>,
}

#[derive(Deserialize)]
struct JsonlRecord {
    #[serde(default)]
    key: Option<Value>,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    partition: Option<i32>,
}

fn json_bytes(v: Option<Value>) -> Option<Vec<u8>> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.into_bytes()),
        Some(other) => Some(other.to_string().into_bytes()),
    }
}

fn check_partition(p: Option<i32>) -> Result<Option<i32>, String> {
    match p {
        Some(n) if n < 0 => Err(format!("partition {n} — отрицательный номер")),
        other => Ok(other),
    }
}

pub fn parse_jsonl_line(line: &str, topic: &str) -> Result<EncodedMessage, String> {
    let r: JsonlRecord =
        serde_json::from_str(line).map_err(|e| format!("невалидный JSON: {e}"))?;
    Ok(EncodedMessage {
        topic: topic.to_string(),
        partition: check_partition(r.partition)?,
        key: json_bytes(r.key),
        value: json_bytes(r.value),
        headers: r.headers.into_iter().map(|(k, v)| (k, v.into_bytes())).collect(),
    })
}

pub fn read_jsonl<R: BufRead + Send + 'static>(
    reader: R,
    topic: String,
) -> impl Iterator<Item = SourceItem> + Send {
    reader.lines().enumerate().filter_map(move |(i, line)| {
        let line_no = i as u64 + 1;
        match line {
            Ok(l) if l.trim().is_empty() => None,
            Ok(l) => Some(SourceItem {
                line: line_no,
                message: parse_jsonl_line(&l, &topic),
            }),
            Err(e) => Some(SourceItem {
                line: line_no,
                message: Err(format!("ошибка чтения: {e}")),
            }),
        }
    })
}

pub fn read_csv<R: Read + Send + 'static>(
    reader: R,
    topic: String,
) -> Result<impl Iterator<Item = SourceItem> + Send, String> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(reader);
    let headers = rdr.headers().map_err(|e| format!("CSV: {e}"))?.clone();
    let col = |name: &str| headers.iter().position(|h| h.trim() == name);
    let value_col = col("value").ok_or_else(|| "CSV: нет обязательной колонки value".to_string())?;
    let key_col = col("key");
    let partition_col = col("partition");
    let header_cols: Vec<(usize, String)> = headers
        .iter()
        .enumerate()
        .filter_map(|(i, h)| h.trim().strip_prefix("header.").map(|n| (i, n.to_string())))
        .collect();
    Ok(rdr.into_records().map(move |rec| {
        let rec = match rec {
            Ok(r) => r,
            Err(e) => {
                let line = e.position().map_or(0, |p| p.line());
                return SourceItem { line, message: Err(format!("CSV: {e}")) };
            }
        };
        let line = rec.position().map_or(0, |p| p.line());
        let field = |i: usize| rec.get(i).unwrap_or("");
        let message = (|| {
            let partition = match partition_col.map(field).map(str::trim) {
                None | Some("") => None,
                Some(s) => Some(s.parse::<i32>().map_err(|_| format!("partition «{s}» — не число"))?),
            };
            Ok(EncodedMessage {
                topic: topic.clone(),
                partition: check_partition(partition)?,
                key: key_col.map(field).filter(|k| !k.is_empty()).map(|k| k.as_bytes().to_vec()),
                value: Some(field(value_col).as_bytes().to_vec()),
                headers: header_cols
                    .iter()
                    .filter(|(i, _)| !field(*i).is_empty())
                    .map(|(i, n)| (n.clone(), field(*i).as_bytes().to_vec()))
                    .collect(),
            })
        })();
        SourceItem { line, message }
    }))
}

pub fn open_file(
    path: &Path,
    format: FileFormat,
    topic: &str,
) -> AppResult<Box<dyn Iterator<Item = SourceItem> + Send>> {
    let file = File::open(path)
        .map_err(|e| AppError::invalid(format!("Не удалось открыть {}: {e}", path.display())))?;
    Ok(match format {
        FileFormat::Jsonl => Box::new(read_jsonl(BufReader::new(file), topic.to_string())),
        FileFormat::Csv => Box::new(
            read_csv(BufReader::new(file), topic.to_string()).map_err(AppError::invalid)?,
        ),
    })
}
```

- [ ] **Step 6: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib produce`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src-tauri/Cargo.toml src-tauri/Cargo.lock src-tauri/src/produce
git commit -m "feat(produce): чтение пакета из JSON Lines и CSV, ограничитель скорости"
```

---

### Task 10: Пакетная отправка — сессия и команда

**Files:**
- Create: `src-tauri/src/produce/batch.rs`
- Modify: `src-tauri/src/produce/mod.rs` (`pub mod batch;`)
- Modify: `src-tauri/src/commands/mod.rs` (перенести сюда `RemoveOnDrop` из `commands/browse.rs`, `pub(crate)`)
- Modify: `src-tauri/src/commands/browse.rs` (использовать `super::RemoveOnDrop`)
- Modify: `src-tauri/src/commands/produce.rs` (`produce_batch`)
- Modify: `src-tauri/src/lib.rs` (регистрация)
- Create: `src-tauri/tests/it_batch.rs`

**Interfaces:**
- Consumes: `ProduceMessage`, `render_message`, `encode_message`, `open_file`, `SourceItem`, `RateLimiter`, `ProducerSettings`, `owned_headers` (Task 6), `build_client_config`, `CapturingContext`, `SessionRegistry`
- Produces:
  - `enum BatchSource { Copies { count: u64, message: ProduceMessage }, File { path: String, format: FileFormat, topic: String } }` (JSON `{"type":"copies",…}` / `{"type":"file",…}`)
  - `struct BatchParams { pub connection_id: String, pub source: BatchSource, pub settings: ProducerSettings, pub rate_per_sec: Option<u32> }` (camelCase)
  - `const MAX_COPIES: u64 = 1_000_000`, `const MAX_REPORTED_ERRORS: usize = 20`
  - `struct BatchError { pub line: u64, pub message: String }`
  - `enum BatchEvent { Progress { sent, failed, total: Option<u64> }, Errors { errors: Vec<BatchError> }, Finished { sent, failed, cancelled: bool }, Error { error: AppError } }` (тег `type`, camelCase)
  - `trait BatchSink: Send + 'static { fn send(&self, e: BatchEvent) -> bool }`
  - `fn validate(p: &BatchParams) -> AppResult<()>`
  - `fn run_batch(profile: &ConnectionProfile, secrets: &dyn SecretStore, params: BatchParams, cancel: Arc<AtomicBool>, sink: &dyn BatchSink)`
  - команда `produce_batch(params: BatchParams, onEvent: Channel<BatchEvent>, confirm) -> SessionId` (guard), остановка — `stop_session`

- [ ] **Step 1: Unit-тесты** — создать `src-tauri/src/produce/batch.rs` с тестами, `pub mod batch;` в `produce/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::produce::payload::{PayloadFormat, PayloadInput};

    fn copies(count: u64) -> BatchParams {
        BatchParams {
            connection_id: "c".into(),
            source: BatchSource::Copies {
                count,
                message: ProduceMessage {
                    topic: "t".into(),
                    partition: None,
                    key: PayloadInput { format: PayloadFormat::Null, text: String::new() },
                    value: PayloadInput { format: PayloadFormat::String, text: "{{seq}}".into() },
                    headers: vec![],
                },
            },
            settings: ProducerSettings::default(),
            rate_per_sec: None,
        }
    }

    #[test]
    fn validation() {
        assert!(validate(&copies(1)).is_ok());
        assert!(validate(&copies(MAX_COPIES)).is_ok());
        for bad in [copies(0), copies(MAX_COPIES + 1)] {
            assert_eq!(validate(&bad).unwrap_err().code, ErrorCode::InvalidInput);
        }
        let mut bad_template = copies(1);
        if let BatchSource::Copies { message, .. } = &mut bad_template.source {
            message.value.text = "{{nope}}".into();
        }
        assert_eq!(validate(&bad_template).unwrap_err().code, ErrorCode::InvalidInput);
        let file = BatchParams {
            source: BatchSource::File { path: " ".into(), format: FileFormat::Jsonl, topic: "t".into() },
            ..copies(1)
        };
        assert_eq!(validate(&file).unwrap_err().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn params_json() {
        let p: BatchParams = serde_json::from_value(serde_json::json!({
            "connectionId": "c",
            "source": {"type": "file", "path": "/tmp/x.jsonl", "format": "jsonl", "topic": "t"},
            "settings": {"acks": "1", "compression": "none", "idempotence": false},
            "ratePerSec": 100
        }))
        .unwrap();
        assert!(matches!(p.source, BatchSource::File { format: FileFormat::Jsonl, .. }));
        assert_eq!(p.rate_per_sec, Some(100));
        let v = serde_json::to_value(BatchEvent::Finished { sent: 3, failed: 1, cancelled: false }).unwrap();
        assert_eq!(v, serde_json::json!({"type": "finished", "sent": 3, "failed": 1, "cancelled": false}));
    }

    #[test]
    fn copies_render_seq_per_message() {
        let BatchSource::Copies { message, .. } = copies(3).source else { unreachable!() };
        let items: Vec<SourceItem> = copies_iter(3, message).collect();
        let values: Vec<Vec<u8>> = items.into_iter().map(|i| i.message.unwrap().value.unwrap()).collect();
        assert_eq!(values, [b"0".to_vec(), b"1".to_vec(), b"2".to_vec()]);
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib produce::batch`
Expected: FAIL — нет `BatchParams`.

- [ ] **Step 3: Реализация** — над тестами в `batch.rs`:

```rust
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::client::ClientContext;
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
use rdkafka::types::RDKafkaErrorCode;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, from_kafka};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::producer::{ProducerSettings, owned_headers};
use crate::produce::payload::{ProduceMessage, encode_message};
use crate::produce::rate::RateLimiter;
use crate::produce::source::{FileFormat, SourceItem, open_file};
use crate::produce::template::{RenderContext, render_message, system_env};
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;
use crate::sync::MutexExt;

pub const MAX_COPIES: u64 = 1_000_000;
pub const MAX_REPORTED_ERRORS: usize = 20;
const TICK: Duration = Duration::from_millis(100);
/// Сколько после отмены ждём подтверждений уже отправленных сообщений.
const CANCEL_DRAIN: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BatchSource {
    Copies { count: u64, message: ProduceMessage },
    File { path: String, format: FileFormat, topic: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchParams {
    pub connection_id: String,
    pub source: BatchSource,
    pub settings: ProducerSettings,
    #[serde(default)]
    pub rate_per_sec: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchError {
    pub line: u64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BatchEvent {
    Progress { sent: u64, failed: u64, total: Option<u64> },
    /// Первые MAX_REPORTED_ERRORS ошибок (список целиком, при каждом изменении).
    Errors { errors: Vec<BatchError> },
    Finished { sent: u64, failed: u64, cancelled: bool },
    Error { error: AppError },
}

pub trait BatchSink: Send + 'static {
    /// false — получатель закрыт.
    fn send(&self, event: BatchEvent) -> bool;
}

pub fn validate(p: &BatchParams) -> AppResult<()> {
    p.settings.validate()?;
    match &p.source {
        BatchSource::Copies { count, .. } if *count == 0 || *count > MAX_COPIES => Err(
            AppError::invalid(format!("Количество копий — от 1 до {MAX_COPIES}")),
        ),
        // Шаблон проверяется заранее на первой копии: иначе ошибка плейсхолдера повторилась бы N раз.
        BatchSource::Copies { message, .. } => {
            let mut rng = fastrand::Rng::new();
            let first = render_message(
                message,
                &mut RenderContext { seq: 0, now: chrono::Utc::now(), rng: &mut rng, env: &system_env },
            )?;
            encode_message(&first).map(|_| ())
        }
        BatchSource::File { path, topic, .. } if path.trim().is_empty() || topic.trim().is_empty() => {
            Err(AppError::invalid("Укажите файл и топик"))
        }
        BatchSource::File { .. } => Ok(()),
    }
}

/// N копий сообщения: плейсхолдеры рендерятся для каждой копии, `{{seq}}` = 0..N−1.
fn copies_iter(count: u64, message: ProduceMessage) -> impl Iterator<Item = SourceItem> + Send {
    let mut rng = fastrand::Rng::new();
    (0..count).map(move |seq| {
        let rendered = render_message(
            &message,
            &mut RenderContext { seq, now: chrono::Utc::now(), rng: &mut rng, env: &system_env },
        );
        SourceItem {
            line: seq + 1,
            message: rendered.and_then(|m| encode_message(&m)).map_err(|e| e.message),
        }
    })
}

type Delivery = (u64, Result<(), String>);

struct BatchContext {
    capture: CapturingContext,
    deliveries: Mutex<Sender<Delivery>>,
}

impl ClientContext for BatchContext {
    fn error(&self, error: KafkaError, reason: &str) {
        self.capture.error(error, reason);
    }
}

impl ProducerContext for BatchContext {
    type DeliveryOpaque = Box<u64>;

    fn delivery(&self, result: &DeliveryResult<'_>, line: Box<u64>) {
        let r = result.as_ref().map(|_| ()).map_err(|(e, _)| e.to_string());
        let _ = self.deliveries.lock_safe().send((*line, r));
    }
}

struct Stats<'a> {
    sink: &'a dyn BatchSink,
    total: Option<u64>,
    sent: u64,
    failed: u64,
    in_flight: u64,
    errors: Vec<BatchError>,
    errors_dirty: bool,
    last_progress: Instant,
    closed: bool,
}

impl Stats<'_> {
    fn fail(&mut self, line: u64, message: String) {
        self.failed += 1;
        if self.errors.len() < MAX_REPORTED_ERRORS {
            self.errors.push(BatchError { line, message });
            self.errors_dirty = true;
        }
    }

    fn drain(&mut self, rx: &Receiver<Delivery>) {
        while let Ok((line, r)) = rx.try_recv() {
            self.in_flight = self.in_flight.saturating_sub(1);
            match r {
                Ok(()) => self.sent += 1,
                Err(e) => self.fail(line, e),
            }
        }
    }

    fn emit(&mut self, e: BatchEvent) {
        if !self.sink.send(e) {
            self.closed = true;
        }
    }

    fn progress(&mut self, force: bool) {
        if !force && self.last_progress.elapsed() < TICK {
            return;
        }
        if self.errors_dirty {
            self.errors_dirty = false;
            let errors = self.errors.clone();
            self.emit(BatchEvent::Errors { errors });
        }
        let (sent, failed, total) = (self.sent, self.failed, self.total);
        self.emit(BatchEvent::Progress { sent, failed, total });
        self.last_progress = Instant::now();
    }
}

pub fn run_batch(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: BatchParams,
    cancel: Arc<AtomicBool>,
    sink: &dyn BatchSink,
) {
    if let Err(error) = run_inner(profile, secrets, params, &cancel, sink) {
        sink.send(BatchEvent::Error { error });
    }
}

fn run_inner(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: BatchParams,
    cancel: &AtomicBool,
    sink: &dyn BatchSink,
) -> AppResult<()> {
    validate(&params)?;
    let (items, total): (Box<dyn Iterator<Item = SourceItem> + Send>, Option<u64>) = match params.source {
        BatchSource::Copies { count, message } => (Box::new(copies_iter(count, message)), Some(count)),
        BatchSource::File { path, format, topic } => (open_file(Path::new(&path), format, &topic)?, None),
    };
    let mut cfg = build_client_config(profile, secrets)?;
    params.settings.apply(&mut cfg);
    cfg.set("message.timeout.ms", profile.request_timeout_ms.to_string());
    let (tx, rx) = mpsc::channel();
    let ctx = BatchContext { capture: CapturingContext::default(), deliveries: Mutex::new(tx) };
    let producer: BaseProducer<BatchContext> = cfg.create_with_context(ctx).map_err(|e| from_kafka(&e, None))?;

    let mut stats = Stats {
        sink,
        total,
        sent: 0,
        failed: 0,
        in_flight: 0,
        errors: Vec::new(),
        errors_dirty: false,
        last_progress: Instant::now(),
        closed: false,
    };
    let mut limiter = RateLimiter::new(params.rate_per_sec, Instant::now());
    let mut cancelled = false;
    let stop = |stats: &Stats| cancel.load(Ordering::Relaxed) || stats.closed;

    'items: for item in items {
        // Ограничение скорости: ждём короткими шагами, чтобы отмена срабатывала быстро.
        loop {
            if stop(&stats) {
                cancelled = true;
                break 'items;
            }
            let wait = limiter.wait_time(Instant::now());
            if wait.is_zero() {
                break;
            }
            producer.poll(wait.min(TICK));
            stats.drain(&rx);
            stats.progress(false);
        }
        limiter.record();
        let m = match item.message {
            Ok(m) => m,
            Err(e) => {
                stats.fail(item.line, e);
                continue;
            }
        };
        let mut record: BaseRecord<'_, [u8], [u8], Box<u64>> =
            BaseRecord::with_opaque_to(&m.topic, Box::new(item.line)).headers(owned_headers(&m.headers));
        if let Some(k) = &m.key {
            record = record.key(k.as_slice());
        }
        if let Some(v) = &m.value {
            record = record.payload(v.as_slice());
        }
        if let Some(p) = m.partition {
            record = record.partition(p);
        }
        loop {
            match producer.send(record) {
                Ok(()) => {
                    stats.in_flight += 1;
                    break;
                }
                // Очередь librdkafka заполнена: даём ей отправить накопленное.
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                    record = r;
                    producer.poll(TICK);
                    stats.drain(&rx);
                    stats.progress(false);
                    if stop(&stats) {
                        cancelled = true;
                        break 'items;
                    }
                }
                Err((e, _)) => {
                    stats.fail(item.line, e.to_string());
                    break;
                }
            }
        }
        producer.poll(Duration::ZERO);
        stats.drain(&rx);
        stats.progress(false);
    }

    // Ждём подтверждения уже отправленного: до таймаута запроса, после отмены — до CANCEL_DRAIN.
    let timeout = Duration::from_millis(profile.request_timeout_ms);
    let mut deadline = Instant::now() + if cancelled { CANCEL_DRAIN } else { timeout };
    while stats.in_flight > 0 && Instant::now() < deadline {
        producer.poll(TICK);
        stats.drain(&rx);
        stats.progress(false);
        if !cancelled && stop(&stats) {
            cancelled = true;
            deadline = deadline.min(Instant::now() + CANCEL_DRAIN);
        }
    }
    if stats.in_flight > 0 {
        // Неподтверждённые сообщения считаются неудачными: доставлены ли они — неизвестно.
        let lost = stats.in_flight;
        stats.in_flight = 0;
        stats.failed += lost;
        if stats.errors.len() < MAX_REPORTED_ERRORS {
            stats.errors.push(BatchError {
                line: 0,
                message: format!("{lost} сообщений не подтверждены брокером до таймаута"),
            });
            stats.errors_dirty = true;
        }
    }
    stats.progress(true);
    let (sent, failed) = (stats.sent, stats.failed);
    stats.emit(BatchEvent::Finished { sent, failed, cancelled });
    Ok(())
}
```

- [ ] **Step 4: Unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib produce::batch`
Expected: PASS, 3 теста.

- [ ] **Step 5: Команда** — в `src-tauri/src/commands/mod.rs` перенести из `commands/browse.rs` структуру `RemoveOnDrop` и её `impl Drop` (сделать `pub(crate) struct RemoveOnDrop { pub(crate) sessions: Arc<SessionRegistry>, pub(crate) id: String }`; импорт `use crate::browse::registry::SessionRegistry;`). В `commands/browse.rs` удалить её определение и импортировать `use super::RemoveOnDrop;`.

В `commands/produce.rs` добавить:
```rust
use tauri::ipc::Channel;

use super::RemoveOnDrop;
use crate::error::AppError;
use crate::produce::batch::{self, BatchEvent, BatchParams, BatchSink};

impl BatchSink for Channel<BatchEvent> {
    fn send(&self, event: BatchEvent) -> bool {
        Channel::<BatchEvent>::send(self, event).is_ok()
    }
}

#[tauri::command]
pub fn produce_batch(
    state: State<'_, AppState>,
    params: BatchParams,
    on_event: Channel<BatchEvent>,
    confirm: Option<String>,
) -> AppResult<String> {
    let profile = state.guarded(&params.connection_id, confirm.as_deref())?;
    batch::validate(&params)?;
    let secrets = state.secrets.clone();
    let (id, cancel) = state.sessions.register(&params.connection_id);
    let guard = RemoveOnDrop { sessions: state.sessions.clone(), id: id.clone() };
    std::thread::Builder::new()
        .name(format!("batch-{id}"))
        .spawn(move || {
            let _guard = guard;
            batch::run_batch(&profile, secrets.as_ref(), params, cancel, &on_event);
        })
        .map_err(|e| AppError::internal(format!("Не удалось запустить поток отправки: {e}")))?;
    Ok(id)
}
```
и тест в `mod tests` файла `commands/produce.rs` (проверка защиты без Tauri — через `AppState::guarded` + `batch::validate`, как делает команда):
```rust
    #[test]
    fn batch_requires_guard_and_valid_params() {
        let (_d, st) = state();
        assert_eq!(st.guarded("ro", Some("conn-ro")).unwrap_err().code, ErrorCode::ReadOnly);
        let p: crate::produce::batch::BatchParams = serde_json::from_value(serde_json::json!({
            "connectionId": "dev",
            "source": {"type": "copies", "count": 0, "message": {"topic": "t", "key": {"format": "null"}, "value": {"format": "string", "text": "v"}}},
            "settings": {"acks": "all", "compression": "none", "idempotence": false}
        }))
        .unwrap();
        assert_eq!(crate::produce::batch::validate(&p).unwrap_err().code, ErrorCode::InvalidInput);
    }
```

Регистрация в `lib.rs`: `commands::produce::produce_batch,`.

- [ ] **Step 6: Интеграционные тесты** — создать `src-tauri/tests/it_batch.rs`:

```rust
mod common;

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::kafka::producer::ProducerSettings;
use kafka_explorer_lib::kafka::topics::topic_details;
use kafka_explorer_lib::produce::batch::{BatchEvent, BatchParams, BatchSink, BatchSource, run_batch};
use kafka_explorer_lib::produce::payload::{PayloadFormat, PayloadInput, ProduceMessage};
use kafka_explorer_lib::produce::source::FileFormat;
use kafka_explorer_lib::secrets::MemoryStore;

#[derive(Clone, Default)]
struct VecSink(Arc<Mutex<Vec<BatchEvent>>>);

impl BatchSink for VecSink {
    fn send(&self, e: BatchEvent) -> bool {
        self.0.lock().unwrap().push(e);
        true
    }
}

impl VecSink {
    fn finished(&self) -> (u64, u64, bool) {
        match self.0.lock().unwrap().last().cloned() {
            Some(BatchEvent::Finished { sent, failed, cancelled }) => (sent, failed, cancelled),
            other => panic!("ожидали Finished, получили {other:?}"),
        }
    }

    fn errors(&self) -> Vec<(u64, String)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|e| match e {
                BatchEvent::Errors { errors } => Some(errors.iter().map(|x| (x.line, x.message.clone())).collect()),
                _ => None,
            })
            .unwrap_or_default()
    }
}

fn run(bs: &str, params: BatchParams, cancel: Arc<AtomicBool>) -> VecSink {
    let sink = VecSink::default();
    run_batch(&common::plaintext_profile(bs), &MemoryStore::default(), params, cancel, &sink);
    sink
}

fn copies(topic: &str, count: u64, rate: Option<u32>) -> BatchParams {
    BatchParams {
        connection_id: "it".into(),
        source: BatchSource::Copies {
            count,
            message: ProduceMessage {
                topic: topic.into(),
                partition: None,
                key: PayloadInput { format: PayloadFormat::String, text: "k{{seq}}".into() },
                value: PayloadInput { format: PayloadFormat::Json, text: r#"{"n":{{seq}}}"#.into() },
                headers: vec![],
            },
        },
        settings: ProducerSettings::default(),
        rate_per_sec: rate,
    }
}

async fn message_count(bs: &str, topic: &str) -> Option<i64> {
    let h = KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap();
    let t = topic.to_string();
    tokio::task::spawn_blocking(move || topic_details(&h, &t).unwrap().message_count).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn thousand_copies() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-batch", 3).await;
    let (bs2, t) = (bs.clone(), topic.to_string());
    let sink = tokio::task::spawn_blocking(move || run(&bs2, copies(&t, 1000, None), Arc::new(AtomicBool::new(false))))
        .await
        .unwrap();
    assert_eq!(sink.finished(), (1000, 0, false));
    assert_eq!(message_count(&bs, &topic).await, Some(1000));
}

#[tokio::test(flavor = "multi_thread")]
async fn jsonl_file_with_bad_line() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-batch-file", 1).await;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    writeln!(f, r#"{{"key":"a","value":{{"n":1}},"headers":{{"src":"file"}}}}"#).unwrap();
    writeln!(f, "{{oops").unwrap();
    writeln!(f, r#"{{"value":"plain","partition":0}}"#).unwrap();
    let params = BatchParams {
        connection_id: "it".into(),
        source: BatchSource::File { path: f.path().to_string_lossy().into(), format: FileFormat::Jsonl, topic: topic.to_string() },
        settings: ProducerSettings::default(),
        rate_per_sec: None,
    };
    let bs2 = bs.clone();
    let sink = tokio::task::spawn_blocking(move || run(&bs2, params, Arc::new(AtomicBool::new(false)))).await.unwrap();
    assert_eq!(sink.finished(), (2, 1, false));
    let errors = sink.errors();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, 2);
    assert_eq!(message_count(&bs, &topic).await, Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_stops_rate_limited_batch() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-batch-cancel", 1).await;
    let cancel = Arc::new(AtomicBool::new(false));
    let (bs2, t, c2) = (bs.clone(), topic.to_string(), cancel.clone());
    let task = tokio::task::spawn_blocking(move || run(&bs2, copies(&t, 100_000, Some(200)), c2));
    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.store(true, Ordering::Relaxed);
    let started = std::time::Instant::now();
    let sink = task.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(3), "отмена срабатывает быстро");
    let (sent, failed, cancelled) = sink.finished();
    assert!(cancelled);
    assert_eq!(failed, 0);
    assert!(sent > 0 && sent < 1000, "отправлено {sent}");
}
```

- [ ] **Step 7: Тесты проходят**

Run:
```bash
KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_batch --test it_browse
cargo test --manifest-path src-tauri/Cargo.toml --lib
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
```
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src-tauri/src src-tauri/tests/it_batch.rs
git commit -m "feat(produce): пакетная отправка с прогрессом, ограничением скорости и отменой"
```

---

### Task 11: Consumer-сессия — ядро

**Files:**
- Modify: `src-tauri/src/kafka/context.rs` (журнал ребалансов)
- Modify: `src-tauri/src/browse/types.rs` (поле `topic` у `BrowsedMessage`)
- Modify: `src-tauri/src/browse/session.rs` (`topic` в `to_browsed`; `pub(crate)` для `to_browsed`, `estimated_bytes`, `BATCH_SIZE`, `MAX_BATCH_BYTES`, `BATCH_INTERVAL`)
- Create: `src-tauri/src/consume/mod.rs`, `consume/types.rs`, `consume/control.rs`, `consume/session.rs`
- Modify: `src-tauri/src/lib.rs` (`pub mod consume;`)
- Modify: `src/api/types.ts` (`BrowsedMessage.topic`), `src/components/browse/MessageTable.tsx` (`messageKey` с топиком), тестовые фикстуры `BrowsedMessage` во фронтенде
- Create: `src-tauri/tests/it_consume.rs`

**Interfaces:**
- Consumes: `CapturingContext`, `build_client_config`, `CompiledFilter`, `FilterSpec`, `DataFormat`, `to_browsed`, `kafka::recovery::*`, `TopicPartitionRef`, `OffsetTarget`
- Produces:
  - `BrowsedMessage.topic: String` (Rust и TS); `messageKey(m) = "${topic}:${partition}:${offset}"`
  - `enum RebalanceRecord { Assigned { partitions: Vec<TopicPartitionRef>, at: i64 }, Revoked { partitions, at }, Error { message: String, at: i64 } }` (тег `kind`, camelCase); `CapturingContext::take_rebalances(&self) -> Vec<RebalanceRecord>`
  - `consume::types`: `enum OffsetReset { Earliest, Latest }`, `enum IsolationLevel { ReadCommitted, ReadUncommitted }` (JSON `"readCommitted"`), `struct ConsumerParams { connection_id, group_id, topics: Vec<String>, topic_pattern: Option<String>, auto_offset_reset, enable_auto_commit: bool, isolation_level, key_format: DataFormat, value_format: DataFormat, filter: FilterSpec }`, `fn validate(p: &ConsumerParams) -> AppResult<()>`, `fn subscription(p: &ConsumerParams) -> Vec<String>`, `fn default_group_id(user: Option<&str>, suffix: &str) -> String`, `fn current_user() -> Option<String>`, `enum CommitTarget { Selected { topic, partition, offset }, AllConsumed }` (тег `type`), `enum ConsumeEvent { Messages { messages }, Progress { consumed, matched }, Rebalance { record: RebalanceRecord }, Committed { offsets: Vec<OffsetTarget> }, Paused { partitions }, Resumed { partitions }, Reconnecting { message }, Recovered, Finished { consumed, matched }, Error { error } }` (тег `type`)
  - `consume::control`: `enum ConsumerCommand { Commit { target, reply }, Pause { partitions, reply }, Resume { partitions, reply } }`, `struct ConsumerControls` (`Default`): `register(&self, session_id: &str, connection_id: &str, tx: Sender<ConsumerCommand>)`, `remove(&self, session_id: &str)`, `get(&self, session_id: &str) -> AppResult<(String, Sender<ConsumerCommand>)>`; `fn request<T>(tx: &Sender<ConsumerCommand>, timeout: Duration, make: impl FnOnce(Sender<AppResult<T>>) -> ConsumerCommand) -> AppResult<T>`
  - `consume::session`: `trait ConsumeSink: Send + 'static { fn send(&self, e: ConsumeEvent) -> bool }`, `fn run_consumer(profile, secrets, params: ConsumerParams, cancel: Arc<AtomicBool>, commands: Receiver<ConsumerCommand>, sink: &dyn ConsumeSink)`

- [ ] **Step 1: Топик в `BrowsedMessage`** — в `browse/types.rs` добавить первым полем `pub topic: String,`; в `browse/session.rs` в `to_browsed` — `topic: m.topic().to_string(),`, в тестовой `big_message` — `topic: "t".into(),`. Сделать `pub(crate)`: `to_browsed`, `estimated_bytes`, константы `BATCH_SIZE`, `MAX_BATCH_BYTES`, `BATCH_INTERVAL`.

Во фронтенде: в `src/api/types.ts` в `BrowsedMessage` добавить первым полем `topic: string;`; в `src/components/browse/MessageTable.tsx`:
```ts
export const messageKey = (m: BrowsedMessage) => `${m.topic}:${m.partition}:${m.offset}`;
```
Во всех тестовых фикстурах `BrowsedMessage` (`grep -rln "size: 0" src --include="*.test.ts*"`: `browse/state.test.ts`, `browse/useBrowseSession.test.tsx`, `browse/MessageDetails.test.tsx`) добавить `topic: "t"`.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib && pnpm vitest run && pnpm build`
Expected: PASS.

- [ ] **Step 2: Тесты журнала ребалансов** — в `mod tests` файла `kafka/context.rs`:

```rust
    #[test]
    fn rebalance_log_is_taken_once() {
        let ctx = CapturingContext::default();
        assert!(ctx.take_rebalances().is_empty());
        ctx.record_rebalance(RebalanceRecord::Error { message: "x".into(), at: 1 });
        ctx.record_rebalance(RebalanceRecord::Assigned {
            partitions: vec![TopicPartitionRef { topic: "t".into(), partition: 0 }],
            at: 2,
        });
        let taken = ctx.take_rebalances();
        assert_eq!(taken.len(), 2);
        assert!(ctx.take_rebalances().is_empty());
        assert_eq!(
            serde_json::to_value(&taken[1]).unwrap(),
            serde_json::json!({"kind": "assigned", "partitions": [{"topic": "t", "partition": 0}], "at": 2})
        );
    }
```

- [ ] **Step 3: Тесты типов и контрола** — создать `consume/mod.rs` (`pub mod control; pub mod session; pub mod types;`), `consume/types.rs` и `consume/control.rs` с тестами, `pub mod consume;` в `lib.rs`.

`consume/types.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn params() -> ConsumerParams {
        serde_json::from_value(serde_json::json!({
            "connectionId": "c",
            "groupId": "g",
            "topics": ["a", "b"],
            "autoOffsetReset": "earliest",
            "enableAutoCommit": false,
            "isolationLevel": "readCommitted",
            "keyFormat": "auto",
            "valueFormat": "json"
        }))
        .unwrap()
    }

    #[test]
    fn validation_and_subscription() {
        let p = params();
        assert!(validate(&p).is_ok());
        assert_eq!(subscription(&p), ["a", "b"]);
        let mut re = params();
        re.topics.clear();
        re.topic_pattern = Some("orders\\..*".into());
        assert!(validate(&re).is_ok());
        assert_eq!(subscription(&re), ["^orders\\..*"]);
        re.topic_pattern = Some("^orders".into());
        assert_eq!(subscription(&re), ["^orders"], "^ не дублируется");

        let mut bad = vec![];
        let mut p1 = params();
        p1.group_id = " ".into();
        bad.push(p1);
        let mut p2 = params();
        p2.topics.clear();
        bad.push(p2);
        let mut p3 = params();
        p3.topic_pattern = Some("x".into());
        bad.push(p3); // и список, и regex
        let mut p4 = params();
        p4.topics.clear();
        p4.topic_pattern = Some("(".into());
        bad.push(p4);
        for p in bad {
            assert_eq!(validate(&p).unwrap_err().code, ErrorCode::InvalidInput);
        }
    }

    #[test]
    fn generated_group_id() {
        assert_eq!(default_group_id(Some("Анна Petrova"), "a1b2c3"), "kafka-explorer-Petrova-a1b2c3");
        assert_eq!(default_group_id(None, "a1b2c3"), "kafka-explorer-user-a1b2c3");
        assert_eq!(default_group_id(Some("qa.bot"), "ffffff"), "kafka-explorer-qabot-ffffff");
    }

    #[test]
    fn events_and_commit_target_json() {
        let t: CommitTarget = serde_json::from_value(serde_json::json!({"type": "selected", "topic": "t", "partition": 1, "offset": 9})).unwrap();
        assert_eq!(t, CommitTarget::Selected { topic: "t".into(), partition: 1, offset: 9 });
        let all: CommitTarget = serde_json::from_value(serde_json::json!({"type": "allConsumed"})).unwrap();
        assert_eq!(all, CommitTarget::AllConsumed);
        let v = serde_json::to_value(ConsumeEvent::Finished { consumed: 3, matched: 2 }).unwrap();
        assert_eq!(v, serde_json::json!({"type": "finished", "consumed": 3, "matched": 2}));
    }
}
```

`consume/control.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use std::sync::mpsc;

    #[test]
    fn request_roundtrip_timeout_and_closed_session() {
        let (tx, rx) = mpsc::channel::<ConsumerCommand>();
        let worker = std::thread::spawn(move || {
            if let Ok(ConsumerCommand::Pause { reply, .. }) = rx.recv() {
                reply.send(Ok(())).unwrap();
            }
            // Второй запрос не обслуживаем — ждёт таймаута; потом канал закрывается.
            let _second = rx.recv();
            std::thread::sleep(Duration::from_millis(300));
        });
        let ok = request(&tx, Duration::from_secs(2), |reply| ConsumerCommand::Pause { partitions: vec![], reply });
        assert!(ok.is_ok());
        let slow = request(&tx, Duration::from_millis(100), |reply| ConsumerCommand::Resume { partitions: vec![], reply });
        assert_eq!(slow.unwrap_err().code, ErrorCode::Timeout);
        worker.join().unwrap();
        let closed = request(&tx, Duration::from_millis(100), |reply| ConsumerCommand::Resume { partitions: vec![], reply });
        assert_eq!(closed.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn controls_registry() {
        let c = ConsumerControls::default();
        let (tx, _rx) = mpsc::channel();
        c.register("s1", "conn", tx);
        assert_eq!(c.get("s1").unwrap().0, "conn");
        c.remove("s1");
        assert_eq!(c.get("s1").unwrap_err().code, ErrorCode::NotFound);
    }
}
```

- [ ] **Step 4: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib consume kafka::context`
Expected: FAIL — нет `ConsumerParams`, `request`, `record_rebalance`.

- [ ] **Step 5: Журнал ребалансов** — в `kafka/context.rs`:

```rust
use serde::Serialize;

use crate::kafka::groups::TopicPartitionRef;
use rdkafka::consumer::{BaseConsumer, Rebalance};
use rdkafka::TopicPartitionList;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RebalanceRecord {
    Assigned { partitions: Vec<TopicPartitionRef>, at: i64 },
    Revoked { partitions: Vec<TopicPartitionRef>, at: i64 },
    Error { message: String, at: i64 },
}

fn refs(tpl: &TopicPartitionList) -> Vec<TopicPartitionRef> {
    let mut v: Vec<TopicPartitionRef> = tpl
        .elements()
        .iter()
        .map(|e| TopicPartitionRef { topic: e.topic().to_string(), partition: e.partition() })
        .collect();
    v.sort();
    v
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
```
В `struct CapturingContext` добавить поле `rebalances: Arc<Mutex<Vec<RebalanceRecord>>>,` (Default и Clone остаются derive) и методы в `impl CapturingContext`:
```rust
    /// Ребалансы с прошлого вызова (заполняется только у consumer'ов с subscribe).
    pub fn take_rebalances(&self) -> Vec<RebalanceRecord> {
        std::mem::take(&mut *self.rebalances.lock().unwrap_or_else(|p| p.into_inner()))
    }

    fn record_rebalance(&self, r: RebalanceRecord) {
        self.rebalances.lock().unwrap_or_else(|p| p.into_inner()).push(r);
    }
```
`impl ConsumerContext for CapturingContext {}` заменить на:
```rust
impl ConsumerContext for CapturingContext {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let at = now_ms();
        self.record_rebalance(match rebalance {
            Rebalance::Assign(tpl) => RebalanceRecord::Assigned { partitions: refs(tpl), at },
            Rebalance::Revoke(tpl) => RebalanceRecord::Revoked { partitions: refs(tpl), at },
            Rebalance::Error(e) => RebalanceRecord::Error { message: e.to_string(), at },
        });
    }
}
```

- [ ] **Step 6: Типы** — над тестами в `consume/types.rs`:

```rust
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::browse::filter::FilterSpec;
use crate::browse::format::DataFormat;
use crate::browse::types::BrowsedMessage;
use crate::error::{AppError, AppResult};
use crate::kafka::context::RebalanceRecord;
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OffsetReset {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IsolationLevel {
    ReadCommitted,
    ReadUncommitted,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerParams {
    pub connection_id: String,
    pub group_id: String,
    #[serde(default)]
    pub topics: Vec<String>,
    /// Regex-подписка (librdkafka: имя топика, начинающееся с «^»).
    #[serde(default)]
    pub topic_pattern: Option<String>,
    pub auto_offset_reset: OffsetReset,
    pub enable_auto_commit: bool,
    pub isolation_level: IsolationLevel,
    pub key_format: DataFormat,
    pub value_format: DataFormat,
    #[serde(default)]
    pub filter: FilterSpec,
}

pub fn validate(p: &ConsumerParams) -> AppResult<()> {
    if p.group_id.trim().is_empty() {
        return Err(AppError::invalid("Укажите group.id"));
    }
    let has_topics = p.topics.iter().any(|t| !t.trim().is_empty());
    let pattern = p.topic_pattern.as_deref().map(str::trim).filter(|s| !s.is_empty());
    match (has_topics, pattern) {
        (true, None) => Ok(()),
        (false, Some(re)) => Regex::new(re)
            .map(|_| ())
            .map_err(|e| AppError::invalid(format!("Некорректное регулярное выражение: {e}"))),
        (false, None) => Err(AppError::invalid("Укажите топики или регулярное выражение")),
        (true, Some(_)) => Err(AppError::invalid("Укажите либо список топиков, либо регулярное выражение")),
    }
}

pub fn subscription(p: &ConsumerParams) -> Vec<String> {
    match p.topic_pattern.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(re) if re.starts_with('^') => vec![re.to_string()],
        Some(re) => vec![format!("^{re}")],
        None => p.topics.iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect(),
    }
}

/// kafka-explorer-<user>-<suffix>: в имени пользователя — только латиница, цифры, «_» и «-».
pub fn default_group_id(user: Option<&str>, suffix: &str) -> String {
    let clean: String = user
        .unwrap_or("")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        .collect();
    let user = if clean.is_empty() { "user" } else { clean.as_str() };
    format!("kafka-explorer-{user}-{suffix}")
}

pub fn current_user() -> Option<String> {
    std::env::var("USER").or_else(|_| std::env::var("USERNAME")).ok()
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum CommitTarget {
    /// Закоммитить до выбранного сообщения включительно: offset + 1.
    Selected { topic: String, partition: i32, offset: i64 },
    /// Текущие позиции по всем назначенным партициям.
    AllConsumed,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ConsumeEvent {
    Messages { messages: Vec<BrowsedMessage> },
    Progress { consumed: u64, matched: u64 },
    Rebalance { record: RebalanceRecord },
    Committed { offsets: Vec<OffsetTarget> },
    Paused { partitions: Vec<TopicPartitionRef> },
    Resumed { partitions: Vec<TopicPartitionRef> },
    Reconnecting { message: String },
    Recovered,
    Finished { consumed: u64, matched: u64 },
    Error { error: AppError },
}
```
`default_group_id(Some("Анна Petrova"), …)` даёт `Petrova` (кириллица и пробел отбрасываются) — так и задумано.

- [ ] **Step 7: Контрол** — над тестами в `consume/control.rs`:

```rust
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::time::Duration;

use crate::consume::types::CommitTarget;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};
use crate::sync::MutexExt;

/// Команды в поток consumer-сессии (consumer не Sync — обращаться к нему можно только из его потока).
pub enum ConsumerCommand {
    Commit { target: CommitTarget, reply: Sender<AppResult<Vec<OffsetTarget>>> },
    Pause { partitions: Vec<TopicPartitionRef>, reply: Sender<AppResult<()>> },
    Resume { partitions: Vec<TopicPartitionRef>, reply: Sender<AppResult<()>> },
}

struct Control {
    connection_id: String,
    tx: Sender<ConsumerCommand>,
}

#[derive(Default)]
pub struct ConsumerControls {
    inner: Mutex<HashMap<String, Control>>,
}

fn session_gone() -> AppError {
    AppError::not_found("Consumer-сессия не найдена или уже завершена")
}

impl ConsumerControls {
    pub fn register(&self, session_id: &str, connection_id: &str, tx: Sender<ConsumerCommand>) {
        self.inner.lock_safe().insert(
            session_id.to_string(),
            Control { connection_id: connection_id.to_string(), tx },
        );
    }

    pub fn remove(&self, session_id: &str) {
        self.inner.lock_safe().remove(session_id);
    }

    pub fn get(&self, session_id: &str) -> AppResult<(String, Sender<ConsumerCommand>)> {
        self.inner
            .lock_safe()
            .get(session_id)
            .map(|c| (c.connection_id.clone(), c.tx.clone()))
            .ok_or_else(session_gone)
    }
}

/// Отправляет команду в поток сессии и ждёт ответа не дольше `timeout`. Блокирующая.
pub fn request<T>(
    tx: &Sender<ConsumerCommand>,
    timeout: Duration,
    make: impl FnOnce(Sender<AppResult<T>>) -> ConsumerCommand,
) -> AppResult<T> {
    let (reply, rx) = mpsc::channel();
    tx.send(make(reply)).map_err(|_| session_gone())?;
    match rx.recv_timeout(timeout) {
        Ok(r) => r,
        Err(RecvTimeoutError::Timeout) => Err(AppError::new(ErrorCode::Timeout, "Consumer-сессия не ответила вовремя")),
        Err(RecvTimeoutError::Disconnected) => Err(session_gone()),
    }
}
```

- [ ] **Step 8: Unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib consume kafka::context`
Expected: PASS.

- [ ] **Step 9: Интеграционный тест сессии** — создать `src-tauri/tests/it_consume.rs`:

```rust
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use common::TestMsg;
use kafka_explorer_lib::browse::filter::FilterSpec;
use kafka_explorer_lib::browse::format::DataFormat;
use kafka_explorer_lib::consume::control::{ConsumerCommand, request};
use kafka_explorer_lib::consume::session::{ConsumeSink, run_consumer};
use kafka_explorer_lib::consume::types::{CommitTarget, ConsumeEvent, ConsumerParams, IsolationLevel, OffsetReset};
use kafka_explorer_lib::kafka::context::RebalanceRecord;
use kafka_explorer_lib::kafka::group_ops::group_offsets;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;

#[derive(Clone, Default)]
struct VecSink(Arc<Mutex<Vec<ConsumeEvent>>>);

impl ConsumeSink for VecSink {
    fn send(&self, e: ConsumeEvent) -> bool {
        self.0.lock().unwrap().push(e);
        true
    }
}

impl VecSink {
    fn messages(&self) -> usize {
        self.0.lock().unwrap().iter().map(|e| match e {
            ConsumeEvent::Messages { messages } => messages.len(),
            _ => 0,
        }).sum()
    }

    /// Последнее назначение (число партиций), если оно было.
    fn last_assigned(&self) -> Option<usize> {
        self.0.lock().unwrap().iter().rev().find_map(|e| match e {
            ConsumeEvent::Rebalance { record: RebalanceRecord::Assigned { partitions, .. } } => Some(partitions.len()),
            _ => None,
        })
    }

    fn revoked(&self) -> bool {
        self.0.lock().unwrap().iter().any(|e| matches!(e, ConsumeEvent::Rebalance { record: RebalanceRecord::Revoked { .. } }))
    }

    fn finished(&self) -> bool {
        matches!(self.0.lock().unwrap().last(), Some(ConsumeEvent::Finished { .. }))
    }
}

struct Session {
    sink: VecSink,
    tx: Sender<ConsumerCommand>,
    cancel: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

fn start(bs: &str, group: &str, topic: &str) -> Session {
    let params = ConsumerParams {
        connection_id: "it".into(),
        group_id: group.into(),
        topics: vec![topic.into()],
        topic_pattern: None,
        auto_offset_reset: OffsetReset::Earliest,
        enable_auto_commit: false,
        isolation_level: IsolationLevel::ReadCommitted,
        key_format: DataFormat::Auto,
        value_format: DataFormat::Auto,
        filter: FilterSpec::default(),
    };
    let (sink, cancel) = (VecSink::default(), Arc::new(AtomicBool::new(false)));
    let (tx, rx) = mpsc::channel();
    let (s, c, profile) = (sink.clone(), cancel.clone(), common::plaintext_profile(bs));
    let handle = std::thread::spawn(move || run_consumer(&profile, &MemoryStore::default(), params, c, rx, &s));
    Session { sink, tx, cancel, handle }
}

fn wait_until(what: &str, f: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "не дождались: {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consume_commit_and_rebalance() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-consume", 2).await;
    let msgs: Vec<TestMsg> = (0..10).map(|i| TestMsg { partition: i % 2, key: None, value: "v", headers: &[] }).collect();
    common::produce(&bs, &topic, &msgs).await;
    let group = common::TempGroup::new(&bs, "it-consume");
    let (bs2, t, g) = (bs.clone(), topic.to_string(), group.to_string());

    tokio::task::spawn_blocking(move || {
        let a = start(&bs2, &g, &t);
        wait_until("A получила 10 сообщений", || a.sink.messages() == 10);
        assert_eq!(a.sink.last_assigned(), Some(2));

        let committed = request(&a.tx, Duration::from_secs(10), |reply| ConsumerCommand::Commit { target: CommitTarget::AllConsumed, reply }).unwrap();
        let mut offs: Vec<(i32, i64)> = committed.iter().map(|o| (o.partition, o.offset)).collect();
        offs.sort();
        assert_eq!(offs, [(0, 5), (1, 5)]);

        let selected = request(&a.tx, Duration::from_secs(10), |reply| ConsumerCommand::Commit {
            target: CommitTarget::Selected { topic: t.clone(), partition: 0, offset: 1 },
            reply,
        })
        .unwrap();
        assert_eq!(selected[0].offset, 2, "до выбранного включительно = offset + 1");
        let h = KafkaHandles::create(&common::plaintext_profile(&bs2), &MemoryStore::default()).unwrap();
        let rows = group_offsets(&h, &g).unwrap();
        assert_eq!(rows.iter().map(|r| r.committed).collect::<Vec<_>>(), [Some(2), Some(5)]);

        let b = start(&bs2, &g, &t);
        wait_until("ребаланс: A отдала партицию", || a.sink.revoked() && a.sink.last_assigned() == Some(1));
        wait_until("B получила партицию", || b.sink.last_assigned() == Some(1));

        for s in [&a, &b] {
            s.cancel.store(true, Ordering::Relaxed);
        }
        let started = Instant::now();
        for s in [a, b] {
            s.handle.join().unwrap();
            assert!(s.sink.finished());
        }
        assert!(started.elapsed() < Duration::from_secs(15), "остановка с выходом из группы не зависает");
    })
    .await
    .unwrap();
}
```

- [ ] **Step 10: Реализация сессии** — создать `consume/session.rs`:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::{Offset, TopicPartitionList};

use crate::browse::filter::CompiledFilter;
use crate::browse::session::{BATCH_INTERVAL, BATCH_SIZE, MAX_BATCH_BYTES, estimated_bytes, to_browsed};
use crate::browse::types::BrowsedMessage;
use crate::consume::control::ConsumerCommand;
use crate::consume::types::{CommitTarget, ConsumeEvent, ConsumerParams, IsolationLevel, OffsetReset, subscription, validate};
use crate::error::{AppError, AppResult, from_kafka};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::op_error;
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};
use crate::kafka::recovery::{PROBE_TIMEOUT, RecoveryMonitor, is_transient};
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;

const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

pub trait ConsumeSink: Send + 'static {
    /// false — получатель закрыт, сессию нужно завершить.
    fn send(&self, event: ConsumeEvent) -> bool;
}

struct Out<'a> {
    sink: &'a dyn ConsumeSink,
    batch: Vec<BrowsedMessage>,
    bytes: usize,
    consumed: u64,
    matched: u64,
    last_flush: Instant,
    last_progress: Instant,
    closed: bool,
}

impl Out<'_> {
    fn emit(&mut self, e: ConsumeEvent) {
        if !self.sink.send(e) {
            self.closed = true;
        }
    }

    fn push(&mut self, m: BrowsedMessage) {
        self.matched += 1;
        self.bytes += estimated_bytes(&m);
        self.batch.push(m);
    }

    fn flush(&mut self) {
        if !self.batch.is_empty() {
            let messages = std::mem::take(&mut self.batch);
            self.bytes = 0;
            self.emit(ConsumeEvent::Messages { messages });
        }
        self.last_flush = Instant::now();
        let (consumed, matched) = (self.consumed, self.matched);
        self.emit(ConsumeEvent::Progress { consumed, matched });
        self.last_progress = Instant::now();
    }

    fn maybe_flush(&mut self) {
        let due = self.batch.len() >= BATCH_SIZE
            || self.bytes > MAX_BATCH_BYTES
            || (!self.batch.is_empty() && self.last_flush.elapsed() >= BATCH_INTERVAL)
            || self.last_progress.elapsed() >= PROGRESS_INTERVAL;
        if due {
            self.flush();
        }
    }
}

pub fn run_consumer(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: ConsumerParams,
    cancel: Arc<AtomicBool>,
    commands: Receiver<ConsumerCommand>,
    sink: &dyn ConsumeSink,
) {
    if let Err(error) = run_inner(profile, secrets, &params, &cancel, &commands, sink) {
        sink.send(ConsumeEvent::Error { error });
    }
}

fn create(p: &ConnectionProfile, secrets: &dyn SecretStore, params: &ConsumerParams, ctx: &CapturingContext) -> AppResult<BaseConsumer<CapturingContext>> {
    let mut cfg = build_client_config(p, secrets)?;
    cfg.set("group.id", params.group_id.trim())
        .set("client.id", "kafka-explorer-session")
        .set("enable.auto.commit", if params.enable_auto_commit { "true" } else { "false" })
        .set("auto.offset.reset", match params.auto_offset_reset {
            OffsetReset::Earliest => "earliest",
            OffsetReset::Latest => "latest",
        })
        .set("isolation.level", match params.isolation_level {
            IsolationLevel::ReadCommitted => "read_committed",
            IsolationLevel::ReadUncommitted => "read_uncommitted",
        });
    cfg.create_with_context(ctx.clone()).map_err(|e| from_kafka(&e, None))
}

fn tpl_of(partitions: &[TopicPartitionRef]) -> AppResult<TopicPartitionList> {
    let mut tpl = TopicPartitionList::new();
    for p in partitions {
        tpl.add_partition(&p.topic, p.partition);
    }
    if tpl.count() == 0 {
        return Err(AppError::invalid("Не выбраны партиции"));
    }
    Ok(tpl)
}

/// Ошибки команд не завершают сессию; причину берём из последней ошибки клиента без опроса
/// консьюмера (опрос забрал бы сообщения из сессии).
fn command_error(ctx: &CapturingContext, e: KafkaError) -> AppError {
    from_kafka(&e, ctx.last_error().as_deref())
}

fn commit(consumer: &BaseConsumer<CapturingContext>, ctx: &CapturingContext, target: CommitTarget) -> AppResult<Vec<OffsetTarget>> {
    let offsets: Vec<OffsetTarget> = match target {
        CommitTarget::Selected { topic, partition, offset } => {
            vec![OffsetTarget { topic, partition, offset: offset + 1 }]
        }
        CommitTarget::AllConsumed => consumer
            .position()
            .map_err(|e| command_error(ctx, e))?
            .elements()
            .iter()
            .filter_map(|e| match e.offset() {
                Offset::Offset(o) if o >= 0 => Some(OffsetTarget { topic: e.topic().to_string(), partition: e.partition(), offset: o }),
                _ => None,
            })
            .collect(),
    };
    if offsets.is_empty() {
        return Err(AppError::invalid("Нет прочитанных сообщений для коммита"));
    }
    let mut tpl = TopicPartitionList::new();
    for o in &offsets {
        tpl.add_partition_offset(&o.topic, o.partition, Offset::Offset(o.offset))
            .map_err(|e| command_error(ctx, e))?;
    }
    consumer.commit(&tpl, CommitMode::Sync).map_err(|e| command_error(ctx, e))?;
    Ok(offsets)
}

fn handle(cmd: ConsumerCommand, consumer: &BaseConsumer<CapturingContext>, ctx: &CapturingContext, out: &mut Out) {
    match cmd {
        ConsumerCommand::Commit { target, reply } => {
            let r = commit(consumer, ctx, target);
            if let Ok(offsets) = &r {
                out.emit(ConsumeEvent::Committed { offsets: offsets.clone() });
            }
            let _ = reply.send(r);
        }
        ConsumerCommand::Pause { partitions, reply } => {
            let r = tpl_of(&partitions).and_then(|t| consumer.pause(&t).map_err(|e| command_error(ctx, e)));
            if r.is_ok() {
                out.emit(ConsumeEvent::Paused { partitions });
            }
            let _ = reply.send(r);
        }
        ConsumerCommand::Resume { partitions, reply } => {
            let r = tpl_of(&partitions).and_then(|t| consumer.resume(&t).map_err(|e| command_error(ctx, e)));
            if r.is_ok() {
                out.emit(ConsumeEvent::Resumed { partitions });
            }
            let _ = reply.send(r);
        }
    }
}

fn run_inner(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: &ConsumerParams,
    cancel: &AtomicBool,
    commands: &Receiver<ConsumerCommand>,
    sink: &dyn ConsumeSink,
) -> AppResult<()> {
    validate(params)?;
    let filter = CompiledFilter::compile(&params.filter)?;
    let ctx = CapturingContext::default();
    let consumer = create(profile, secrets, params, &ctx)?;
    let topics = subscription(params);
    let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&refs).map_err(|e| from_kafka(&e, None))?;

    let mut out = Out {
        sink,
        batch: Vec::with_capacity(BATCH_SIZE),
        bytes: 0,
        consumed: 0,
        matched: 0,
        last_flush: Instant::now(),
        last_progress: Instant::now(),
        closed: false,
    };
    let mut recovery = RecoveryMonitor::default();
    let result = loop {
        if cancel.load(Ordering::Relaxed) || out.closed {
            break Ok(());
        }
        let now = Instant::now();
        if let Some((since, e)) = recovery.expired(now) {
            let e = e.clone();
            out.flush();
            break Err(op_error(&consumer, since, e));
        }
        if recovery.should_probe(now) {
            let ok = consumer.fetch_metadata(None, PROBE_TIMEOUT).is_ok();
            if recovery.on_probe(Instant::now(), ok) {
                out.emit(ConsumeEvent::Recovered);
            }
        }
        let polled_at = Instant::now();
        match consumer.poll(POLL_TIMEOUT) {
            None => {}
            Some(Err(KafkaError::PartitionEOF(_))) => {}
            Some(Err(e)) if is_transient(&e) => {
                let message = from_kafka(&e, ctx.last_error_since(polled_at).as_deref()).message;
                if recovery.on_transient(Instant::now(), e) {
                    out.flush();
                    out.emit(ConsumeEvent::Reconnecting { message });
                }
            }
            Some(Err(e)) => {
                out.flush();
                break Err(op_error(&consumer, polled_at, e));
            }
            Some(Ok(msg)) => {
                if recovery.on_progress() {
                    out.emit(ConsumeEvent::Recovered);
                }
                out.consumed += 1;
                let m = to_browsed(&msg, params.key_format, params.value_format);
                if filter.matches(&m.view()) {
                    out.push(m);
                }
            }
        }
        for record in ctx.take_rebalances() {
            out.flush();
            out.emit(ConsumeEvent::Rebalance { record });
        }
        while let Ok(cmd) = commands.try_recv() {
            handle(cmd, &consumer, &ctx, &mut out);
        }
        out.maybe_flush();
    };
    out.flush();
    // Выход из группы: unsubscribe и закрытие консьюмера (Drop дожидается consumer_close).
    let _ = consumer.unsubscribe();
    drop(consumer);
    for record in ctx.take_rebalances() {
        out.emit(ConsumeEvent::Rebalance { record });
    }
    result?;
    let (consumed, matched) = (out.consumed, out.matched);
    out.emit(ConsumeEvent::Finished { consumed, matched });
    Ok(())
}
```

- [ ] **Step 11: Тесты проходят**

Run:
```bash
cargo test --manifest-path src-tauri/Cargo.toml --lib
KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_consume --test it_browse
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
```
Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add src-tauri/src src-tauri/tests/it_consume.rs src
git commit -m "feat(consume): consumer-сессия с журналом ребалансов, коммитом и pause/resume"
```

---

### Task 12: Команды consumer-сессий

**Files:**
- Create: `src-tauri/src/commands/consume.rs`
- Modify: `src-tauri/src/commands/mod.rs` (`pub mod consume;`)
- Modify: `src-tauri/src/state.rs` (поле `consumers: Arc<ConsumerControls>`)
- Modify: `src-tauri/src/lib.rs` (регистрация)

**Interfaces:**
- Consumes: `run_consumer`, `ConsumeSink`, `ConsumerControls`, `request`, `validate`, `default_group_id`, `current_user`, `AppState::{guarded, sessions, consumers}`
- Produces — Tauri-команды:
  - `start_consumer(params: ConsumerParams, onEvent: Channel<ConsumeEvent>, confirm) -> SessionId` (guard: read-only запрещает сессии целиком)
  - `commit_offsets(sessionId, target: CommitTarget, confirm) -> OffsetTarget[]` (guard по подключению сессии)
  - `pause_partitions(sessionId, partitions: TopicPartitionRef[]) -> ()`, `resume_partitions(sessionId, partitions) -> ()`
  - `suggest_group_id() -> string`
  - остановка — существующая `stop_session(sessionId)`
  - `start_consumer_impl<S: ConsumeSink>(st: &AppState, params, sink: S, confirm) -> AppResult<String>`, `commit_offsets_impl(st, session_id, target, confirm)`

- [ ] **Step 1: Тесты** — создать `commands/consume.rs` с тестами, `pub mod consume;` в `commands/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{Environment, sample_profile};

    struct NullSink;
    impl ConsumeSink for NullSink {
        fn send(&self, _: ConsumeEvent) -> bool {
            true
        }
    }

    fn state() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        for (id, env, ro) in [("ro", Environment::Dev, true), ("prod", Environment::Prod, false), ("dev", Environment::Dev, false)] {
            let mut p = sample_profile(id);
            p.environment = env;
            p.read_only = ro;
            p.bootstrap_servers = "127.0.0.1:1".into();
            st.connections.upsert(p).unwrap();
        }
        (dir, st)
    }

    fn params(conn: &str, group: &str) -> ConsumerParams {
        serde_json::from_value(serde_json::json!({
            "connectionId": conn, "groupId": group, "topics": ["t"],
            "autoOffsetReset": "earliest", "enableAutoCommit": false, "isolationLevel": "readCommitted",
            "keyFormat": "auto", "valueFormat": "auto"
        }))
        .unwrap()
    }

    #[test]
    fn consumer_sessions_are_guarded() {
        let (_d, st) = state();
        let e = start_consumer_impl(&st, params("ro", "g"), NullSink, Some("conn-ro".into())).unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
        let e = start_consumer_impl(&st, params("prod", "g"), NullSink, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::ConfirmationRequired);
        let e = start_consumer_impl(&st, params("dev", " "), NullSink, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(st.sessions.is_empty(), "отклонённый запуск не регистрирует сессию");
    }

    #[tokio::test]
    async fn commit_for_unknown_session_is_not_found() {
        let (_d, st) = state();
        let e = commit_offsets_impl(&st, "nope".into(), CommitTarget::AllConsumed, None).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
    }

    #[test]
    fn suggested_group_id_shape() {
        let id = suggest_group_id();
        assert!(id.starts_with("kafka-explorer-"), "{id}");
        assert_eq!(id.rsplit('-').next().unwrap().len(), 6);
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib commands::consume`
Expected: FAIL — нет `start_consumer_impl`.

- [ ] **Step 3: Реализация** — над тестами:

```rust
use std::sync::Arc;
use std::time::Duration;

use tauri::State;
use tauri::ipc::Channel;

use super::blocking;
use crate::browse::registry::SessionRegistry;
use crate::consume::control::{ConsumerCommand, ConsumerControls, request};
use crate::consume::session::{ConsumeSink, run_consumer};
use crate::consume::types::{CommitTarget, ConsumeEvent, ConsumerParams, current_user, default_group_id, validate};
use crate::error::{AppError, AppResult};
use crate::kafka::groups::{OffsetTarget, TopicPartitionRef};
use crate::state::AppState;

impl ConsumeSink for Channel<ConsumeEvent> {
    fn send(&self, event: ConsumeEvent) -> bool {
        Channel::<ConsumeEvent>::send(self, event).is_ok()
    }
}

/// Снимает сессию с учёта (реестр и канал команд) при любом выходе из потока.
struct ConsumerGuard {
    sessions: Arc<SessionRegistry>,
    controls: Arc<ConsumerControls>,
    id: String,
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        self.controls.remove(&self.id);
        self.sessions.remove(&self.id);
    }
}

#[tauri::command]
pub fn start_consumer(
    state: State<'_, AppState>,
    params: ConsumerParams,
    on_event: Channel<ConsumeEvent>,
    confirm: Option<String>,
) -> AppResult<String> {
    start_consumer_impl(state.inner(), params, on_event, confirm)
}

pub fn start_consumer_impl<S: ConsumeSink>(
    st: &AppState,
    params: ConsumerParams,
    sink: S,
    confirm: Option<String>,
) -> AppResult<String> {
    let profile = st.guarded(&params.connection_id, confirm.as_deref())?;
    validate(&params)?;
    let secrets = st.secrets.clone();
    let (id, cancel) = st.sessions.register(&params.connection_id);
    let (tx, rx) = std::sync::mpsc::channel();
    st.consumers.register(&id, &params.connection_id, tx);
    let guard = ConsumerGuard { sessions: st.sessions.clone(), controls: st.consumers.clone(), id: id.clone() };
    std::thread::Builder::new()
        .name(format!("consumer-{id}"))
        .spawn(move || {
            let _guard = guard;
            run_consumer(&profile, secrets.as_ref(), params, cancel, rx, &sink);
        })
        .map_err(|e| AppError::internal(format!("Не удалось запустить поток consumer-сессии: {e}")))?;
    Ok(id)
}

/// Таймаут ожидания ответа сессии: таймаут запроса подключения (коммит синхронный) + запас.
fn session_timeout(st: &AppState, connection_id: &str) -> AppResult<Duration> {
    Ok(Duration::from_millis(st.profile(connection_id)?.request_timeout_ms) + Duration::from_secs(2))
}

#[tauri::command]
pub async fn commit_offsets(
    state: State<'_, AppState>,
    session_id: String,
    target: CommitTarget,
    confirm: Option<String>,
) -> AppResult<Vec<OffsetTarget>> {
    commit_offsets_impl(state.inner(), session_id, target, confirm).await
}

pub async fn commit_offsets_impl(
    st: &AppState,
    session_id: String,
    target: CommitTarget,
    confirm: Option<String>,
) -> AppResult<Vec<OffsetTarget>> {
    let (connection_id, tx) = st.consumers.get(&session_id)?;
    st.guarded(&connection_id, confirm.as_deref())?;
    let timeout = session_timeout(st, &connection_id)?;
    blocking(move || request(&tx, timeout, |reply| ConsumerCommand::Commit { target, reply })).await
}

#[tauri::command]
pub async fn pause_partitions(
    state: State<'_, AppState>,
    session_id: String,
    partitions: Vec<TopicPartitionRef>,
) -> AppResult<()> {
    let (connection_id, tx) = state.consumers.get(&session_id)?;
    let timeout = session_timeout(state.inner(), &connection_id)?;
    blocking(move || request(&tx, timeout, |reply| ConsumerCommand::Pause { partitions, reply })).await
}

#[tauri::command]
pub async fn resume_partitions(
    state: State<'_, AppState>,
    session_id: String,
    partitions: Vec<TopicPartitionRef>,
) -> AppResult<()> {
    let (connection_id, tx) = state.consumers.get(&session_id)?;
    let timeout = session_timeout(state.inner(), &connection_id)?;
    blocking(move || request(&tx, timeout, |reply| ConsumerCommand::Resume { partitions, reply })).await
}

#[tauri::command]
pub fn suggest_group_id() -> String {
    let suffix: String = uuid::Uuid::new_v4().simple().to_string().chars().take(6).collect();
    default_group_id(current_user().as_deref(), &suffix)
}
```

`AppState` (`state.rs`): поле `pub consumers: Arc<crate::consume::control::ConsumerControls>,`, в `new` — `Arc::new(Default::default())`.

Регистрация в `lib.rs`:
```rust
            commands::consume::start_consumer,
            commands::consume::commit_offsets,
            commands::consume::pause_partitions,
            commands::consume::resume_partitions,
            commands::consume::suggest_group_id,
```

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src
git commit -m "feat(commands): запуск consumer-сессий, коммит, pause/resume"
```

---

### Task 13: API фронтенда для Producer и consumer-сессий

**Files:**
- Modify: `src/api/types.ts`, `src/api/client.ts`, `src/api/client.test.ts`

**Interfaces:**
- Consumes: Tauri-команды Tasks 8, 10, 12
- Produces:
  - TS-типы: `PayloadFormat`, `PayloadInput`, `HeaderInput`, `ProduceMessage`, `ProducerSettings`, `ProduceResult`, `HistoryEntry`, `Template`, `BatchSource`, `BatchParams`, `BatchError`, `BatchEvent`, `ConsumerParams`, `RebalanceRecord`, `CommitTarget`, `ConsumeEvent`
  - `api.produce(connectionId, message, settings, confirm)`, `api.listHistory(connectionId)`, `api.listTemplates(connectionId)`, `api.saveTemplate(template)`, `api.deleteTemplate(id)`, `api.startBatch(params, onEvent, confirm)`, `api.startConsumer(params, onEvent, confirm)`, `api.commitOffsets(sessionId, target, confirm)`, `api.pausePartitions(sessionId, partitions)`, `api.resumePartitions(sessionId, partitions)`, `api.suggestGroupId()`

- [ ] **Step 1: Тесты** — в `src/api/client.test.ts`:

```ts
  it("startConsumer передаёт параметры, Channel и подтверждение", async () => {
    invoke.mockResolvedValue("sid");
    const onEvent = vi.fn();
    const id = await api.startConsumer({ groupId: "g" } as ConsumerParams, onEvent, "prod-1");
    expect(id).toBe("sid");
    const [cmd, args] = invoke.mock.calls[0];
    expect(cmd).toBe("start_consumer");
    expect(args.confirm).toBe("prod-1");
    args.onEvent.onmessage({ type: "recovered" });
    expect(onEvent).toHaveBeenCalledWith({ type: "recovered" });
  });

  it("produce и commitOffsets", async () => {
    invoke.mockResolvedValue({ partition: 0, offset: 1, timestamp: 2 });
    const message = { topic: "t", partition: null, key: { format: "null", text: "" }, value: { format: "string", text: "v" }, headers: [] } as ProduceMessage;
    const settings = { acks: "all", compression: "none", idempotence: false } as const;
    await api.produce("c", message, settings, null);
    expect(invoke).toHaveBeenCalledWith("produce", { connectionId: "c", message, settings, confirm: null });
    await api.commitOffsets("s", { type: "allConsumed" }, null);
    expect(invoke).toHaveBeenLastCalledWith("commit_offsets", { sessionId: "s", target: { type: "allConsumed" }, confirm: null });
  });
```
и в импорт типов теста добавить `ConsumerParams, ProduceMessage`.

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/api/client.test.ts`
Expected: FAIL — `api.startConsumer is not a function`.

- [ ] **Step 3: Типы** — в конец `src/api/types.ts`:

```ts
export type PayloadFormat = "string" | "json" | "hex" | "base64" | "null";

export interface PayloadInput {
  format: PayloadFormat;
  text: string;
}

export interface HeaderInput {
  name: string;
  value: string;
  format: "string" | "hex";
}

export interface ProduceMessage {
  topic: string;
  /** null — партицию выбирает партиционер */
  partition: number | null;
  key: PayloadInput;
  value: PayloadInput;
  headers: HeaderInput[];
}

export interface ProducerSettings {
  acks: "0" | "1" | "all";
  compression: "none" | "gzip" | "snappy" | "lz4" | "zstd";
  idempotence: boolean;
}

export interface ProduceResult {
  partition: number;
  offset: number;
  timestamp: number;
}

export interface HistoryEntry {
  id: string;
  sentAt: number;
  message: ProduceMessage;
  settings: ProducerSettings;
  result: ProduceResult;
  /** Значения усечены до 64 КБ — повторить нельзя */
  truncated: boolean;
}

export interface Template {
  id: string;
  name: string;
  /** null — глобальный шаблон */
  connectionId: string | null;
  message: ProduceMessage;
  settings: ProducerSettings;
}

export type BatchSource =
  | { type: "copies"; count: number; message: ProduceMessage }
  | { type: "file"; path: string; format: "jsonl" | "csv"; topic: string };

export interface BatchParams {
  connectionId: string;
  source: BatchSource;
  settings: ProducerSettings;
  ratePerSec: number | null;
}

export interface BatchError {
  line: number;
  message: string;
}

export type BatchEvent =
  | { type: "progress"; sent: number; failed: number; total: number | null }
  | { type: "errors"; errors: BatchError[] }
  | { type: "finished"; sent: number; failed: number; cancelled: boolean }
  | { type: "error"; error: AppError };

export interface ConsumerParams {
  connectionId: string;
  groupId: string;
  topics: string[];
  topicPattern: string | null;
  autoOffsetReset: "earliest" | "latest";
  enableAutoCommit: boolean;
  isolationLevel: "readCommitted" | "readUncommitted";
  keyFormat: DataFormat;
  valueFormat: DataFormat;
  filter: FilterSpec;
}

export type RebalanceRecord =
  | { kind: "assigned"; partitions: TopicPartitionRef[]; at: number }
  | { kind: "revoked"; partitions: TopicPartitionRef[]; at: number }
  | { kind: "error"; message: string; at: number };

export type CommitTarget = { type: "selected"; topic: string; partition: number; offset: number } | { type: "allConsumed" };

export type ConsumeEvent =
  | { type: "messages"; messages: BrowsedMessage[] }
  | { type: "progress"; consumed: number; matched: number }
  | { type: "rebalance"; record: RebalanceRecord }
  | { type: "committed"; offsets: OffsetTarget[] }
  | { type: "paused"; partitions: TopicPartitionRef[] }
  | { type: "resumed"; partitions: TopicPartitionRef[] }
  | { type: "reconnecting"; message: string }
  | { type: "recovered" }
  | { type: "finished"; consumed: number; matched: number }
  | { type: "error"; error: AppError };
```

- [ ] **Step 4: Клиент** — в `src/api/client.ts` импортировать новые типы и добавить в `api`:

```ts
  produce: (connectionId: string, message: ProduceMessage, settings: ProducerSettings, confirm: string | null) =>
    call<ProduceResult>("produce", { connectionId, message, settings, confirm }),
  listHistory: (connectionId: string) => call<HistoryEntry[]>("list_history", { connectionId }),
  listTemplates: (connectionId: string) => call<Template[]>("list_templates", { connectionId }),
  saveTemplate: (template: Template) => call<Template>("save_template", { template }),
  deleteTemplate: (id: string) => call<void>("delete_template", { id }),
  startBatch: (params: BatchParams, onEvent: (e: BatchEvent) => void, confirm: string | null) => {
    const channel = new Channel<BatchEvent>();
    channel.onmessage = onEvent;
    return call<string>("produce_batch", { params, onEvent: channel, confirm });
  },

  startConsumer: (params: ConsumerParams, onEvent: (e: ConsumeEvent) => void, confirm: string | null) => {
    const channel = new Channel<ConsumeEvent>();
    channel.onmessage = onEvent;
    return call<string>("start_consumer", { params, onEvent: channel, confirm });
  },
  commitOffsets: (sessionId: string, target: CommitTarget, confirm: string | null) =>
    call<OffsetTarget[]>("commit_offsets", { sessionId, target, confirm }),
  pausePartitions: (sessionId: string, partitions: TopicPartitionRef[]) =>
    call<void>("pause_partitions", { sessionId, partitions }),
  resumePartitions: (sessionId: string, partitions: TopicPartitionRef[]) =>
    call<void>("resume_partitions", { sessionId, partitions }),
  suggestGroupId: () => call<string>("suggest_group_id"),
```

- [ ] **Step 5: Тесты проходят**

Run: `pnpm vitest run src/api && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api
git commit -m "feat(ui): API отправки сообщений и consumer-сессий"
```

---

### Task 14: Вкладка Producer — форма и одиночная отправка

**Files:**
- Create: `src/components/producer/form.ts`, `src/components/producer/form.test.ts`
- Create: `src/components/producer/HeadersEditor.tsx`
- Create: `src/components/producer/ProducerPanel.tsx`, `src/components/producer/ProducerPanel.test.tsx`
- Modify: `src/state/workspace.ts`, `src/components/TabContent.tsx`, `src/components/Layout.tsx`, `src/components/Sidebar.tsx`
- Modify: `src/components/topics/TopicDetails.tsx` (кнопка «Отправить сообщение»)
- Modify: `src/styles.css`

**Interfaces:**
- Consumes: `api.produce`, `api.listTopics`, `useConnection`, `ConfirmDialog`, `needsConfirmation`, `ErrorBanner`, `formatTimestamp`
- Produces:
  - `interface ProducerForm { topic: string; partition: string; key: PayloadInput; value: PayloadInput; headers: HeaderInput[]; settings: ProducerSettings }`
  - `DEFAULT_SETTINGS: ProducerSettings` (`all`, `none`, `false`), `emptyProducerForm(topic?: string): ProducerForm`
  - `formToMessage(f: ProducerForm): ProduceMessage` (бросает `Error` с текстом для пользователя), `messageToForm(m: ProduceMessage, settings?: ProducerSettings): ProducerForm`
  - `type ProducerPrefill = { message: ProduceMessage; settings?: ProducerSettings; nonce: number }` (в `state/workspace.ts`)
  - `TabSpec` += `{ kind: "producer"; connectionId: string; prefill?: ProducerPrefill }` (id `producer:<conn>`)
  - `<HeadersEditor headers onChange />`, `<ProducerPanel connectionId prefill? />` — форма отправки; в Task 15 и 16 сюда добавляются шаблоны, история и пакетная отправка (`ProducerPanel` экспортирует внутреннее состояние формы через пропсы дочерних компонентов)

- [ ] **Step 1: Тесты формы** — создать `src/components/producer/form.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { DEFAULT_SETTINGS, emptyProducerForm, formToMessage, messageToForm } from "./form";

describe("producer form", () => {
  it("пустая форма", () => {
    const f = emptyProducerForm("orders");
    expect(f.topic).toBe("orders");
    expect(f.key).toEqual({ format: "string", text: "" });
    expect(f.value).toEqual({ format: "json", text: "" });
    expect(f.settings).toEqual(DEFAULT_SETTINGS);
  });

  it("форма → сообщение", () => {
    const f = { ...emptyProducerForm(" orders "), partition: " 2 ", value: { format: "json" as const, text: '{"a":1}' } };
    f.headers = [{ name: "trace", value: "x", format: "string" }, { name: " ", value: "", format: "string" }];
    expect(formToMessage(f)).toEqual({
      topic: "orders",
      partition: 2,
      key: { format: "string", text: "" },
      value: { format: "json", text: '{"a":1}' },
      headers: [{ name: "trace", value: "x", format: "string" }],
    });
    expect(formToMessage({ ...f, partition: "" }).partition).toBeNull();
  });

  it("ошибки", () => {
    expect(() => formToMessage(emptyProducerForm(""))).toThrow("Укажите топик");
    expect(() => formToMessage({ ...emptyProducerForm("t"), partition: "-1" })).toThrow("Партиция — целое число ≥ 0");
    expect(() => formToMessage({ ...emptyProducerForm("t"), partition: "x" })).toThrow("Партиция — целое число ≥ 0");
  });

  it("сообщение → форма", () => {
    const m = formToMessage({ ...emptyProducerForm("t"), partition: "1" });
    const f = messageToForm(m, { acks: "1", compression: "gzip", idempotence: false });
    expect(f.partition).toBe("1");
    expect(f.settings.compression).toBe("gzip");
    expect(messageToForm({ ...m, partition: null }).partition).toBe("");
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/producer/form.test.ts`
Expected: FAIL — модуль не найден.

- [ ] **Step 3: `form.ts`**:

```ts
import type { HeaderInput, PayloadInput, ProduceMessage, ProducerSettings } from "../../api/types";

export interface ProducerForm {
  topic: string;
  /** "" — авто (партиционер) */
  partition: string;
  key: PayloadInput;
  value: PayloadInput;
  headers: HeaderInput[];
  settings: ProducerSettings;
}

export const DEFAULT_SETTINGS: ProducerSettings = { acks: "all", compression: "none", idempotence: false };

export function emptyProducerForm(topic = ""): ProducerForm {
  return {
    topic,
    partition: "",
    key: { format: "string", text: "" },
    value: { format: "json", text: "" },
    headers: [],
    settings: DEFAULT_SETTINGS,
  };
}

/** Проверки формы до отправки; содержимое форматов и плейсхолдеры проверяет Rust. */
export function formToMessage(f: ProducerForm): ProduceMessage {
  const topic = f.topic.trim();
  if (!topic) throw new Error("Укажите топик");
  let partition: number | null = null;
  const p = f.partition.trim();
  if (p) {
    if (!/^\d+$/.test(p)) throw new Error("Партиция — целое число ≥ 0");
    partition = Number(p);
  }
  return {
    topic,
    partition,
    key: f.key,
    value: f.value,
    headers: f.headers.filter((h) => h.name.trim()).map((h) => ({ ...h, name: h.name.trim() })),
  };
}

export function messageToForm(m: ProduceMessage, settings: ProducerSettings = DEFAULT_SETTINGS): ProducerForm {
  return {
    topic: m.topic,
    partition: m.partition === null ? "" : String(m.partition),
    key: m.key,
    value: m.value,
    headers: m.headers,
    settings,
  };
}
```

- [ ] **Step 4: Тест формы проходит**

Run: `pnpm vitest run src/components/producer/form.test.ts`
Expected: PASS.

- [ ] **Step 5: Тест панели** — создать `src/components/producer/ProducerPanel.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

const conns: ConnectionProfile[] = [
  { ...newProfile(), id: "dev", name: "dev-1", environment: "dev" },
  { ...newProfile(), id: "prod", name: "prod-1", environment: "prod" },
  { ...newProfile(), id: "ro", name: "ro-1", environment: "dev", readOnly: true },
];

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      listTopics: vi.fn().mockResolvedValue([{ name: "orders", internal: false, partitionCount: 1, replicationFactor: 1, underReplicated: 0 }]),
      produce: vi.fn().mockResolvedValue({ partition: 0, offset: 41, timestamp: 0 }),
      listTemplates: vi.fn().mockResolvedValue([]),
      listHistory: vi.fn().mockResolvedValue([]),
    },
  };
});

import { api } from "../../api/client";
import { ProducerPanel } from "./ProducerPanel";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listConnections).mockResolvedValue(conns);
});

describe("ProducerPanel", () => {
  it("отправляет сообщение и показывает результат", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProducerPanel connectionId="dev" />);
    await user.type(await screen.findByLabelText("Топик"), "orders");
    await user.type(screen.getByLabelText("Значение"), '{{"a":1}');
    await user.click(screen.getByRole("button", { name: "Добавить заголовок" }));
    await user.type(screen.getByLabelText("Имя заголовка 1"), "trace");
    await user.type(screen.getByLabelText("Значение заголовка 1"), "t1");
    await user.click(screen.getByRole("button", { name: "Отправить" }));
    expect(api.produce).toHaveBeenCalledWith(
      "dev",
      {
        topic: "orders",
        partition: null,
        key: { format: "string", text: "" },
        value: { format: "json", text: '{"a":1}' },
        headers: [{ name: "trace", value: "t1", format: "string" }],
      },
      { acks: "all", compression: "none", idempotence: false },
      null,
    );
    expect(await screen.findByText(/партиция 0, offset 41/)).toBeInTheDocument();
  });

  it("prod требует имя подключения", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProducerPanel connectionId="prod" />);
    await user.type(await screen.findByLabelText("Топик"), "orders");
    await user.click(screen.getByRole("button", { name: "Отправить" }));
    expect(api.produce).not.toHaveBeenCalled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Отправить в prod" }));
    expect(vi.mocked(api.produce).mock.calls[0][3]).toBe("prod-1");
  });

  it("read-only: отправка недоступна", async () => {
    renderWithClient(<ProducerPanel connectionId="ro" />);
    expect(await screen.findByText(/только для чтения/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Отправить" })).not.toBeInTheDocument();
  });
});
```

- [ ] **Step 6: Убедиться в падении**

Run: `pnpm vitest run src/components/producer`
Expected: FAIL — нет `ProducerPanel`.

- [ ] **Step 7: `HeadersEditor.tsx`**:

```tsx
import type { HeaderInput } from "../../api/types";

export function HeadersEditor({ headers, onChange }: { headers: HeaderInput[]; onChange: (h: HeaderInput[]) => void }) {
  const set = (i: number, patch: Partial<HeaderInput>) => onChange(headers.map((h, j) => (j === i ? { ...h, ...patch } : h)));
  return (
    <fieldset className="headers-editor">
      <legend>Заголовки</legend>
      {headers.map((h, i) => (
        <div key={i} className="toolbar">
          <input aria-label={`Имя заголовка ${i + 1}`} placeholder="имя" value={h.name} onChange={(e) => set(i, { name: e.target.value })} />
          <input aria-label={`Значение заголовка ${i + 1}`} placeholder="значение" value={h.value} onChange={(e) => set(i, { value: e.target.value })} />
          <select aria-label={`Формат заголовка ${i + 1}`} value={h.format} onChange={(e) => set(i, { format: e.target.value as HeaderInput["format"] })}>
            <option value="string">string</option>
            <option value="hex">hex</option>
          </select>
          <button type="button" className="icon" aria-label={`Удалить заголовок ${i + 1}`} onClick={() => onChange(headers.filter((_, j) => j !== i))}>
            ×
          </button>
        </div>
      ))}
      <button type="button" onClick={() => onChange([...headers, { name: "", value: "", format: "string" }])}>
        Добавить заголовок
      </button>
    </fieldset>
  );
}
```

- [ ] **Step 8: `ProducerPanel.tsx`**:

```tsx
import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { PayloadFormat, PayloadInput, ProduceMessage, ProducerSettings } from "../../api/types";
import { formatTimestamp } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import type { ProducerPrefill } from "../../state/workspace";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { emptyProducerForm, formToMessage, messageToForm, type ProducerForm } from "./form";
import { HeadersEditor } from "./HeadersEditor";

const FORMATS: PayloadFormat[] = ["string", "json", "hex", "base64", "null"];

function PayloadField({ label, value, onChange }: { label: string; value: PayloadInput; onChange: (v: PayloadInput) => void }) {
  return (
    <div className="payload-field">
      <div className="toolbar">
        <label htmlFor={`pf-${label}`}>{label}</label>
        <select aria-label={`Формат: ${label}`} value={value.format} onChange={(e) => onChange({ ...value, format: e.target.value as PayloadFormat })}>
          {FORMATS.map((f) => (
            <option key={f} value={f}>
              {f}
            </option>
          ))}
        </select>
      </div>
      <textarea
        id={`pf-${label}`}
        rows={label === "Значение" ? 8 : 2}
        disabled={value.format === "null"}
        value={value.text}
        onChange={(e) => onChange({ ...value, text: e.target.value })}
      />
    </div>
  );
}

export function ProducerPanel({ connectionId, prefill }: { connectionId: string; prefill?: ProducerPrefill }) {
  const connection = useConnection(connectionId);
  const queryClient = useQueryClient();
  const [form, setForm] = useState<ProducerForm>(() =>
    prefill ? messageToForm(prefill.message, prefill.settings) : emptyProducerForm(),
  );
  const [formError, setFormError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  // Новый prefill («Отправить повторно», «Отправить сообщение» из топика) заменяет форму.
  useEffect(() => {
    if (prefill) setForm(messageToForm(prefill.message, prefill.settings));
  }, [prefill]);
  const topics = useQuery({ queryKey: ["topics", connectionId], queryFn: () => api.listTopics(connectionId) });
  const send = useMutation({
    mutationFn: ({ message, confirm }: { message: ProduceMessage; confirm: string | null }) =>
      api.produce(connectionId, message, form.settings, confirm),
    onSuccess: () => {
      setConfirming(false);
      void queryClient.invalidateQueries({ queryKey: ["history", connectionId] });
    },
  });
  const set = (patch: Partial<ProducerForm>) => setForm((f) => ({ ...f, ...patch }));
  const setSettings = (patch: Partial<ProducerSettings>) => setForm((f) => ({ ...f, settings: { ...f.settings, ...patch } }));

  const submit = (confirm: string | null) => {
    try {
      const message = formToMessage(form);
      setFormError(null);
      send.mutate({ message, confirm });
    } catch (e) {
      setFormError((e as Error).message);
    }
  };

  if (!connection) return <p className="empty">Загрузка…</p>;
  const readOnly = connection.readOnly;

  return (
    <div className="page producer">
      <div className="toolbar">
        <h2>Producer</h2>
      </div>
      {readOnly && <p className="warn">Подключение открыто только для чтения — отправка недоступна.</p>}
      <div className="form">
        <label htmlFor="pr-topic">Топик</label>
        <input id="pr-topic" list={`topics-${connectionId}`} value={form.topic} onChange={(e) => set({ topic: e.target.value })} />
        <datalist id={`topics-${connectionId}`}>
          {topics.data?.filter((t) => !t.internal).map((t) => <option key={t.name} value={t.name} />)}
        </datalist>
        <label htmlFor="pr-partition">Партиция</label>
        <input id="pr-partition" placeholder="авто" value={form.partition} onChange={(e) => set({ partition: e.target.value })} />
      </div>
      <PayloadField label="Ключ" value={form.key} onChange={(key) => set({ key })} />
      <PayloadField label="Значение" value={form.value} onChange={(value) => set({ value })} />
      <HeadersEditor headers={form.headers} onChange={(headers) => set({ headers })} />
      <p className="muted">
        Плейсхолдеры: {"{{uuid}} {{now}} {{nowMillis}} {{seq}} {{randomInt 1 100}} {{randomString 8}} {{env NAME}}"}
      </p>
      <fieldset className="toolbar">
        <legend>Параметры продюсера</legend>
        <label>
          acks{" "}
          <select value={form.settings.acks} onChange={(e) => setSettings({ acks: e.target.value as ProducerSettings["acks"] })}>
            <option value="0">0</option>
            <option value="1">1</option>
            <option value="all">all</option>
          </select>
        </label>
        <label>
          сжатие{" "}
          <select value={form.settings.compression} onChange={(e) => setSettings({ compression: e.target.value as ProducerSettings["compression"] })}>
            {["none", "gzip", "snappy", "lz4", "zstd"].map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </label>
        <label>
          <input type="checkbox" checked={form.settings.idempotence} onChange={(e) => setSettings({ idempotence: e.target.checked })} /> идемпотентность
        </label>
      </fieldset>
      {formError && <p className="warn">{formError}</p>}
      {send.error && <ErrorBanner error={send.error} />}
      {send.data && !send.isPending && (
        <p role="status">
          Отправлено: партиция {send.data.partition}, offset {send.data.offset}, {formatTimestamp(send.data.timestamp)}
        </p>
      )}
      {!readOnly &&
        (confirming ? (
          <ConfirmDialog
            connection={connection}
            title={`Отправить сообщение в «${form.topic.trim()}»?`}
            confirmLabel="Отправить в prod"
            busy={send.isPending}
            onConfirm={(c) => submit(c.confirm)}
            onCancel={() => setConfirming(false)}
          />
        ) : (
          <div className="toolbar">
            <button
              type="button"
              className="primary"
              disabled={send.isPending}
              onClick={() => (needsConfirmation(connection) ? setConfirming(true) : submit(null))}
            >
              Отправить
            </button>
          </div>
        ))}
    </div>
  );
}
```

В `src/styles.css`:
```css
.producer textarea { width: 100%; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.payload-field { display: flex; flex-direction: column; gap: 4px; }
.headers-editor { border: 1px solid var(--border); border-radius: 4px; display: flex; flex-direction: column; gap: 4px; }
```

- [ ] **Step 9: Вкладка и точки входа**

`src/state/workspace.ts`:
```ts
import type { ProduceMessage, ProducerSettings } from "../api/types";

export type ProducerPrefill = { message: ProduceMessage; settings?: ProducerSettings; nonce: number };
```
В `TabSpec` добавить `| { kind: "producer"; connectionId: string; prefill?: ProducerPrefill }` (id по умолчанию — `producer:<conn>`, ветка `default` в `tabId` уже это даёт).

`src/components/TabContent.tsx`:
```tsx
    case "producer":
      return <ProducerPanel connectionId={tab.connectionId} prefill={tab.prefill} />;
```
`src/components/Layout.tsx`, в `tabTitle`:
```tsx
    case "producer":
      return `${conn} · Producer`;
```
`src/components/Sidebar.tsx`, после «Consumer groups»:
```tsx
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "producer", connectionId: c.id })}>
                  Producer
                </button>
              </li>
```
`src/components/topics/TopicDetails.tsx` — в `toolbar` после «Обновить» (кнопка видна и для read-only: вкладка сама покажет, что отправка недоступна):
```tsx
        <button
          type="button"
          onClick={() =>
            open({
              kind: "producer",
              connectionId,
              prefill: {
                message: { topic, partition: null, key: { format: "string", text: "" }, value: { format: "json", text: "" }, headers: [] },
                nonce: Date.now(),
              },
            })
          }
        >
          Отправить сообщение
        </button>
```
с `const open = useWorkspace((s) => s.open);` в теле компонента.

- [ ] **Step 10: Тесты проходят**

Run: `pnpm vitest run && pnpm build`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add src
git commit -m "feat(ui): вкладка Producer с одиночной отправкой"
```

---

### Task 15: Шаблоны и история в Producer

**Files:**
- Create: `src/components/producer/TemplatesBar.tsx`, `src/components/producer/HistoryList.tsx`
- Create: `src/components/producer/TemplatesHistory.test.tsx`
- Modify: `src/components/producer/ProducerPanel.tsx`

**Interfaces:**
- Consumes: `api.listTemplates`, `api.saveTemplate`, `api.deleteTemplate`, `api.listHistory`, `formToMessage`, `messageToForm`, `ProducerForm`
- Produces:
  - `<TemplatesBar connectionId form: ProducerForm onLoad(form: ProducerForm) />` — выбор шаблона, «Сохранить как шаблон» (имя + «для всех подключений»), удаление
  - `<HistoryList connectionId onRepeat(form: ProducerForm) />` — последние отправки; «Повторить» недоступно для `truncated`
  - Ключи кэша: `["templates", connectionId]`, `["history", connectionId]`

- [ ] **Step 1: Тесты** — создать `src/components/producer/TemplatesHistory.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { HistoryEntry, Template } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { DEFAULT_SETTINGS, emptyProducerForm } from "./form";

const message = { topic: "orders", partition: null, key: { format: "string" as const, text: "k" }, value: { format: "json" as const, text: "{}" }, headers: [] };
const tpl: Template = { id: "t1", name: "заказ", connectionId: null, message, settings: DEFAULT_SETTINGS };
const history: HistoryEntry[] = [
  { id: "h1", sentAt: 0, message: { ...message, key: { format: "string", text: "old" } }, settings: DEFAULT_SETTINGS, result: { partition: 0, offset: 7, timestamp: 0 }, truncated: false },
  { id: "h2", sentAt: 0, message, settings: DEFAULT_SETTINGS, result: { partition: 0, offset: 8, timestamp: 0 }, truncated: true },
];

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listTemplates: vi.fn(),
      saveTemplate: vi.fn(async (t: Template) => ({ ...t, id: "new" })),
      deleteTemplate: vi.fn().mockResolvedValue(undefined),
      listHistory: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { HistoryList } from "./HistoryList";
import { TemplatesBar } from "./TemplatesBar";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listTemplates).mockResolvedValue([tpl]);
  vi.mocked(api.listHistory).mockResolvedValue(history);
});

describe("TemplatesBar", () => {
  it("загружает и сохраняет шаблон", async () => {
    const user = userEvent.setup();
    const onLoad = vi.fn();
    renderWithClient(<TemplatesBar connectionId="c" form={emptyProducerForm("orders")} onLoad={onLoad} />);
    await user.selectOptions(await screen.findByLabelText("Шаблон"), "t1");
    expect(onLoad).toHaveBeenCalledWith(expect.objectContaining({ topic: "orders", key: { format: "string", text: "k" } }));
    await user.click(screen.getByRole("button", { name: "Сохранить как шаблон" }));
    await user.type(screen.getByLabelText("Имя шаблона"), "новый");
    await user.click(screen.getByLabelText("Для всех подключений"));
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveTemplate).toHaveBeenCalledWith(expect.objectContaining({ id: "", name: "новый", connectionId: null }));
  });
});

describe("HistoryList", () => {
  it("повторяет запись, усечённую — нельзя", async () => {
    const user = userEvent.setup();
    const onRepeat = vi.fn();
    renderWithClient(<HistoryList connectionId="c" onRepeat={onRepeat} />);
    const buttons = await screen.findAllByRole("button", { name: "Повторить" });
    expect(buttons[1]).toBeDisabled();
    await user.click(buttons[0]);
    expect(onRepeat).toHaveBeenCalledWith(expect.objectContaining({ key: { format: "string", text: "old" } }));
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/producer/TemplatesHistory.test.tsx`
Expected: FAIL — модули не найдены.

- [ ] **Step 3: `TemplatesBar.tsx`**:

```tsx
import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import { ErrorBanner } from "../common/ErrorBanner";
import { formToMessage, messageToForm, type ProducerForm } from "./form";

interface Props {
  connectionId: string;
  form: ProducerForm;
  onLoad: (form: ProducerForm) => void;
}

export function TemplatesBar({ connectionId, form, onLoad }: Props) {
  const queryClient = useQueryClient();
  const key = ["templates", connectionId];
  const templates = useQuery({ queryKey: key, queryFn: () => api.listTemplates(connectionId) });
  const [selected, setSelected] = useState("");
  const [saving, setSaving] = useState(false);
  const [name, setName] = useState("");
  const [global, setGlobal] = useState(false);
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ["templates"] });
  const save = useMutation({
    mutationFn: () => {
      // В шаблоне топик может быть пустым — проверяем только структуру формы.
      const message = formToMessage({ ...form, topic: form.topic.trim() || "-" });
      return api.saveTemplate({
        id: "",
        name,
        connectionId: global ? null : connectionId,
        message: { ...message, topic: form.topic.trim() },
        settings: form.settings,
      });
    },
    onSuccess: (t) => {
      setSaving(false);
      setName("");
      setSelected(t.id);
      invalidate();
    },
  });
  const remove = useMutation({
    mutationFn: (id: string) => api.deleteTemplate(id),
    onSuccess: () => {
      setSelected("");
      invalidate();
    },
  });

  return (
    <div className="toolbar">
      <label>
        Шаблон{" "}
        <select
          aria-label="Шаблон"
          value={selected}
          onChange={(e) => {
            setSelected(e.target.value);
            const t = templates.data?.find((x) => x.id === e.target.value);
            if (t) onLoad(messageToForm(t.message, t.settings));
          }}
        >
          <option value="">—</option>
          {templates.data?.map((t) => (
            <option key={t.id} value={t.id}>
              {t.name}
              {t.connectionId === null ? " (все подключения)" : ""}
            </option>
          ))}
        </select>
      </label>
      {selected && (
        <button type="button" className="link" onClick={() => remove.mutate(selected)}>
          удалить шаблон
        </button>
      )}
      {saving ? (
        <>
          <input aria-label="Имя шаблона" placeholder="имя шаблона" value={name} onChange={(e) => setName(e.target.value)} />
          <label>
            <input type="checkbox" aria-label="Для всех подключений" checked={global} onChange={(e) => setGlobal(e.target.checked)} /> для всех подключений
          </label>
          <button type="button" className="primary" disabled={!name.trim() || save.isPending} onClick={() => save.mutate()}>
            Сохранить
          </button>
          <button type="button" onClick={() => setSaving(false)}>
            Отмена
          </button>
        </>
      ) : (
        <button type="button" onClick={() => setSaving(true)}>
          Сохранить как шаблон
        </button>
      )}
      {(save.error || remove.error) && <ErrorBanner error={save.error ?? remove.error} />}
    </div>
  );
}
```

- [ ] **Step 4: `HistoryList.tsx`**:

```tsx
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { formatTimestamp } from "../../lib/format";
import { ErrorBanner } from "../common/ErrorBanner";
import { messageToForm, type ProducerForm } from "./form";

export function HistoryList({ connectionId, onRepeat }: { connectionId: string; onRepeat: (form: ProducerForm) => void }) {
  const q = useQuery({ queryKey: ["history", connectionId], queryFn: () => api.listHistory(connectionId) });
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  if (!q.data || q.data.length === 0) return <p className="muted">История отправки пуста.</p>;
  return (
    <table>
      <thead>
        <tr>
          <th>Время</th>
          <th>Топик</th>
          <th>P / offset</th>
          <th>Ключ</th>
          <th />
        </tr>
      </thead>
      <tbody>
        {q.data.map((h) => (
          <tr key={h.id}>
            <td>{formatTimestamp(h.sentAt)}</td>
            <td>{h.message.topic}</td>
            <td>
              {h.result.partition} / {h.result.offset}
            </td>
            <td className="muted">{h.message.key.format === "null" ? "null" : h.message.key.text.slice(0, 40)}</td>
            <td>
              <button
                type="button"
                className="link"
                disabled={h.truncated}
                title={h.truncated ? "Значение длиннее 64 КБ сохранено не полностью — повторить нельзя" : undefined}
                onClick={() => onRepeat(messageToForm(h.message, h.settings))}
              >
                Повторить
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 5: Подключить в `ProducerPanel`** — импорты `TemplatesBar`, `HistoryList`; сразу после `<h2>Producer</h2>`-тулбара:
```tsx
      <TemplatesBar connectionId={connectionId} form={form} onLoad={setForm} />
```
и в конец страницы (после кнопок отправки):
```tsx
      <h3>История (последние 100)</h3>
      <HistoryList connectionId={connectionId} onRepeat={setForm} />
```

- [ ] **Step 6: Тесты проходят**

Run: `pnpm vitest run src/components/producer && pnpm build`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/components/producer
git commit -m "feat(ui): шаблоны и история отправки"
```

---

### Task 16: Пакетная отправка в UI

**Files:**
- Modify: `src-tauri/Cargo.toml` (`tauri-plugin-dialog = "2"`), `src-tauri/src/lib.rs` (`.plugin(tauri_plugin_dialog::init())`), `src-tauri/capabilities/default.json` (`"dialog:allow-open"`)
- Modify: `package.json` (`@tauri-apps/plugin-dialog`)
- Create: `src/components/producer/batch.ts`, `src/components/producer/batch.test.ts`
- Create: `src/components/producer/BatchPanel.tsx`, `src/components/producer/BatchPanel.test.tsx`
- Modify: `src/components/producer/ProducerPanel.tsx`

**Interfaces:**
- Consumes: `api.startBatch`, `api.stopSession`, `formToMessage`, `ConfirmDialog`, `needsConfirmation`, `open` из `@tauri-apps/plugin-dialog`
- Produces:
  - `interface BatchState { status: "idle" | "running" | "finished" | "error"; sent: number; failed: number; total: number | null; errors: BatchError[]; cancelled: boolean; error: AppError | null }`, `initialBatchState`, `batchReducer(s, a: { type: "start" } | { type: "event"; event: BatchEvent } | { type: "failed"; error: AppError })`
  - `fileFormatOf(path: string): "jsonl" | "csv" | null`
  - `<BatchPanel connection form: ProducerForm />`

- [ ] **Step 1: Плагин диалогов**

```bash
pnpm add @tauri-apps/plugin-dialog@^2
```
`src-tauri/Cargo.toml`: `tauri-plugin-dialog = "2"`. `src-tauri/src/lib.rs`: после `.plugin(tauri_plugin_opener::init())` добавить `.plugin(tauri_plugin_dialog::init())`. `src-tauri/capabilities/default.json`: в `permissions` добавить `"dialog:allow-open"`.

Run: `cargo build --manifest-path src-tauri/Cargo.toml`
Expected: сборка без ошибок.

- [ ] **Step 2: Тесты редьюсера** — создать `src/components/producer/batch.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { batchReducer, fileFormatOf, initialBatchState } from "./batch";

describe("batchReducer", () => {
  it("прогресс, ошибки, завершение", () => {
    let s = batchReducer(initialBatchState, { type: "start" });
    expect(s.status).toBe("running");
    s = batchReducer(s, { type: "event", event: { type: "progress", sent: 5, failed: 1, total: 10 } });
    expect([s.sent, s.failed, s.total]).toEqual([5, 1, 10]);
    s = batchReducer(s, { type: "event", event: { type: "errors", errors: [{ line: 3, message: "плохо" }] } });
    expect(s.errors).toHaveLength(1);
    s = batchReducer(s, { type: "event", event: { type: "finished", sent: 9, failed: 1, cancelled: true } });
    expect(s).toMatchObject({ status: "finished", sent: 9, cancelled: true });
    const err = { code: "INVALID_INPUT" as const, message: "x", retryable: false };
    expect(batchReducer(s, { type: "event", event: { type: "error", error: err } }).error).toEqual(err);
  });

  it("формат по расширению", () => {
    expect(fileFormatOf("/a/b.JSONL")).toBe("jsonl");
    expect(fileFormatOf("/a/b.ndjson")).toBe("jsonl");
    expect(fileFormatOf("c:\\x.csv")).toBe("csv");
    expect(fileFormatOf("/a/b.txt")).toBeNull();
  });
});
```

- [ ] **Step 3: Убедиться в падении**

Run: `pnpm vitest run src/components/producer/batch.test.ts`
Expected: FAIL — модуль не найден.

- [ ] **Step 4: `batch.ts`**:

```ts
import type { AppError, BatchError, BatchEvent } from "../../api/types";

export interface BatchState {
  status: "idle" | "running" | "finished" | "error";
  sent: number;
  failed: number;
  total: number | null;
  errors: BatchError[];
  cancelled: boolean;
  error: AppError | null;
}

export const initialBatchState: BatchState = {
  status: "idle",
  sent: 0,
  failed: 0,
  total: null,
  errors: [],
  cancelled: false,
  error: null,
};

export type BatchAction = { type: "start" } | { type: "event"; event: BatchEvent } | { type: "failed"; error: AppError };

export function batchReducer(s: BatchState, a: BatchAction): BatchState {
  switch (a.type) {
    case "start":
      return { ...initialBatchState, status: "running" };
    case "failed":
      return { ...s, status: "error", error: a.error };
    case "event": {
      const e = a.event;
      switch (e.type) {
        case "progress":
          return { ...s, sent: e.sent, failed: e.failed, total: e.total };
        case "errors":
          return { ...s, errors: e.errors };
        case "finished":
          return { ...s, status: "finished", sent: e.sent, failed: e.failed, cancelled: e.cancelled };
        case "error":
          return { ...s, status: "error", error: e.error };
      }
    }
  }
}

export function fileFormatOf(path: string): "jsonl" | "csv" | null {
  const p = path.toLowerCase();
  if (p.endsWith(".jsonl") || p.endsWith(".ndjson")) return "jsonl";
  if (p.endsWith(".csv")) return "csv";
  return null;
}
```

- [ ] **Step 5: Тест панели** — создать `src/components/producer/BatchPanel.test.tsx`:

```tsx
import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { BatchEvent, ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";
import { emptyProducerForm } from "./form";

let onEvent: (e: BatchEvent) => void = () => {};

vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn().mockResolvedValue("/data/msgs.jsonl") }));
vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      startBatch: vi.fn(async (_p: unknown, cb: (e: BatchEvent) => void) => {
        onEvent = cb;
        return "sid";
      }),
      stopSession: vi.fn().mockResolvedValue(true),
    },
  };
});

import { api } from "../../api/client";
import { BatchPanel } from "./BatchPanel";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const form = { ...emptyProducerForm("orders"), value: { format: "json" as const, text: '{"n":{{seq}}}' } };

beforeEach(() => vi.clearAllMocks());

describe("BatchPanel", () => {
  it("N копий с ограничением скорости, прогресс и отмена", async () => {
    const user = userEvent.setup();
    renderWithClient(<BatchPanel connection={dev} form={form} />);
    await user.clear(screen.getByLabelText("Копий"));
    await user.type(screen.getByLabelText("Копий"), "100");
    await user.type(screen.getByLabelText("Сообщений в секунду"), "10");
    await user.click(screen.getByRole("button", { name: "Запустить" }));
    const [params, , confirm] = vi.mocked(api.startBatch).mock.calls[0];
    expect(params).toMatchObject({ connectionId: "c", ratePerSec: 10, source: { type: "copies", count: 100, message: { topic: "orders" } } });
    expect(confirm).toBeNull();
    act(() => onEvent({ type: "progress", sent: 40, failed: 0, total: 100 }));
    expect(screen.getByText("отправлено 40 из 100 · ошибок 0")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.stopSession).toHaveBeenCalledWith("sid");
  });

  it("файл: формат по расширению", async () => {
    const user = userEvent.setup();
    renderWithClient(<BatchPanel connection={dev} form={form} />);
    await user.click(screen.getByLabelText("Из файла"));
    await user.click(screen.getByRole("button", { name: "Выбрать файл…" }));
    expect(await screen.findByText("/data/msgs.jsonl")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Запустить" }));
    expect(vi.mocked(api.startBatch).mock.calls[0][0].source).toEqual({ type: "file", path: "/data/msgs.jsonl", format: "jsonl", topic: "orders" });
  });
});
```

- [ ] **Step 6: Убедиться в падении**

Run: `pnpm vitest run src/components/producer/BatchPanel.test.tsx`
Expected: FAIL — модуль не найден.

- [ ] **Step 7: `BatchPanel.tsx`**:

```tsx
import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { api, toAppError } from "../../api/client";
import type { BatchSource, ConnectionProfile } from "../../api/types";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { batchReducer, fileFormatOf, initialBatchState } from "./batch";
import { formToMessage, type ProducerForm } from "./form";

type Mode = "copies" | "file";

export function BatchPanel({ connection, form }: { connection: ConnectionProfile; form: ProducerForm }) {
  const [mode, setMode] = useState<Mode>("copies");
  const [count, setCount] = useState("10");
  const [rate, setRate] = useState("");
  const [path, setPath] = useState<string | null>(null);
  const [formError, setFormError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  const [state, dispatch] = useReducer(batchReducer, initialBatchState);
  const sessionRef = useRef<string | null>(null);
  const running = state.status === "running";

  const stop = useCallback(async () => {
    const id = sessionRef.current;
    sessionRef.current = null;
    if (id) await api.stopSession(id).catch(() => false);
  }, []);
  // Закрытие вкладки останавливает пакет.
  useEffect(() => () => void stop(), [stop]);

  const buildSource = (): BatchSource => {
    if (mode === "copies") {
      const n = Number(count);
      if (!Number.isInteger(n) || n < 1 || n > 1_000_000) throw new Error("Копий — от 1 до 1 000 000");
      return { type: "copies", count: n, message: formToMessage(form) };
    }
    if (!path) throw new Error("Выберите файл");
    const format = fileFormatOf(path);
    if (!format) throw new Error("Поддерживаются файлы .jsonl, .ndjson и .csv");
    if (!form.topic.trim()) throw new Error("Укажите топик в форме выше");
    return { type: "file", path, format, topic: form.topic.trim() };
  };

  const start = async (confirm: string | null) => {
    let source: BatchSource;
    const r = rate.trim() ? Number(rate) : null;
    try {
      source = buildSource();
      if (r !== null && (!Number.isInteger(r) || r < 1)) throw new Error("Скорость — целое число ≥ 1");
      setFormError(null);
    } catch (e) {
      setFormError((e as Error).message);
      return;
    }
    setConfirming(false);
    await stop();
    dispatch({ type: "start" });
    try {
      sessionRef.current = await api.startBatch(
        { connectionId: connection.id, source, settings: form.settings, ratePerSec: r },
        (event) => dispatch({ type: "event", event }),
        confirm,
      );
    } catch (e) {
      dispatch({ type: "failed", error: toAppError(e) });
    }
  };

  const chooseFile = async () => {
    const selected = await openDialog({
      multiple: false,
      directory: false,
      filters: [{ name: "JSON Lines / CSV", extensions: ["jsonl", "ndjson", "csv"] }],
    });
    if (typeof selected === "string") setPath(selected);
  };

  return (
    <fieldset className="panel">
      <legend>Пакетная отправка</legend>
      <div className="toolbar">
        <label>
          <input type="radio" name={`batch-mode-${connection.id}`} checked={mode === "copies"} onChange={() => setMode("copies")} /> N копий формы
        </label>
        <label>
          <input type="radio" aria-label="Из файла" name={`batch-mode-${connection.id}`} checked={mode === "file"} onChange={() => setMode("file")} /> из файла
        </label>
        {mode === "copies" ? (
          <label>
            Копий <input aria-label="Копий" type="number" min={1} value={count} onChange={(e) => setCount(e.target.value)} style={{ width: 100 }} />
          </label>
        ) : (
          <>
            <button type="button" onClick={() => void chooseFile()}>
              Выбрать файл…
            </button>
            {path && <code>{path}</code>}
          </>
        )}
        <label>
          Скорость{" "}
          <input aria-label="Сообщений в секунду" type="number" min={1} placeholder="без ограничения" value={rate} onChange={(e) => setRate(e.target.value)} style={{ width: 120 }} />
        </label>
      </div>
      <p className="muted">
        {mode === "copies"
          ? "Плейсхолдеры подставляются для каждой копии, {{seq}} — номер копии с 0."
          : "JSON Lines: {\"key\":…, \"value\":…, \"headers\":{…}, \"partition\":…} в строке. CSV: колонки key, value, partition, header.<имя>."}
      </p>
      {formError && <p className="warn">{formError}</p>}
      {state.error && <ErrorBanner error={state.error} />}
      {state.status !== "idle" && (
        <p role="status">
          {state.total !== null ? `отправлено ${state.sent} из ${state.total}` : `отправлено ${state.sent}`} · ошибок {state.failed}
          {state.status === "finished" && (state.cancelled ? " · отменено" : " · готово")}
        </p>
      )}
      {state.errors.length > 0 && (
        <ul className="warn">
          {state.errors.map((e, i) => (
            <li key={i}>
              {e.line > 0 ? `строка ${e.line}: ` : ""}
              {e.message}
            </li>
          ))}
        </ul>
      )}
      {confirming ? (
        <ConfirmDialog
          connection={connection}
          title="Запустить пакетную отправку?"
          confirmLabel="Запустить в prod"
          onConfirm={(c) => void start(c.confirm)}
          onCancel={() => setConfirming(false)}
        />
      ) : (
        <div className="toolbar">
          <button type="button" className="primary" disabled={running} onClick={() => (needsConfirmation(connection) ? setConfirming(true) : void start(null))}>
            Запустить
          </button>
          <button type="button" disabled={!running} onClick={() => void stop()}>
            Отменить
          </button>
        </div>
      )}
    </fieldset>
  );
}
```

В `ProducerPanel` (только когда `!readOnly`), после блока кнопок отправки:
```tsx
      {!readOnly && <BatchPanel connection={connection} form={form} />}
```

- [ ] **Step 8: Тесты проходят**

Run: `pnpm vitest run src/components/producer && pnpm build && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add package.json pnpm-lock.yaml src src-tauri
git commit -m "feat(ui): пакетная отправка — копии и файлы, прогресс и отмена"
```

---

### Task 17: «Отправить повторно» из просмотра сообщений

**Files:**
- Create: `src/components/producer/fromMessage.ts`, `src/components/producer/fromMessage.test.ts`
- Modify: `src/components/browse/MessageDetails.tsx` (кнопка; `topic` берётся из `message.topic`, проп `topic` удаляется)
- Modify: `src/components/browse/BrowsePanel.tsx`, `src/components/browse/MessageDetails.test.tsx`

**Interfaces:**
- Consumes: `BrowsedMessage` (с `topic`, Task 11), `ProducerPrefill`, `useWorkspace.open`
- Produces:
  - `browsedToMessage(m: BrowsedMessage): ProduceMessage` — бросает `Error`, если key/value усечены (`truncated`); форматы: `json`→`json`, `string`→`string`, `hex`→`hex`, `base64`→`base64`, нет значения → `null`; заголовки — `string`, значения вида `0x…` (не UTF-8) — `hex` без префикса; партиция — `null` (партиционер)
  - `<MessageDetails message utc connectionId />` — проп `topic` больше не нужен

- [ ] **Step 1: Тесты** — создать `src/components/producer/fromMessage.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import type { BrowsedMessage, DecodedData } from "../../api/types";
import { browsedToMessage } from "./fromMessage";

const d = (format: DecodedData["format"], text: string, truncated = false): DecodedData => ({ format, text, size: text.length, truncated });

describe("browsedToMessage", () => {
  it("сохраняет форматы и заголовки", () => {
    const m: BrowsedMessage = {
      topic: "orders",
      partition: 3,
      offset: 10,
      timestamp: 0,
      key: d("hex", "00ff"),
      value: d("json", '{"a":1}'),
      headers: [
        { name: "trace", value: "abc" },
        { name: "bin", value: "0xff00" },
        { name: "nil", value: null },
      ],
      size: 9,
    };
    expect(browsedToMessage(m)).toEqual({
      topic: "orders",
      partition: null,
      key: { format: "hex", text: "00ff" },
      value: { format: "json", text: '{"a":1}' },
      headers: [
        { name: "trace", value: "abc", format: "string" },
        { name: "bin", value: "ff00", format: "hex" },
        { name: "nil", value: "", format: "string" },
      ],
    });
  });

  it("null и усечённые значения", () => {
    const base: BrowsedMessage = { topic: "t", partition: 0, offset: 0, timestamp: null, key: null, value: d("string", "v"), headers: [], size: 1 };
    expect(browsedToMessage(base).key).toEqual({ format: "null", text: "" });
    expect(() => browsedToMessage({ ...base, value: d("string", "v", true) })).toThrow("больше 1 МБ");
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/producer/fromMessage.test.ts`
Expected: FAIL — модуль не найден.

- [ ] **Step 3: `fromMessage.ts`**:

```ts
import type { BrowsedMessage, DecodedData, PayloadInput, ProduceMessage } from "../../api/types";

function payload(d: DecodedData | null): PayloadInput {
  if (!d) return { format: "null", text: "" };
  if (d.truncated) throw new Error("Значение больше 1 МБ показано не полностью — отправить его повторно нельзя");
  // format "auto" в ответе не встречается: Rust возвращает фактический формат.
  const format = d.format === "auto" ? "string" : d.format;
  return { format, text: d.text };
}

/** Сообщение из Browse → форма Producer (партицию выбирает партиционер). */
export function browsedToMessage(m: BrowsedMessage): ProduceMessage {
  return {
    topic: m.topic,
    partition: null,
    key: payload(m.key),
    value: payload(m.value),
    headers: m.headers.map((h) => {
      const v = h.value ?? "";
      // header_text в Rust помечает не-UTF-8 значения префиксом 0x.
      return /^0x[0-9a-f]*$/i.test(v) && v.length > 2
        ? { name: h.name, value: v.slice(2), format: "hex" as const }
        : { name: h.name, value: v, format: "string" as const };
    }),
  };
}
```

- [ ] **Step 4: Кнопка в `MessageDetails`** — в `src/components/browse/MessageDetails.tsx`:
  - убрать проп `topic` из `Props` и сигнатуры; в `ResetGroupHere` передавать `topic={message.topic}`;
  - импорт `import { browsedToMessage } from "../producer/fromMessage";`;
  - в теле `MessageDetails` (после `useConnection`): `const open = useWorkspace((s) => s.open);` и `const [resendError, setResendError] = useState<string | null>(null);`;
  - после строки с метаданными сообщения (перед `ResetGroupHere`):
```tsx
      {connection && !connection.readOnly && (
        <button
          type="button"
          className="link"
          onClick={() => {
            try {
              open({ kind: "producer", connectionId, prefill: { message: browsedToMessage(message), nonce: Date.now() } });
              setResendError(null);
            } catch (e) {
              setResendError((e as Error).message);
            }
          }}
        >
          Отправить повторно
        </button>
      )}
      {resendError && <p className="warn">{resendError}</p>}
```
Хуки `useState`/`useWorkspace` вызываются до раннего `return` для `message === null`.

`src/components/browse/BrowsePanel.tsx`: `<MessageDetails message={selected} utc={utc} connectionId={connectionId} />`.

`src/components/browse/MessageDetails.test.tsx`: убрать `topic="orders"` из рендера, в фикстуре `msg` — `topic: "orders"`; добавить тест:
```tsx
  it("«Отправить повторно» открывает Producer с сообщением", async () => {
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    const m = { ...msg, value: { format: "json" as const, text: "{}", size: 2, truncated: false } };
    renderWithClient(<MessageDetails message={m} utc={false} connectionId="c" />);
    await user.click(await screen.findByRole("button", { name: "Отправить повторно" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({
      kind: "producer",
      prefill: { message: { topic: "orders", value: { format: "json", text: "{}" } } },
    });
  });
```

- [ ] **Step 5: Тесты проходят**

Run: `pnpm vitest run && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src
git commit -m "feat(ui): «Отправить повторно» из просмотра сообщений"
```

---

### Task 18: Вкладка consumer-сессии

**Files:**
- Modify: `src/components/browse/params.ts` (`FilterFields`, `buildFilterSpec`), `src/components/browse/BrowseForm.tsx`
- Create: `src/components/browse/FilterFields.tsx`
- Create: `src/components/consumer/form.ts`, `consumer/form.test.ts`, `consumer/state.ts`, `consumer/state.test.ts`, `consumer/useConsumerSession.ts`, `consumer/AssignmentPanel.tsx`, `consumer/ConsumerPanel.tsx`, `consumer/ConsumerPanel.test.tsx`
- Modify: `src/state/workspace.ts`, `src/components/TabContent.tsx`, `src/components/Layout.tsx`, `src/components/Sidebar.tsx`, `src/components/topics/TopicDetails.tsx`, `src/components/groups/GroupDetails.tsx`

**Interfaces:**
- Consumes: `api.startConsumer`, `api.stopSession`, `api.commitOffsets`, `api.pausePartitions`, `api.resumePartitions`, `api.suggestGroupId`, `appendMessages`, `MessageTable`, `messageKey`, `MessageDetails`, `ConfirmDialog`, `needsConfirmation`, `useConnection`
- Produces:
  - `type FilterFields = Pick<BrowseFormState, "keyPattern" | "valuePattern" | "regex" | "caseSensitive" | "headerName" | "headerValue" | "jsonPath" | "fromTime" | "toTime">`, `buildFilterSpec(f: FilterFields): FilterSpec` (`buildBrowseParams` использует её — поведение Browse не меняется)
  - `<FilterFields<T extends FilterFields> value: T onChange(v: T) />` — fieldset «Фильтры», вынесенный из `BrowseForm`
  - `interface ConsumerFormState extends FilterFields { groupId; topics; useRegex; pattern; autoOffsetReset; enableAutoCommit; isolationLevel; keyFormat; valueFormat; limit }`, `defaultConsumerForm(init?: { topic?: string; groupId?: string })`, `buildConsumerParams(f, connectionId): ConsumerParams`
  - `consumerReducer`, `initialConsumerState`, `partitionKey(p: TopicPartitionRef): string`
  - `useConsumerSession(): { state, start(params, confirm), stop() }`
  - `TabSpec` += `{ kind: "consumer"; connectionId: string; key: string; topic?: string; groupId?: string }` (id `consumer:<conn>:<key>` — несколько сессий параллельно)
  - `<ConsumerPanel connectionId topic? groupId? />`

- [ ] **Step 1: Вынести фильтры** — в `src/components/browse/params.ts`:

```ts
export type FilterFields = Pick<
  BrowseFormState,
  "keyPattern" | "valuePattern" | "regex" | "caseSensitive" | "headerName" | "headerValue" | "jsonPath" | "fromTime" | "toTime"
>;

export function buildFilterSpec(f: FilterFields): FilterSpec {
  const text = (pattern: string): TextMatch | undefined =>
    pattern ? { pattern, regex: f.regex, caseSensitive: f.caseSensitive } : undefined;
  const filter: FilterSpec = {};
  const key = text(f.keyPattern);
  if (key) filter.key = key;
  const value = text(f.valuePattern);
  if (value) filter.value = value;
  if (f.headerName.trim()) {
    const hv = text(f.headerValue);
    filter.header = hv ? { name: f.headerName.trim(), value: hv } : { name: f.headerName.trim() };
  }
  if (f.jsonPath.trim()) filter.jsonPath = f.jsonPath.trim();
  if (f.fromTime) filter.fromTimestamp = parseLocalTime(f.fromTime, "фильтр «с»");
  if (f.toTime) filter.toTimestamp = parseLocalTime(f.toTime, "фильтр «по»");
  if (filter.fromTimestamp !== undefined && filter.toTimestamp !== undefined && filter.fromTimestamp > filter.toTimestamp) {
    throw new Error("Фильтр времени: «с» не может быть позже «по»");
  }
  return filter;
}
```
В `buildBrowseParams` блок от `const text = …` до проверки времени включительно заменить на `const filter = buildFilterSpec(f);`.

Создать `src/components/browse/FilterFields.tsx` — fieldset «Фильтры» из `BrowseForm.tsx` (разметка та же, метки не меняются):
```tsx
import type { FilterFields as Fields } from "./params";

export function FilterFields<T extends Fields>({ value: form, onChange }: { value: T; onChange: (v: T) => void }) {
  const set = <K extends keyof Fields>(k: K, v: Fields[K]) => onChange({ ...form, [k]: v } as T);
  return (
    <fieldset>
      <legend>Фильтры</legend>
      <label className="field">
        <span>Ключ содержит</span>
        <input placeholder="k7" value={form.keyPattern} onChange={(e) => set("keyPattern", e.target.value)} />
      </label>
      <label className="field">
        <span>Значение содержит</span>
        <input placeholder="FAILED" value={form.valuePattern} onChange={(e) => set("valuePattern", e.target.value)} />
      </label>
      <label>
        <input type="checkbox" checked={form.regex} onChange={(e) => set("regex", e.target.checked)} /> regex
      </label>
      <label>
        <input type="checkbox" checked={form.caseSensitive} onChange={(e) => set("caseSensitive", e.target.checked)} /> Aa
      </label>
      <label className="field">
        <span>Заголовок</span>
        <input placeholder="trace-id" value={form.headerName} onChange={(e) => set("headerName", e.target.value)} style={{ width: 110 }} />
      </label>
      <label className="field">
        <span>Значение заголовка</span>
        <input placeholder="abc-123" value={form.headerValue} onChange={(e) => set("headerValue", e.target.value)} style={{ width: 130 }} />
      </label>
      <label className="field">
        <span>JSONPath</span>
        <input
          placeholder={'$.order.status == "FAILED"'}
          value={form.jsonPath}
          onChange={(e) => set("jsonPath", e.target.value)}
          style={{ width: 260 }}
        />
      </label>
      <label className="field">
        <span>Время с</span>
        <input type="datetime-local" step={1} value={form.fromTime} onChange={(e) => set("fromTime", e.target.value)} />
      </label>
      <label className="field">
        <span>Время по</span>
        <input type="datetime-local" step={1} value={form.toTime} onChange={(e) => set("toTime", e.target.value)} />
      </label>
    </fieldset>
  );
}
```
В `BrowseForm.tsx` fieldset «Фильтры» целиком заменить на `<FilterFields value={form} onChange={onChange} />` (импорт `import { FilterFields } from "./FilterFields";`).

Run: `pnpm vitest run src/components/browse && pnpm build`
Expected: PASS (тесты Browse не меняются).

- [ ] **Step 2: Тесты формы и редьюсера** — создать `src/components/consumer/form.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { buildConsumerParams, defaultConsumerForm } from "./form";

describe("consumer form", () => {
  it("список топиков", () => {
    const f = { ...defaultConsumerForm({ topic: "a, b\nc", groupId: "g" }), valuePattern: "FAILED" };
    const p = buildConsumerParams(f, "conn");
    expect(p).toMatchObject({
      connectionId: "conn",
      groupId: "g",
      topics: ["a", "b", "c"],
      topicPattern: null,
      autoOffsetReset: "latest",
      enableAutoCommit: false,
      isolationLevel: "readCommitted",
      filter: { value: { pattern: "FAILED", regex: false, caseSensitive: false } },
    });
  });

  it("regex-подписка и ошибки", () => {
    const f = { ...defaultConsumerForm({ groupId: "g" }), useRegex: true, pattern: "orders\\..*" };
    expect(buildConsumerParams(f, "c")).toMatchObject({ topics: [], topicPattern: "orders\\..*" });
    expect(() => buildConsumerParams({ ...f, pattern: "(" }, "c")).toThrow("регулярное выражение");
    expect(() => buildConsumerParams(defaultConsumerForm({ groupId: "g" }), "c")).toThrow("Укажите топики");
    expect(() => buildConsumerParams(defaultConsumerForm({ topic: "t" }), "c")).toThrow("Укажите group.id");
    expect(() => buildConsumerParams({ ...defaultConsumerForm({ topic: "t", groupId: "g" }), limit: 0 }, "c")).toThrow("Буфер");
  });
});
```

Создать `src/components/consumer/state.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import type { BrowsedMessage } from "../../api/types";
import { consumerReducer, initialConsumerState } from "./state";

const msg = (offset: number): BrowsedMessage => ({ topic: "t", partition: 0, offset, timestamp: null, key: null, value: null, headers: [], size: 0 });
const tp = (partition: number) => ({ topic: "t", partition });

describe("consumerReducer", () => {
  it("сообщения, назначения, пауза, коммит", () => {
    let s = consumerReducer(initialConsumerState, { type: "start", limit: 2 });
    s = consumerReducer(s, { type: "event", event: { type: "messages", messages: [msg(1), msg(2), msg(3)] } });
    expect(s.messages.map((m) => m.offset)).toEqual([2, 3]);
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "assigned", partitions: [tp(0), tp(1)], at: 1 } } });
    expect(s.assignment).toEqual([tp(0), tp(1)]);
    s = consumerReducer(s, { type: "event", event: { type: "paused", partitions: [tp(1)] } });
    expect(s.paused).toEqual(["t:1"]);
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "revoked", partitions: [tp(1)], at: 2 } } });
    expect(s.assignment).toEqual([tp(0)]);
    expect(s.paused).toEqual([], "отозванная партиция снимается с паузы");
    expect(s.log).toHaveLength(2);
    s = consumerReducer(s, { type: "event", event: { type: "committed", offsets: [{ topic: "t", partition: 0, offset: 4 }] } });
    expect(s.lastCommit).toEqual([{ topic: "t", partition: 0, offset: 4 }]);
    s = consumerReducer(s, { type: "event", event: { type: "finished", consumed: 3, matched: 3 } });
    expect(s.status).toBe("finished");
    expect(s.assignment).toEqual([]);
  });
});
```

- [ ] **Step 3: Убедиться в падении**

Run: `pnpm vitest run src/components/consumer`
Expected: FAIL — модули не найдены.

- [ ] **Step 4: `consumer/form.ts`**:

```ts
import type { ConsumerParams, DataFormat } from "../../api/types";
import { buildFilterSpec, type FilterFields } from "../browse/params";

export interface ConsumerFormState extends FilterFields {
  groupId: string;
  /** Топики через запятую, пробел или перевод строки */
  topics: string;
  useRegex: boolean;
  pattern: string;
  autoOffsetReset: ConsumerParams["autoOffsetReset"];
  enableAutoCommit: boolean;
  isolationLevel: ConsumerParams["isolationLevel"];
  keyFormat: DataFormat;
  valueFormat: DataFormat;
  /** Размер буфера сообщений в UI */
  limit: number;
}

export function defaultConsumerForm(init: { topic?: string; groupId?: string } = {}): ConsumerFormState {
  return {
    groupId: init.groupId ?? "",
    topics: init.topic ?? "",
    useRegex: false,
    pattern: "",
    autoOffsetReset: "latest",
    enableAutoCommit: false,
    isolationLevel: "readCommitted",
    keyFormat: "auto",
    valueFormat: "auto",
    limit: 5000,
    keyPattern: "",
    valuePattern: "",
    regex: false,
    caseSensitive: false,
    headerName: "",
    headerValue: "",
    jsonPath: "",
    fromTime: "",
    toTime: "",
  };
}

const MAX_BUFFER = 100_000;

export function buildConsumerParams(f: ConsumerFormState, connectionId: string): ConsumerParams {
  if (!f.groupId.trim()) throw new Error("Укажите group.id");
  if (!Number.isInteger(f.limit) || f.limit < 1 || f.limit > MAX_BUFFER) throw new Error(`Буфер — от 1 до ${MAX_BUFFER} сообщений`);
  let topics: string[] = [];
  let topicPattern: string | null = null;
  if (f.useRegex) {
    if (!f.pattern.trim()) throw new Error("Укажите регулярное выражение для топиков");
    try {
      new RegExp(f.pattern.trim());
    } catch {
      throw new Error("Некорректное регулярное выражение для топиков");
    }
    topicPattern = f.pattern.trim();
  } else {
    topics = f.topics.split(/[\s,]+/).filter(Boolean);
    if (topics.length === 0) throw new Error("Укажите топики");
  }
  return {
    connectionId,
    groupId: f.groupId.trim(),
    topics,
    topicPattern,
    autoOffsetReset: f.autoOffsetReset,
    enableAutoCommit: f.enableAutoCommit,
    isolationLevel: f.isolationLevel,
    keyFormat: f.keyFormat,
    valueFormat: f.valueFormat,
    filter: buildFilterSpec(f),
  };
}
```

- [ ] **Step 5: `consumer/state.ts`**:

```ts
import type { AppError, BrowsedMessage, ConsumeEvent, OffsetTarget, RebalanceRecord, TopicPartitionRef } from "../../api/types";
import { appendMessages } from "../browse/state";

export const partitionKey = (p: TopicPartitionRef) => `${p.topic}:${p.partition}`;
const LOG_LIMIT = 200;

export interface ConsumerState {
  status: "idle" | "running" | "finished" | "error";
  sessionId: string | null;
  messages: BrowsedMessage[];
  consumed: number;
  matched: number;
  assignment: TopicPartitionRef[];
  paused: string[];
  log: RebalanceRecord[];
  lastCommit: OffsetTarget[] | null;
  reconnecting: string | null;
  error: AppError | null;
  limit: number;
}

export const initialConsumerState: ConsumerState = {
  status: "idle",
  sessionId: null,
  messages: [],
  consumed: 0,
  matched: 0,
  assignment: [],
  paused: [],
  log: [],
  lastCommit: null,
  reconnecting: null,
  error: null,
  limit: 5000,
};

export type ConsumerAction =
  | { type: "start"; limit: number }
  | { type: "started"; sessionId: string }
  | { type: "event"; event: ConsumeEvent }
  | { type: "failed"; error: AppError }
  | { type: "stopped" };

const without = (list: TopicPartitionRef[], remove: TopicPartitionRef[]) => {
  const keys = new Set(remove.map(partitionKey));
  return list.filter((p) => !keys.has(partitionKey(p)));
};

export function consumerReducer(s: ConsumerState, a: ConsumerAction): ConsumerState {
  switch (a.type) {
    case "start":
      return { ...initialConsumerState, status: "running", limit: a.limit };
    case "started":
      return { ...s, sessionId: a.sessionId };
    case "failed":
      return { ...s, status: "error", error: a.error, reconnecting: null };
    case "stopped":
      return s.status === "running" ? { ...s, status: "finished", assignment: [], paused: [], reconnecting: null } : s;
    case "event": {
      const e = a.event;
      switch (e.type) {
        case "messages":
          return { ...s, messages: appendMessages(s.messages, e.messages, s.limit) };
        case "progress":
          return { ...s, consumed: e.consumed, matched: e.matched };
        case "rebalance": {
          const log = [...s.log, e.record].slice(-LOG_LIMIT);
          const r = e.record;
          if (r.kind === "assigned") {
            // Eager-протокол присылает полное назначение после revoke, cooperative — прирост: объединяем.
            return { ...s, log, assignment: [...without(s.assignment, r.partitions), ...r.partitions] };
          }
          if (r.kind === "revoked") {
            const removed = new Set(r.partitions.map(partitionKey));
            return { ...s, log, assignment: without(s.assignment, r.partitions), paused: s.paused.filter((k) => !removed.has(k)) };
          }
          return { ...s, log };
        }
        case "committed":
          return { ...s, lastCommit: e.offsets };
        case "paused":
          return { ...s, paused: [...new Set([...s.paused, ...e.partitions.map(partitionKey)])] };
        case "resumed": {
          const keys = new Set(e.partitions.map(partitionKey));
          return { ...s, paused: s.paused.filter((k) => !keys.has(k)) };
        }
        case "reconnecting":
          return { ...s, reconnecting: e.message };
        case "recovered":
          return { ...s, reconnecting: null };
        case "finished":
          return { ...s, status: "finished", consumed: e.consumed, matched: e.matched, assignment: [], paused: [], reconnecting: null };
        case "error":
          return { ...s, status: "error", error: e.error, reconnecting: null };
      }
    }
  }
}
```

- [ ] **Step 6: Тесты формы и редьюсера проходят**

Run: `pnpm vitest run src/components/consumer`
Expected: PASS.

- [ ] **Step 7: Хук сессии** — `consumer/useConsumerSession.ts` (тот же протокол токенов, что `useBrowseSession`: события остановленной сессии игнорируются, поздний ответ запуска останавливает сессию):

```ts
import { useCallback, useEffect, useReducer, useRef } from "react";
import { api, toAppError } from "../../api/client";
import type { ConsumerParams } from "../../api/types";
import { consumerReducer, initialConsumerState } from "./state";

export function useConsumerSession(limit: number) {
  const [state, dispatch] = useReducer(consumerReducer, initialConsumerState);
  const sessionRef = useRef<string | null>(null);
  const tokenRef = useRef(0);

  const stopInternal = useCallback(async () => {
    tokenRef.current++;
    const id = sessionRef.current;
    sessionRef.current = null;
    if (id) await api.stopSession(id).catch(() => false);
  }, []);

  const stop = useCallback(async () => {
    dispatch({ type: "stopped" });
    await stopInternal();
  }, [stopInternal]);

  const start = useCallback(
    async (params: ConsumerParams, confirm: string | null) => {
      const stopping = stopInternal();
      const gen = tokenRef.current;
      await stopping;
      if (tokenRef.current !== gen) return;
      const token = ++tokenRef.current;
      dispatch({ type: "start", limit });
      try {
        const id = await api.startConsumer(
          params,
          (event) => {
            if (tokenRef.current === token) dispatch({ type: "event", event });
          },
          confirm,
        );
        if (tokenRef.current !== token) {
          await api.stopSession(id).catch(() => false);
          return;
        }
        sessionRef.current = id;
        dispatch({ type: "started", sessionId: id });
      } catch (e) {
        if (tokenRef.current === token) dispatch({ type: "failed", error: toAppError(e) });
      }
    },
    [stopInternal, limit],
  );

  // Закрытие вкладки — выход из группы.
  useEffect(() => () => void stopInternal(), [stopInternal]);

  return { state, start, stop };
}
```

- [ ] **Step 8: `AssignmentPanel.tsx`**:

```tsx
import type { RebalanceRecord, TopicPartitionRef } from "../../api/types";
import { formatTimestamp } from "../../lib/format";
import { partitionKey } from "./state";

interface Props {
  assignment: TopicPartitionRef[];
  paused: string[];
  log: RebalanceRecord[];
  disabled: boolean;
  onPause: (p: TopicPartitionRef) => void;
  onResume: (p: TopicPartitionRef) => void;
}

const LOG_TEXT = { assigned: "назначены", revoked: "отозваны", error: "ошибка ребаланса" } as const;

export function AssignmentPanel({ assignment, paused, log, disabled, onPause, onResume }: Props) {
  const sorted = [...assignment].sort((a, b) => partitionKey(a).localeCompare(partitionKey(b)));
  return (
    <div className="assignment">
      <h4>Назначение ({assignment.length})</h4>
      {sorted.length === 0 && <p className="muted">Партиции не назначены.</p>}
      <ul>
        {sorted.map((p) => {
          const isPaused = paused.includes(partitionKey(p));
          return (
            <li key={partitionKey(p)}>
              <code>{partitionKey(p)}</code> {isPaused && <span className="warn">пауза</span>}{" "}
              <button
                type="button"
                className="link"
                disabled={disabled}
                aria-label={`${isPaused ? "Продолжить" : "Пауза"} ${partitionKey(p)}`}
                onClick={() => (isPaused ? onResume(p) : onPause(p))}
              >
                {isPaused ? "продолжить" : "пауза"}
              </button>
            </li>
          );
        })}
      </ul>
      <h4>Журнал ребалансов</h4>
      <ul className="muted">
        {[...log].reverse().map((r, i) => (
          <li key={i}>
            {formatTimestamp(r.at)} — {LOG_TEXT[r.kind]}
            {r.kind === "error" ? `: ${r.message}` : `: ${r.partitions.map(partitionKey).join(", ") || "—"}`}
          </li>
        ))}
      </ul>
    </div>
  );
}
```

- [ ] **Step 9: Тест панели** — создать `consumer/ConsumerPanel.test.tsx`:

```tsx
import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile, ConsumeEvent } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

let emit: (e: ConsumeEvent) => void = () => {};
const conns: ConnectionProfile[] = [
  { ...newProfile(), id: "dev", name: "dev-1", environment: "dev" },
  { ...newProfile(), id: "ro", name: "ro-1", environment: "dev", readOnly: true },
];

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      suggestGroupId: vi.fn().mockResolvedValue("kafka-explorer-qa-abc123"),
      startConsumer: vi.fn(async (_p: unknown, cb: (e: ConsumeEvent) => void) => {
        emit = cb;
        return "sid";
      }),
      stopSession: vi.fn().mockResolvedValue(true),
      commitOffsets: vi.fn().mockResolvedValue([{ topic: "orders", partition: 0, offset: 5 }]),
      pausePartitions: vi.fn().mockResolvedValue(undefined),
      resumePartitions: vi.fn().mockResolvedValue(undefined),
    },
  };
});

import { api } from "../../api/client";
import { ConsumerPanel } from "./ConsumerPanel";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listConnections).mockResolvedValue(conns);
});

describe("ConsumerPanel", () => {
  it("запуск, назначение, пауза и коммит всего прочитанного", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConsumerPanel connectionId="dev" topic="orders" />);
    expect(await screen.findByDisplayValue("kafka-explorer-qa-abc123")).toBeInTheDocument();
    expect(screen.getByText(/влияет на кластер/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Подключиться к группе" }));
    expect(vi.mocked(api.startConsumer).mock.calls[0][0]).toMatchObject({ groupId: "kafka-explorer-qa-abc123", topics: ["orders"] });
    act(() => emit({ type: "rebalance", record: { kind: "assigned", partitions: [{ topic: "orders", partition: 0 }], at: 0 } }));
    await user.click(screen.getByRole("button", { name: "Пауза orders:0" }));
    expect(api.pausePartitions).toHaveBeenCalledWith("sid", [{ topic: "orders", partition: 0 }]);
    await user.click(screen.getByRole("button", { name: "Закоммитить всё прочитанное" }));
    expect(api.commitOffsets).toHaveBeenCalledWith("sid", { type: "allConsumed" }, null);
    expect(await screen.findByText(/Закоммичено: orders:0 → 5/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отключиться" }));
    expect(api.stopSession).toHaveBeenCalledWith("sid");
  });

  it("read-only: сессии недоступны", async () => {
    renderWithClient(<ConsumerPanel connectionId="ro" />);
    expect(await screen.findByText(/недоступны для подключения только для чтения/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Подключиться к группе" })).not.toBeInTheDocument();
  });
});
```

- [ ] **Step 10: `ConsumerPanel.tsx`**:

```tsx
import { useEffect, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { BrowsedMessage, CommitTarget, DataFormat, TopicPartitionRef } from "../../api/types";
import { formatCount } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import { FilterFields } from "../browse/FilterFields";
import { MessageDetails } from "../browse/MessageDetails";
import { messageKey, MessageTable } from "../browse/MessageTable";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { AssignmentPanel } from "./AssignmentPanel";
import { buildConsumerParams, type ConsumerFormState, defaultConsumerForm } from "./form";
import { partitionKey } from "./state";
import { useConsumerSession } from "./useConsumerSession";

const FORMATS: DataFormat[] = ["auto", "string", "json", "hex", "base64"];

type Pending = { kind: "start" } | { kind: "commit"; target: CommitTarget } | null;

export function ConsumerPanel({ connectionId, topic, groupId }: { connectionId: string; topic?: string; groupId?: string }) {
  const connection = useConnection(connectionId);
  const [form, setForm] = useState<ConsumerFormState>(() => defaultConsumerForm({ topic, groupId }));
  const [formError, setFormError] = useState<string | null>(null);
  const [pending, setPending] = useState<Pending>(null);
  const [selected, setSelected] = useState<BrowsedMessage | null>(null);
  const [utc, setUtc] = useState(false);
  const { state, start, stop } = useConsumerSession(form.limit);
  const running = state.status === "running";
  const set = (patch: Partial<ConsumerFormState>) => setForm((f) => ({ ...f, ...patch }));

  // Сгенерированный group.id, если группа не задана.
  useEffect(() => {
    if (groupId) return;
    let alive = true;
    void api.suggestGroupId().then((id) => {
      if (alive) setForm((f) => (f.groupId ? f : { ...f, groupId: id }));
    });
    return () => {
      alive = false;
    };
  }, [groupId]);

  const commit = useMutation({
    mutationFn: ({ target, confirm }: { target: CommitTarget; confirm: string | null }) =>
      api.commitOffsets(state.sessionId as string, target, confirm),
    onSettled: () => setPending(null),
  });
  const control = useMutation({
    mutationFn: ({ pause, p }: { pause: boolean; p: TopicPartitionRef }) =>
      pause ? api.pausePartitions(state.sessionId as string, [p]) : api.resumePartitions(state.sessionId as string, [p]),
  });

  if (!connection) return <p className="empty">Загрузка…</p>;
  if (connection.readOnly) {
    return <p className="warn page">Consumer-сессии недоступны для подключения только для чтения: вход в группу меняет её состояние.</p>;
  }

  const launch = (confirm: string | null) => {
    try {
      const params = buildConsumerParams(form, connectionId);
      setFormError(null);
      setSelected(null);
      setPending(null);
      void start(params, confirm);
    } catch (e) {
      setFormError((e as Error).message);
    }
  };
  const requestCommit = (target: CommitTarget) =>
    needsConfirmation(connection) ? setPending({ kind: "commit", target }) : commit.mutate({ target, confirm: null });

  return (
    <div className="page browse">
      <p className="warn">
        Сессия вступает в группу и влияет на кластер: у других участников группы произойдёт ребаланс, коммиты меняют оффсеты
        группы.
      </p>
      <div className="browse-form">
        <fieldset>
          <legend>Группа и топики</legend>
          <label className="field">
            <span>group.id</span>
            <input aria-label="group.id" value={form.groupId} disabled={running} onChange={(e) => set({ groupId: e.target.value })} />
          </label>
          <label>
            <input type="checkbox" checked={form.useRegex} disabled={running} onChange={(e) => set({ useRegex: e.target.checked })} /> regex
          </label>
          {form.useRegex ? (
            <input aria-label="Регулярное выражение топиков" placeholder="orders\..*" value={form.pattern} disabled={running} onChange={(e) => set({ pattern: e.target.value })} />
          ) : (
            <input aria-label="Топики" placeholder="topic-a, topic-b" value={form.topics} disabled={running} onChange={(e) => set({ topics: e.target.value })} />
          )}
          <select aria-label="auto.offset.reset" value={form.autoOffsetReset} disabled={running} onChange={(e) => set({ autoOffsetReset: e.target.value as ConsumerFormState["autoOffsetReset"] })}>
            <option value="latest">с конца (latest)</option>
            <option value="earliest">с начала (earliest)</option>
          </select>
          <label>
            <input type="checkbox" checked={form.enableAutoCommit} disabled={running} onChange={(e) => set({ enableAutoCommit: e.target.checked })} /> автокоммит
          </label>
          <select aria-label="isolation.level" value={form.isolationLevel} disabled={running} onChange={(e) => set({ isolationLevel: e.target.value as ConsumerFormState["isolationLevel"] })}>
            <option value="readCommitted">read_committed</option>
            <option value="readUncommitted">read_uncommitted</option>
          </select>
        </fieldset>
        <fieldset>
          <legend>Формат</legend>
          <select aria-label="Формат ключа" value={form.keyFormat} onChange={(e) => set({ keyFormat: e.target.value as DataFormat })}>
            {FORMATS.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
          <select aria-label="Формат значения" value={form.valueFormat} onChange={(e) => set({ valueFormat: e.target.value as DataFormat })}>
            {FORMATS.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
          <label>
            Буфер <input type="number" min={1} max={100000} value={form.limit} disabled={running} onChange={(e) => set({ limit: Number(e.target.value) })} style={{ width: 90 }} />
          </label>
        </fieldset>
        <FilterFields value={form} onChange={setForm} />
      </div>
      {pending ? (
        <ConfirmDialog
          connection={connection}
          title={pending.kind === "start" ? `Подключиться к группе «${form.groupId}»?` : "Закоммитить оффсеты?"}
          confirmLabel="Подтвердить"
          busy={commit.isPending}
          onConfirm={(c) => (pending.kind === "start" ? launch(c.confirm) : commit.mutate({ target: pending.target, confirm: c.confirm }))}
          onCancel={() => setPending(null)}
        />
      ) : (
        <div className="toolbar">
          <button type="button" className="primary" disabled={running} onClick={() => (needsConfirmation(connection) ? setPending({ kind: "start" }) : launch(null))}>
            Подключиться к группе
          </button>
          <button type="button" disabled={!running} onClick={() => void stop()}>
            Отключиться
          </button>
          <button
            type="button"
            disabled={!running || !selected || !state.sessionId}
            onClick={() => selected && requestCommit({ type: "selected", topic: selected.topic, partition: selected.partition, offset: selected.offset })}
          >
            Закоммитить до выбранного
          </button>
          <button type="button" disabled={!running || !state.sessionId} onClick={() => requestCommit({ type: "allConsumed" })}>
            Закоммитить всё прочитанное
          </button>
          <label>
            <input type="checkbox" checked={utc} onChange={(e) => setUtc(e.target.checked)} /> UTC
          </label>
          <span className="muted">
            прочитано {formatCount(state.consumed)} · найдено {formatCount(state.matched)} · в таблице {state.messages.length}
            {running && (state.reconnecting ? ` · переподключение: ${state.reconnecting}…` : " · в группе")}
            {state.status === "finished" && " · отключено"}
          </span>
        </div>
      )}
      {formError && <ErrorBanner error={{ code: "INVALID_INPUT", message: formError, retryable: false }} />}
      {state.error && <ErrorBanner error={state.error} />}
      {commit.error && <ErrorBanner error={commit.error} />}
      {control.error && <ErrorBanner error={control.error} />}
      {commit.data && (
        <p role="status">Закоммичено: {commit.data.map((o) => `${partitionKey(o)} → ${o.offset}`).join(", ")}</p>
      )}
      <div className="browse-body">
        <MessageTable messages={state.messages} selectedKey={selected ? messageKey(selected) : null} onSelect={setSelected} utc={utc} />
        <div className="details-pane">
          <AssignmentPanel
            assignment={state.assignment}
            paused={state.paused}
            log={state.log}
            disabled={!running || !state.sessionId}
            onPause={(p) => control.mutate({ pause: true, p })}
            onResume={(p) => control.mutate({ pause: false, p })}
          />
          <MessageDetails message={selected} utc={utc} connectionId={connectionId} />
        </div>
      </div>
    </div>
  );
}
```
`partitionKey` принимает `TopicPartitionRef`; `OffsetTarget` содержит `topic` и `partition`, поэтому подходит структурно.

- [ ] **Step 11: Вкладка и точки входа**

`src/state/workspace.ts` — в `TabSpec`:
```ts
  | { kind: "consumer"; connectionId: string; key: string; topic?: string; groupId?: string }
```
в `tabId`:
```ts
    case "consumer":
      return `consumer:${spec.connectionId}:${spec.key}`;
```
и хелпер:
```ts
/** Новая вкладка consumer-сессии (каждый вызов — отдельная сессия). */
export const newConsumerKey = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
```

`TabContent.tsx`:
```tsx
    case "consumer":
      return <ConsumerPanel connectionId={tab.connectionId} topic={tab.topic} groupId={tab.groupId} />;
```
`Layout.tsx`, в `tabTitle`:
```tsx
    case "consumer":
      return `${conn} · Сессия ${tab.groupId ?? tab.topic ?? ""}`.trim();
```
`Sidebar.tsx`, после «Producer»:
```tsx
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "consumer", connectionId: c.id, key: newConsumerKey() })}>
                  Consumer-сессия
                </button>
              </li>
```
`TopicDetails.tsx` — рядом с «Отправить сообщение»:
```tsx
        <button type="button" onClick={() => open({ kind: "consumer", connectionId, key: newConsumerKey(), topic })}>
          Читать как группа
        </button>
```
`GroupDetails.tsx` — в первом `toolbar` после «Обновить» (топики — из закоммиченных партиций группы):
```tsx
        <button
          type="button"
          onClick={() =>
            open({
              kind: "consumer",
              connectionId,
              key: newConsumerKey(),
              groupId,
              topic: [...new Set(rows.map((r) => r.topic))].join(", "),
            })
          }
        >
          Читать как участник
        </button>
```
с `const open = useWorkspace((s) => s.open);` и импортом `newConsumerKey`.

В `src/styles.css`:
```css
.assignment ul { list-style: none; padding: 0; margin: 0 0 8px; }
```

- [ ] **Step 12: Тесты проходят**

Run: `pnpm vitest run && pnpm build`
Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add src
git commit -m "feat(ui): consumer-сессии — назначения, ребалансы, коммит, pause/resume"
```

---

### Task 19: README, полная проверка и ручной смоук M2b

**Files:**
- Modify: `README.md`

- [ ] **Step 1: README** — в раздел «Где хранятся данные» добавить:
```markdown
- Шаблоны сообщений: `templates.json`; история отправки — `history/<id подключения>.json` (последние 100, значения до 64 КБ).
```
и после раздела «Защита от изменений» (из M2a):
````markdown
## Отправка и чтение как группа
- **Producer**: форматы string / json / hex / base64 / null, заголовки, acks / сжатие / идемпотентность. Плейсхолдеры: `{{uuid}}`, `{{now}}`, `{{nowMillis}}`, `{{seq}}`, `{{randomInt 1 100}}`, `{{randomString 8}}`, `{{env NAME}}`.
- **Пакетная отправка**: N копий формы или файл JSON Lines (`{"key":…, "value":…, "headers":{…}, "partition":…}` в строке) / CSV (колонки `key,value,partition,header.<имя>`).
- **Consumer-сессия** вступает в группу (ребаланс у других участников!). Автокоммит по умолчанию выключен; коммит — вручную кнопками.

Проверить коммиты сессии:
```bash
docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group>
```
````

- [ ] **Step 2: Полный прогон**

Run:
```bash
docker compose -f dev/docker-compose.yml up -d
pnpm test && pnpm build && \
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && \
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --lib --tests
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -c '^it-'
```
Expected: всё зелёное; последняя команда печатает `0`.

- [ ] **Step 3: Ручной смоук в `pnpm tauri dev`**

Подготовка: подключение `local` (`dev`), топик `m2b` с 2 партициями (`docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic m2b --partitions 2`).

1. Дерево → «Producer»: топик `m2b` (автодополнение), значение JSON `{"id":"{{uuid}}","n":{{seq}}}`, заголовок `trace=t1` → «Отправить» → «Отправлено: партиция …, offset 0». В истории — запись с подставленным uuid. Значение `{oops` в формате json → ошибка `INVALID_INPUT` у отправки, в Kafka ничего не ушло.
2. «Сохранить как шаблон» `заказ` → очистить форму → выбрать шаблон → форма восстановлена; перезапуск приложения — шаблон на месте.
3. Пакетная отправка: 1000 копий, 200 сообщ./с → прогресс растёт, «Отменить» через пару секунд → «отменено», отправлено < 1000. Без ограничения скорости 1000 копий → «отправлено 1000 из 1000 · готово».
4. Файл JSON Lines из 3 строк (одна битая) → «отправлено 2 · ошибок 1», в списке ошибок «строка 2: невалидный JSON…». CSV `key,value,header.src` → сообщения с заголовком `src`.
5. Топик `m2b` → «Сообщения» → «Читать» → выбрать сообщение → «Отправить повторно» → открылась вкладка Producer с тем же значением и заголовками → «Отправить» → новое сообщение в топике.
6. Топик `m2b` → «Читать как группа» → group.id сгенерирован (`kafka-explorer-<user>-xxxxxx`), «с начала» → «Подключиться к группе» → назначены `m2b:0`, `m2b:1`, сообщения в таблице. Во второй вкладке (ещё раз «Читать как группа», тот же group.id) → в обеих вкладках журнал ребалансов: отозваны/назначены, у каждой по одной партиции.
7. Выбрать сообщение → «Закоммитить до выбранного» → «Закоммичено: m2b:N → offset+1»; `kafka-consumer-groups --describe` показывает этот оффсет. «Закоммитить всё прочитанное» → оффсеты = концу прочитанного.
8. «пауза» у партиции → новые сообщения (отправить через Producer) в неё не приходят; «продолжить» → приходят.
9. «Отключиться» в обеих вкладках → группа в «Consumer groups» переходит в `Empty`; закрытие вкладки во время сессии тоже выводит её из группы.
10. prod-подключение без read-only: «Отправить», «Запустить», «Подключиться к группе», коммит — каждый раз поле «Имя подключения». read-only: Producer показывает «только для чтения», consumer-сессия — «недоступны».
11. Остановить Kafka во время сессии → статус «переподключение…», запуск Kafka в течение минуты → сессия продолжает работу.

Каждую найденную проблему фиксировать отдельным коммитом с тестом, который её воспроизводит.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: Producer, пакетная отправка и consumer-сессии в README"
```

---

## Что сознательно не входит в M2b

- M3: Schema Registry — форматы Avro / Protobuf / JSON Schema при отправке и чтении, выбор subject и версии, валидация по схеме.
- M4: command palette и горячая клавиша Producer (`Cmd+Shift+P`), экспорт сообщений, локализация, восстановление вкладок, остальной бэклог M1 (UI-мелочи, a11y, парсер фильтров, `plan_latest`, дубль vite).
