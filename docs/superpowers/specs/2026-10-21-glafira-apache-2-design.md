# Glafira for Apache Kafka — этап «Открытый код»: Apache 2.0, без ключей, «Поддержать проект»

**Дата:** 2026-10-21
**Зачем:** код открывается под Apache License 2.0, приложение остаётся бесплатным целиком и живёт на пожертвования.
Подсистема лицензионных ключей теряет смысл и удаляется, а не отключается: в публичный репозиторий мёртвый код не
попадает. Корпоративная политика администратора остаётся — это условие допуска инструмента в организацию, а не
платная функция. Публикация репозитория, секреты Apple и релиз v0.3.0 идут после этого этапа отдельными шагами.

## 1. Решения

| Вопрос | Решение |
|---|---|
| Лицензия кода | Apache License 2.0: `LICENSE`, `NOTICE`, `license = "Apache-2.0"` в `src-tauri/Cargo.toml`, `crates/glafira-mcp/Cargo.toml` и `package.json`. Имя Glafira и значок лицензией не покрываются (`TRADEMARK.md`) |
| Правообладатель | `NOTICE` и `bundle.copyright` в `tauri.conf.json`: «© 2026 Viacheslav Novgorodtsev» (сейчас там имя продукта) |
| Функции | Все функции доступны всем: consumer-сессии, prod-подключения, снятие read-only с prod, импорт prod без принудительного read-only, замечания LLM через MCP |
| Подсистема ключей | Удаляется целиком: модуль `license`, крейты `license-format` и `issue-license`, экран «Лицензия», замки `ProLock`, код ошибки `ProRequired`, встроенный публичный ключ и его проверка при сборке |
| Политика администратора | Остаётся: `updatesEnabled`, `updateUrl`, `language`, `mcpEnabled`. Ключ `licenseKey` удаляется из схемы; в уже развёрнутых политиках он идёт по существующей ветке «неизвестный ключ»: предупреждение в лог, не ошибка политики |
| Поддержка проекта | Один адрес `SUPPORT_URL` в `src/lib/links.ts`; три входа: ссылка внизу левой панели на месте «Лицензии», кнопка в «О программе», пункт меню Help на macOS. Страница — `SUPPORT.md` в репозитории релизов, источник `docs/releases-repo/SUPPORT.md`, публикует release.yml |
| «О программе» | Строка «План: Free/Pro» заменяется строкой «Лицензия: Apache 2.0» со ссылкой на `LICENSE` в GitHub; блок политики и её ошибок там уже есть и заменяет блок политики с экрана «Лицензия» |
| Руководство | Глава `license-updates` → `updates` «Обновления и диагностика» на 12 языках; разделы про Free/Pro, активацию и срок удаляются, добавляется раздел про поддержку проекта |
| Хранилище | `license.json` в каталоге данных не мигрирует и удаляется при запуске, если остался от 0.2.x |
| Вклад и безопасность | `CONTRIBUTING.md` с DCO (Apache 2.0 §5 покрывает входящие изменения, CLA не нужен); `SECURITY.md` — приватные сообщения об уязвимостях через GitHub, без личной почты |
| Лицензии зависимостей | `deny.toml` в корне с белым списком из проверки 2026-10-21 и шаг `cargo deny check licenses` в CI; `THIRD_PARTY.md` генерирует `scripts/third-party.mjs` |
| Версия | В ветке остаётся 0.2.0; `CHANGELOG.md` получает раздел 0.3.0; номер поднимается при релизе по чек-листу |

## 2. Что удаляется

| Область | Файлы и сущности |
|---|---|
| Rust | `src-tauri/src/license/` (весь модуль, включая `pro_for_tests`); `commands/license.rs` — кроме `get_policy_status`, которая переезжает в `commands/settings.rs`; `ErrorCode::ProRequired` и строка `PRO_REQUIRED`; поле `AppState.license`; `tracing::info!(... "license")` в `lib.rs`; `KEY_LICENSE_KEY`, `Policy.license_key`, `PolicyStatus.has_license_key` |
| Крейты и инструменты | `crates/license-format`, `tools/issue-license`, `src-tauri/license-public.key`, проверка ключа в `src-tauri/build.rs`, зависимости `license-format` (обычная и `dev-keys`) в `src-tauri/Cargo.toml` |
| Фронтенд | `components/license/` (`LicensePanel`, `LicenseNotice`, тесты), `common/ProLock.tsx`, `lib/useLicense.ts`, `ProGating.test.tsx`; вкладка `license` в `state/workspace.ts`, `TabContent`, `Layout`, `helpChapterFor`; типы `Tier`, `LicenseState`, `LicenseStatus`, `PolicyStatus.hasLicenseKey` и вызовы `getLicenseStatus`/`setLicenseKey`/`removeLicenseKey` в `api/` |
| MCP | `Backend::pro_active` в `crates/glafira-mcp/src/model.rs`, проверка и константа `PRO_REQUIRED` в `server/health_tools.rs`, тесты `pro_required` в `tests/server.rs` |
| Словари | Все ключи, содержащие `license` (44 в `en.json`), а также `consumer.proOnly`, `connections.form.prodPro`, `about.tier`, `warnings.import.prodReadOnly_one/_other` |
| Документация | `docs/licensing.md`; разделы «Free и Pro», «Активация ключа», «Срок действия» главы руководства; шаги про ключи лицензий и `KE_ALLOW_DEV_LICENSE_KEY` в `docs/release-checklist.md` |
| CI и скрипты | В `ci.yml` — шаги для `license-format` и `issue-license` и охрана `license-signing.key`; в `release.yml` — `KE_ALLOW_DEV_LICENSE_KEY`; в `scripts/check-release.mjs` — сравнение с dev-ключом; в `.gitignore` — `license-signing.key`, `tools/issue-license/target`, `crates/license-format/target` |

Прежние спеки в `docs/superpowers/specs/` не правятся: это история проектирования, упоминания Pro там остаются.

## 3. Что меняется в коде

### 3.1 Rust

- **Команды.** `consume.rs`: четыре вызова `require_pro` (старт сессии, коммит, пауза, возобновление) удаляются.
  `connections.rs`: удаляются проверка «перевод в prod и снятие read-only — Pro» и комментарий к ней; `was_prod` /
  `becomes_prod` / `becomes_writable` исчезают, если больше не нужны. `folders.rs` и `storage/exchange.rs`:
  `ImportOptions.is_pro` и `forced_read_only` удаляются, импорт сохраняет `readOnly` как в файле; код предупреждения
  `prodReadOnly` уходит из превью импорта и из типов фронтенда.
- **Политика.** `policy/mod.rs`: ключ `licenseKey` не разбирается — он попадает в существующую ветку
  `tracing::warn!("unknown policy key … ignored")`. `state.rs`: вызов `set_policy_key` удаляется.
- **Миграция и запуск.** `migrate.rs`: `FILES = ["templates.json"]`. Новая функция `remove_legacy_license_file(data_dir)`
  удаляет `license.json`, если он есть; вызывается при создании `AppState`; ошибка удаления пишется в лог уровня warn
  и не мешает запуску.
- **Диагностика.** `InfoFile { app, policy }`; `write_archive` теряет параметр `license`; `commands/app.rs` передаёт
  на один аргумент меньше.
- **MCP.** `health_tools.rs`: `annotate` и `remove_annotation` работают без проверки. Описания инструментов и
  инструкции сервера в `server/mod.rs` переписываются без «Glafira Pro»: «You can leave notes with annotate…».
- **Манифесты.** `license = "Apache-2.0"` и `repository = "https://github.com/vvnovg/kafka-explorer"` в обоих
  Cargo.toml; `"license": "Apache-2.0"` в `package.json`; `bundle.copyright` в `tauri.conf.json`.

### 3.2 Фронтенд

- **Разблокировка.** `ConsumerPanel`: блок «locked» удаляется. `ConnectionForm`: окружение prod доступно всегда,
  флажок «Только чтение» снимается у любого подключения. `ConnectionNode`, `TopicDetails`, `GroupDetails`: пункты
  consumer-сессии без замка и подсказки. `FindingsPanel`: заголовок замечаний без замка; строка `health.proNotes`
  переименовывается в `health.llmNotes` с нейтральным текстом. `ErrorBanner`: ветка кода `PRO_REQUIRED` удаляется.
- **Ссылки.** Новый `src/lib/links.ts`:
  `SUPPORT_URL = "https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md"`,
  `LICENSE_URL = "https://github.com/vvnovg/kafka-explorer/blob/main/LICENSE"`, функции `openSupport()` и
  `openLicense()` через `@tauri-apps/plugin-opener`. Другие адреса в коде не появляются.
- **Левая панель.** Низ: «Настройки», «Поддержать проект», «Справка», «О программе». Ссылка поддержки — кнопка
  класса `link`, вызывает `openSupport()`.
- **«О программе».** В списке: Версия, Сборка, Система, Лицензия (`Apache 2.0` ссылкой на `LICENSE_URL`), Политика.
  После списка абзац: кнопка «Поддержать проект» и подсказка `support.hint`. Строка «План» удаляется.
  `UpdateNotice` использует `about.notice.more` и `about.notice.hideAria` вместо ключей из `license.notice`.
- **Меню Help (macOS, `useAppMenu.ts`).** Порядок: Руководство, Устранение неполадок, разделитель, Поддержать
  проект, О Glafira, Собрать диагностику. Подпись — `support.title`.

## 4. Поддержка проекта

- **`docs/releases-repo/SUPPORT.md`** на русском и английском: что это за проект, что приложение бесплатное и открытое,
  куда идут деньги (сертификат Apple, время на поддержку), два способа — GitHub Sponsors `https://github.com/sponsors/vvnovg`
  и способ в рублях. Адрес рублёвого способа даёт владелец при выполнении плана; без него задача не закрывается.
- **`release.yml`** копирует `SUPPORT.md` в репозиторий релизов рядом с README и включает его в тот же коммит
  `guide: $TAG` (`git add -A guide README.md SUPPORT.md`).
- **`.github/FUNDING.yml`** в этом репозитории: `github: vvnovg` и `custom:` со `SUPPORT_URL`. Такой же файл в
  репозитории релизов владелец кладёт сам — это другой репозиторий.

## 5. Локализация

- **Удаляются** ключи из §2 во всех 12 файлах `src/locales/*.json` скриптом по списку из `en.json`.
- **Добавляются:** `support.title` («Поддержать проект»), `support.hint` (одна фраза: приложение бесплатное и открытое,
  пожертвования помогают его развивать), `about.license` («Лицензия»), `about.notice.more`, `about.notice.hideAria`
  (переносятся из `license.notice`), `health.llmNotes`.
- **Переписываются:** `about.summary` — последняя фраза «Pro adds…» заменяется перечислением тех же возможностей как
  обычных; `hints.connections.environment` — без «(Pro)»; подпись окружения prod в форме подключения — как у остальных
  окружений, без пометки.
- Новые и изменённые строки переводятся на 12 языков по `docs/i18n/glossary.md`; из глоссария уходят термины «Pro»
  и «Free» и примеры «prod (Pro)». Тест паритета ключей (`locales.test.ts`) остаётся страховкой от пропусков.

## 6. Руководство

- **Переименование.** `docs/guide/<lang>/license-updates.md` → `updates.md` во всех 12 языках; `ChapterId` в
  `src/guide/chapters.ts`, `helpChapterFor` (вкладка «О программе» → `updates`), список глав в
  `scripts/guide-publish.mjs`, ссылки `license-updates.md#updates` и `#diagnostics` из других глав.
- **Содержание главы.** Остаются «Обновления», «О программе», «Архив диагностики»; добавляется «Поддержать проект»
  (три входа, адрес страницы). Ссылки на `#free-and-pro` и `#activate` удаляются вместе с фразами.
- **Упоминания Pro** вычищаются на всех языках по grep `\bPro\b`: `getting-started` (ссылка на экран «Лицензия»
  в списке низа панели → «Поддержать проект»; абзац про ключ организации), `connections` (prod и read-only доступны
  всем; импорт без принудительного read-only), `consumer-sessions`, `llm-mcp`, `health-check`, `topics`, `groups`.
  После правки grep по `docs/guide` не находит ни `Pro`, ни `license-updates`, ни `tabs.license`.

## 7. Документация и корневые файлы

- **README.md.** Раздел «Free и Pro» → «Лицензия и поддержка проекта»: Apache 2.0, все функции бесплатны, имя и значок
  — товарные знаки (`TRADEMARK.md`), ссылка на `SUPPORT.md`. Правки строк про Pro в разделах MCP и языков.
- **CHANGELOG.md.** Раздел `## 0.3.0`: все функции бесплатны, экран «Лицензия» удалён, код под Apache 2.0, пункт
  «Поддержать проект», `licenseKey` в политике игнорируется, глава руководства «Обновления и диагностика».
- **docs/admin.md.** Строка `licenseKey` → примечание под таблицей: ключ из версий 0.2.x игнорируется с предупреждением
  в логе. **docs/release-checklist.md.** Без шагов про ключи лицензий и dev-ключ; добавлен шаг «обновить
  `THIRD_PARTY.md`».
- **LICENSE** — текст Apache License 2.0 без изменений. **NOTICE** — название, копирайт, ссылка на `THIRD_PARTY.md`.
- **TRADEMARK.md.** Имя «Glafira», «Glafira for Apache Kafka» и значок не входят в лицензию; форки и пересобранные
  сборки распространяются под другим именем и значком; упоминание «совместимо с Glafira» разрешено; «Apache Kafka» —
  товарный знак Apache Software Foundation.
- **THIRD_PARTY.md** и **scripts/third-party.mjs.** Скрипт читает `cargo deny --manifest-path src-tauri/Cargo.toml list
  -f json -l crate` и `pnpm licenses list --prod --json`, пишет таблицы «Rust», «npm» (имя, версия, лицензия) и
  статический раздел «C-библиотеки в сборке»: librdkafka (BSD-2-Clause), zstd (BSD-3-Clause), zlib (Zlib), lz4 внутри
  librdkafka (BSD-2-Clause), OpenSSL 3 (Apache-2.0). Требует установленного cargo-deny; запускается вручную перед
  релизом.
- **CONTRIBUTING.md.** Как собрать (ссылка на README), что строки интерфейса идут сразу на 12 языков, тесты перед PR,
  DCO: `git commit -s`, PR без Signed-off-by не принимается.
- **SECURITY.md.** Поддерживается последняя версия; уязвимости — через «Report a vulnerability» в GitHub, не через
  публичные Issues; ответ в течение недели.
- **deny.toml** в корне: `[licenses] allow = [MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause,
  BSD-3-Clause, ISC, Zlib, 0BSD, CC0-1.0, Unlicense, BSL-1.0, MIT-0, Unicode-DFS-2016, Unicode-3.0, MPL-2.0,
  CDLA-Permissive-2.0]`, `[licenses.private] ignore = true`.
- **CI (`ci.yml`).** Шаг `EmbarkStudios/cargo-deny-action` с `command: check licenses` и `arguments: --config deny.toml`
  для `src-tauri/Cargo.toml` и `crates/glafira-mcp/Cargo.toml`.

## 8. Тесты

- **Rust, удаляются:** тесты модуля `license`, `policy_license_key_reaches_manager`, `archive_does_not_contain_license_token`,
  `consumer_sessions_require_pro`.
- **Rust, переворачиваются:** в `connections.rs` три теста с `ProRequired` → «prod создаётся и становится доступным
  для записи без ключа»; в `exchange.rs` тесты с `is_pro: false` → импорт prod сохраняет `readOnly` из файла; в
  `policy/mod.rs` тест с `licenseKey` → ключ неизвестен и игнорируется; `diagnostics.rs` — `info.json` без `license`;
  `it_consume.rs` — настройка без `LicenseManager`; `glafira-mcp/tests/server.rs` — `annotate` и `remove_annotation`
  успешны без признака Pro. Новый тест `remove_legacy_license_file`: файл есть → удалён; файла нет → без ошибки.
- **Фронтенд:** удаляются `LicensePanel.test.tsx`, `ProGating.test.tsx`; обновляются `AboutPanel.test.tsx` (строка
  лицензии и кнопка поддержки), `FindingsPanel.test.tsx`, `helpChapterFor.test.ts`, `client.test.ts`,
  `useAppMenu.test.ts` (новый пункт и порядок); новый тест: кнопка поддержки в `Sidebar` вызывает `openUrl` с
  `SUPPORT_URL`.
- **Проверки перед финальным ревью:** `pnpm test`, `pnpm build`, `cargo fmt --check`, `cargo clippy --all-targets -D
  warnings` и `cargo test` для `src-tauri` и `crates/glafira-mcp`, `cargo deny check licenses` с `deny.toml`; grep по
  `src/locales`, `docs/guide`, `README.md`, `docs/admin.md` — ни `Pro`, ни `license` (кроме `about.license`,
  `LICENSE_URL` и раздела о лицензии кода).

## 9. Смоук (`docs/smoke/apache-2.md`)

1. Положить в каталог данных `license.json` с произвольным содержимым, запустить приложение: файл удалён, в логе
   запись уровня warn нет.
2. Consumer-сессия из дерева подключения стартует без ключа; коммит, пауза и возобновление работают.
3. Новое подключение с окружением prod сохраняется; у read-only prod-подключения флажок снимается.
4. Импорт файла с prod-подключением без `readOnly`: превью без предупреждения, подключение импортировано доступным для
   записи.
5. Через MCP (Claude Code) вызвать `annotate` — замечание появляется на вкладке «Замечания» без ключа.
6. Три входа «Поддержать проект» (низ панели, «О программе», меню Help на macOS) открывают `SUPPORT_URL` в браузере.
7. «О программе»: строка «Лицензия: Apache 2.0» открывает `LICENSE_URL`; строки «План» нет.
8. Политика с `licenseKey` (plist на macOS по `docs/admin.md`): «О программе» показывает политику без ошибок, в логе
   предупреждение про неизвестный ключ.
9. F1 на «О программе» открывает главу «Обновления и диагностика»; в руководстве нет главы «Лицензия».
10. Переключить язык на два любых из 12: «Поддержать проект» и подсказка переведены, меню Help перестроено.

## 10. Риски

- **Объём правок на 12 языках.** Смягчение: удаления скриптом, паритет ключей тестом, переводы по глоссарию, grep
  в финальном ревью.
- **Переименование главы ломает внешние ссылки** на опубликованное руководство `guide/<lang>/license-updates…`.
  Принимаем: руководство публикуется вместе с релизом, ссылки на него есть только в README репозитория релизов,
  который обновляется тем же коммитом.
- **Политики с `licenseKey` в организациях.** Продаж не было, развёрнутых политик с ключом нет; предупреждение в логе
  достаточно.
- **`THIRD_PARTY.md` устаревает** при обновлении зависимостей. Смягчение: шаг в чек-листе релиза.

## 11. Вне объёма

Перевод репозитория в публичный, секреты `APPLE_*`, релиз v0.3.0 и поднятие версии, `FUNDING.yml` и `SUPPORT.md`
в репозитории релизов до первого релиза (их туда принесёт release.yml), профиль GitHub Sponsors, те же изменения
в glafira-postgres, новые запреты политики (маски read-only/prod, `allowedBootstrapPatterns`), подпись Windows.
