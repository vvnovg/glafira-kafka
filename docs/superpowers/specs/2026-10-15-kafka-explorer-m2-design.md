# Kafka Explorer — M2 «Запись и группы»: дизайн

**Дата:** 2026-10-15
**Статус:** согласован
**Основная спецификация:** `docs/superpowers/specs/2026-10-14-kafka-explorer-design.md` (§4.3, §4.4, §4.6, §4.7, §5)
**Предыдущий этап:** `docs/superpowers/plans/2026-10-14-kafka-explorer-m1.md`

Документ фиксирует решения, принятые при планировании M2 поверх основной спецификации. Где он молчит — действует основная спецификация.

---

## 1. Разбиение на планы

M2 реализуется двумя последовательными планами, у каждого — своя ветка и финальное ревью:

| План | Файл | Содержимое |
|---|---|---|
| **M2a «Защита, топики, группы»** | `docs/superpowers/plans/2026-10-15-kafka-explorer-m2a.md` | guard мутаций (read-only, prod), операции с топиками, колонка `cleanup.policy`, consumer groups и сброс оффсетов, подвкладка «Consumer groups» у топика, «Сбросить группу сюда»; бэклог «хранилище и пул», «инфраструктура IT» |
| **M2b «Producer и consumer-сессии»** | `docs/superpowers/plans/2026-10-15-kafka-explorer-m2b.md` | одиночная и пакетная отправка, шаблоны с плейсхолдерами, история, consumer-сессии, «Отправить повторно»; бэклог «общий код чтения» |

M2b начинается от `main` со слитым M2a и использует его guard, `ConfirmDialog` и `atomic_write`.

## 2. Решения

### 2.1 Admin API через FFI

В rust-rdkafka 0.37 нет `IncrementalAlterConfigs`, `ListConsumerGroups`, `DescribeConsumerGroups`, `ListConsumerGroupOffsets`, `AlterConsumerGroupOffsets` (есть только устаревший `alter_configs`, который **заменяет весь набор** конфигов ресурса, и `fetch_group_list`). В librdkafka 2.12 (`rdkafka-sys 4.10`) эти функции есть.

Решение: модуль `src-tauri/src/kafka/admin_ext.rs` — **весь новый `unsafe`-код M2 только здесь** (кроме него `unsafe` есть лишь в `kafka/cluster.rs::native_cluster_info` из M1):
- C API берётся из реэкспорта `rdkafka::bindings` (как в `cluster.rs`), новых зависимостей нет;
- клиент берётся из `AdminClient::inner().native_ptr()`, у каждого вызова — своя очередь `rd_kafka_queue_new`, ожидание результата — блокирующий `rd_kafka_queue_poll` с таймаутом запроса; вызывается только из blocking-контекста;
- указатели librdkafka (очередь, событие, AdminOptions, массивы аргументов) освобождаются RAII-обёртками в `Drop`;
- наружу — только безопасные функции с Rust-типами;
- покрытие — интеграционными тестами против dev-Kafka.

Safe-API rdkafka используется там, где он есть: `create_topics`, `create_partitions`, `delete_records`, `delete_topics`, `delete_groups`, `describe_configs`.

### 2.2 Защита мутаций в Rust

`src-tauri/src/guard.rs`:
```
ensure_mutation_allowed(profile, confirm: Option<&str>) -> AppResult<()>
ensure_name_confirmed(expected: &str, typed: Option<&str>) -> AppResult<()>
```
- `profile.read_only` → `READ_ONLY` (никакой confirm не помогает);
- `environment == prod` и `confirm != profile.name` → `CONFIRMATION_REQUIRED`;
- удаление топиков, удаление группы и purge дополнительно требуют ввод имени объекта (`ensure_name_confirmed`; при удалении нескольких топиков — их число).

Каждая мутирующая команда первой строкой вызывает guard. Мутирующие: `create_topic`, `alter_topic_configs`, `add_partitions`, `purge_topic`, `delete_topics`, `reset_offsets`, `delete_group`, `produce`, `produce_batch`, `start_consumer`, `commit_offsets`. **Consumer-сессии в read-only запрещены целиком** (вход в группу вызывает ребаланс). `preview_offset_reset` — не мутирующая.

Новые коды `AppError`: `READ_ONLY`, `CONFIRMATION_REQUIRED`, `GROUP_NOT_EMPTY`.

Фронтенд: общий `ConfirmDialog` (для prod — поле «имя подключения», для удаления/purge — «имя объекта»); для read-only кнопки изменений скрыты (`useConnection(id).readOnly`).

### 2.3 Отложенное из M1

- В M2a: колонка `cleanup.policy` в списке топиков (пакетный `DescribeConfigs` по видимым чанкам).
- **Размер топика на диске — вне MVP:** librdkafka не поддерживает `DescribeLogDirs`.
- Rack брокеров и режим KRaft/ZK, настраиваемый таймаут и шаги TLS/SASL в Test connection, IT для SSL — в M4.

### 2.4 Бэклог M1, включённый в M2

- **M2a, хранилище и пул:** `storage/fs.rs::atomic_write` (tmp → fsync → rename) для всех JSON-файлов; мьютексы — `lock_or_recover` без паники на отравлении; пул создаёт клиента вне мьютекса с double-check; `save_connection` удаляет секрет SASL при переходе на протокол без SASL и откатывает профиль, если запись в keychain не удалась.
- **M2a, инфраструктура IT:** `wait_for_topic` вместо паузы 500 мс, guard `TopicCleanup` удаляет топик в `Drop`, хелпер для удаления групп.
- **M2b, общий код чтения:** инкрементальная обрезка буфера в редьюсере сообщений, усечение payload до копирования/кодирования в декодере, неблокирующая проба восстановления (`RecoveryMonitor` выносится в общий модуль `kafka/recovery.rs`).

Остальной бэклог (UI-мелочи, a11y, парсер фильтров, `plan_latest`, дубль vite) — M4.

## 3. M2a: топики и группы

### 3.1 Операции с топиками

| Операция | Команда | Реализация | Проверки в Rust |
|---|---|---|---|
| Создать | `create_topic(connectionId, spec, confirm)` | `create_topics` | имя: 1–249 символов `[a-zA-Z0-9._-]`, не `.`/`..`; партиции ≥ 1; RF ≥ 1 (или `-1` — по умолчанию брокера) |
| Изменить конфиги | `alter_topic_configs(connectionId, topic, changes[{name, op: set|delete, value?}], confirm)` | `IncrementalAlterConfigs` (FFI) | непустой список, без дублей имён |
| Добавить партиции | `add_partitions(connectionId, topic, newTotal, confirm)` | `create_partitions` | `newTotal > текущего` |
| Purge | `purge_topic(connectionId, topic, partitions?, confirm, typedName)` | `delete_records` до high watermark | имя топика введено; партиции существуют |
| Удалить | `delete_topics(connectionId, topics[], confirm, typedConfirmation)` | `delete_topics` | для одного — имя, для нескольких — их число; результат по каждому топику |
| `cleanup.policy` | `get_topic_policies(connectionId, topics[])` | пакетный `describe_configs` | — |

UI: кнопка «Создать топик» в списке и режим выбора нескольких топиков для удаления; в деталях топика — меню действий (добавить партиции, purge, удалить); подвкладка «Конфигурация» получает режим редактирования с diff «было → стало» и «сбросить к default» (DELETE). Read-only и sensitive-параметры не редактируются. Предпросмотр `kafka-topics --create …` — чистая TS-функция. Пресеты: `delete`, `compact`, `compact,delete`, «тестовый» (`retention.ms=3600000`). Purge для `compact`-топиков показывает предупреждение.

### 3.2 Consumer groups

| Команда | Реализация |
|---|---|
| `list_groups(connectionId)` → `{groups: [{groupId, state, isSimple}], errors: [строка]}` (errors — частичные ошибки отдельных брокеров) | ListConsumerGroups (FFI) |
| `describe_groups(connectionId, groupIds[])` → `[{groupId, state, assignor, members[{memberId, clientId, host, assignment[{topic, partition}]}], error?}]` | DescribeConsumerGroups (FFI) |
| `get_group_offsets(connectionId, groupId)` → `[{topic, partition, committed?, logEnd?, lag?, memberClientId?, error?}]` | ListConsumerGroupOffsets (FFI, все партиции) + `fetch_bounds` |
| `get_groups_lag(connectionId, groupIds[])` → `[{groupId, totalLag?, topics[]}]` | то же для чанка групп |
| `get_topic_groups(connectionId, topic)` → группы с коммитами в топик и их лаг | список групп → offsets каждой (общий дедлайн = таймаут запроса) |
| `preview_offset_reset(connectionId, request)` → `[{topic, partition, current?, target, low, high}]` | `plan_reset` (чистая функция) |
| `reset_offsets(connectionId, groupId, offsets[{topic, partition, offset}], confirm)` | проверка `Empty` → AlterConsumerGroupOffsets (FFI) |
| `delete_group(connectionId, groupId, confirm, typedName)` | проверка `Empty` → `delete_groups` |

Правила:
- лаг = `logEnd − committed`, `None`, если хоть одно из них неизвестно; ошибка никогда не превращается в 0; партиция без коммита — «нет коммита»;
- `plan_reset`: earliest → low, latest → high, offset → clamp [low, high], shift ±N → `committed + N` с clamp (без коммита — ошибка партиции в плане), timestamp → `offsets_for_times`, без сообщений новее метки → high;
- `reset_offsets` применяет **ровно показанный план** (передаётся обратно с фронтенда); перед применением — повторная проверка `Empty`, иначе `GROUP_NOT_EMPTY` со списком участников в `details`;
- список групп: id и состояние — сразу; участники, assignor и лаг — лениво для видимых чанков по 50 групп; ошибка группы — «—» с причиной в tooltip;
- детали группы: автообновление 5 с / 10 с / выкл.

UI: узел «Consumer groups» в дереве → вкладка `groups`; вкладка `group` (партиции, участники, сброс, удаление); подвкладка «Consumer groups» в деталях топика; действие «Сбросить группу сюда» в деталях сообщения Browse открывает вкладку группы с предзаполненным сбросом (стратегия offset, область — партиция сообщения, группа выбирается из списка).

## 4. M2b: Producer и consumer-сессии

### 4.1 Producer

- Вкладка `producer` на подключение (дерево, детали топика, «Отправить повторно»).
- `produce(connectionId, message, settings, confirm)` → `{partition, offset, timestamp}`; ожидание доставки не дольше таймаута запроса.
- Форматы key/value в M2: `string`, `json` (валидируется в Rust), `hex`, `base64`, `null`; заголовки — string или hex. SR-форматы — M3.
- Настройки: `acks` (0/1/all), compression (none/gzip/snappy/lz4/zstd), idempotence. Producer'ы кэшируются в пуле по ключу «подключение + настройки», не больше 4 на подключение; вытесняются вместе с подключением.
- Плейсхолдеры рендерятся в Rust (`produce/template.rs`) в key, value и значениях заголовков: `{{uuid}}`, `{{now}}`, `{{nowMillis}}`, `{{seq}}`, `{{randomInt min max}}`, `{{randomString n}}`, `{{env NAME}}`. Неизвестный плейсхолдер или неверные аргументы → `INVALID_INPUT` до отправки.
- Шаблоны: `templates.json` (`schemaVersion`), уровень подключения или глобальный (`connectionId: null`); `list_templates`, `save_template`, `delete_template`.
- Пакетная отправка `produce_batch(params, channel)` → `SessionId`: источник — N копий формы (`{{seq}}` = 0..N−1) или файл JSON Lines / CSV (колонки `key,value,partition`, заголовки — `header.<имя>`); файл выбирается через `tauri-plugin-dialog`, Rust читает его потоково по пути; лимит скорости (сообщ./с) опционален; прогресс раз в 100 мс (отправлено, ошибок, первые 20 ошибок с номером строки); отмена — `stop_session`. Сессия живёт в общем `SessionRegistry`.
- История: `history/<conn-id>.json`, последние 100 успешно отправленных (после подстановки плейсхолдеров); значения больше 64 КБ сохраняются усечёнными с флагом `truncated`, такие записи повторить нельзя. Повтор — заполняет форму.
- «Отправить повторно» в деталях Browse открывает Producer с key, value, headers сообщения (текстом; бинарное — hex).

### 4.2 Consumer-сессии

- `start_consumer(params, channel, confirm)` → `SessionId`; параметры: group.id (вручную или `kafka-explorer-<user>-<6 hex>`), топики (список) или regex-подписка (`^…`), `auto.offset.reset`, `enable.auto.commit` (по умолчанию выкл.), `isolation.level`, форматы и фильтры как в Browse. Перед запуском — баннер «влияет на кластер».
- Поток сессии владеет `BaseConsumer` с `subscribe`; контекст сообщает `assigned` / `revoked` (с временем) через канал событий.
- `commit_offsets(sessionId, mode: selected{partition, offset} | allConsumed, confirm)`, `pause_partitions`, `resume_partitions` передаются в поток сессии через mpsc-канал команд (consumer не `Sync`), ответ — через одноразовый канал с таймаутом.
- Коммит «до выбранного» = `offset + 1` его партиции; «всё прочитанное» = позиции по всем назначенным партициям.
- Остановка: `unsubscribe` + `close` (корректный выход из группы).
- Переподключение — общий `kafka/recovery.rs` (вынесен из browse).
- UI: те же `MessageTable` и `MessageDetails`, панель Assignment (партиции, pause/resume, журнал ребалансов), кнопки коммита; несколько сессий — в разных вкладках.

## 5. Тестирование

- **Unit (Rust):** guard, валидация имени топика, `plan_reset`, расчёт лага, плейсхолдеры, парсинг JSONL/CSV, ограничитель скорости, хранилища шаблонов и истории, `atomic_write`.
- **IT (Rust, dev-Kafka):** цикл топика (create → alter → add partitions → purge → delete); группы (коммиты → list/describe/offsets/lag → preview/reset → delete; активная группа → `GROUP_NOT_EMPTY`); produce → browse; пакет 1000 сообщений с отменой; две consumer-сессии в одной группе (ребаланс, ручной коммит); read-only — каждая мутирующая команда возвращает `READ_ONLY`, prod без confirm — `CONFIRMATION_REQUIRED`.
- **Фронтенд (Vitest):** `ConfirmDialog`, редактор конфигов с diff, форма создания топика, форма сброса с предпросмотром, форма Producer, панель consumer-сессии.
