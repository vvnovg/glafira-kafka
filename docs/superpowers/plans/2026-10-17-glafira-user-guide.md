# D «Руководство пользователя» Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Руководство пользователя на 8 языках: вкладка «Справка» в приложении (офлайн, F1, меню Help на macOS) и те же главы в репозитории релизов.

**Architecture:** Главы — Markdown в `docs/guide/<код>/<глава>.md` с метками `[[ui:ключ]]` (подставляется значение словаря) и якорями `{#id}`. Фронтенд лениво загружает главы текущего языка (`import.meta.glob ?raw`), разбирает разделы, подставляет метки и рендерит `react-markdown`. `scripts/i18n-check.mjs` проверяет структуру, метки и ссылки во всех языках; `scripts/guide-publish.mjs` собирает Markdown для GitHub, workflow релиза кладёт его в `vvnovg/glafira-kafka-releases/guide/`.

**Tech Stack:** React 19, react-i18next, `react-markdown` 9, Vite `import.meta.glob`, `@tauri-apps/api/menu`, `@tauri-apps/plugin-opener`, Vitest, node:test.

**Спецификация:** `docs/superpowers/specs/2026-10-17-glafira-user-guide-design.md`.

## Global Constraints

- Языки: en, ru, de, fr, es, it, pl, pt-BR — у каждого одинаковый набор глав: `getting-started`, `connections`, `topics`, `browse`, `producer`, `groups`, `consumer-sessions`, `license-updates`, `troubleshooting`.
- Глава начинается с `# Заголовок`; разделы — `## Заголовок {#id}`, id латиницей в kebab-case, одинаковые во всех языках (списки якорей — в задачах 4–7).
- Элемент интерфейса — только меткой `[[ui:ключ]]`, ключ без параметров `{{…}}`; нужного ключа нет — добавить его во все 8 словарей.
- Ссылки между главами — `[текст](глава.md#якорь)`; внешние — только `https://`; сырого HTML в главах нет.
- Значения, команды, пути — в `` `code` ``; стиль, обращение, термины — по `docs/i18n/glossary.md`; en — эталон, ru — эталон смысла, Pro-языки — перевод en.
- Пока главы Pro-языков не переведены (задачи 8–9), в их файлах лежит английский текст с теми же якорями — структура проверяется с самого начала.
- Справка без интернета; внешние ссылки — через `@tauri-apps/plugin-opener`.
- Меню Help — только macOS; на Windows и Linux строка меню не добавляется.
- Проверки после каждой задачи: `pnpm test`, `pnpm test:scripts`, `pnpm i18n:check`, `pnpm build`; если менялся Rust или конфиг Tauri — `cargo test --lib`, clippy, fmt (из `src-tauri`, `CARGO_INCREMENTAL=0`).
- Коммиты — на русском в стиле репозитория, заканчиваются строкой атрибуции модели исполнителя. Push только с согласия пользователя.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `docs/guide/<код>/*.md` | главы |
| `src/guide/chapters.ts` | список глав, загрузка глав языка (`import.meta.glob`) |
| `src/guide/parse.ts` (+ `.test.ts`) | разбор главы: заголовок, разделы, якоря, подстановка меток, поиск |
| `src/components/help/HelpPanel.tsx` (+ `.test.tsx`) | вкладка «Справка»: оглавление, текст, поиск, ссылки |
| `src/components/help/helpChapterFor.ts` | вкладка → глава для F1 |
| `src/lib/useHelpShortcut.ts` | F1 |
| `src/lib/useAppMenu.ts` (+ `.test.ts`) | меню Help на macOS |
| `scripts/i18n-check.mjs` (+ `.test.mjs`) | проверки глав |
| `scripts/guide-publish.mjs` (+ `.test.mjs`) | сборка Markdown для GitHub |
| `.github/workflows/release.yml` | публикация `guide/` в репозиторий релизов |
| `docs/smoke/guide.md` | смоук |

---

### Task 1: Проверки глав и заготовки

**Files:**
- Modify: `scripts/i18n-check.mjs`, `scripts/i18n-check.test.mjs`
- Create: `docs/guide/<код>/<глава>.md` × 8 языков × 9 глав (заготовки)

**Interfaces:**
- Produces (экспорт `scripts/i18n-check.mjs`):
  - `GUIDE_CHAPTERS = ["getting-started","connections","topics","browse","producer","groups","consumer-sessions","license-updates","troubleshooting"]`;
  - `parseGuideChapter(text: string): { title: string | null; anchors: string[]; labels: string[]; links: { chapter: string; anchor: string | null }[]; problems: string[] }` — `problems`: нет `# ` в первой непустой строке, якорь не kebab-case, сырой HTML (`<` + буква), ссылка не `https://` и не `*.md[#…]`;
  - `checkGuide(files: Record<string /*"<код>/<глава>.md"*/, string>, codes: string[], enFlat: Map<string,string>): string[]` — набор глав у каждого языка = `GUIDE_CHAPTERS`; якоря главы одинаковы во всех языках (как в en); каждая метка — ключ en без `{{`; ссылка ведёт на существующую главу и якорь (в главах того же языка).
  - В блоке запуска: читать `docs/guide/**/*.md`, `problems.push(...checkGuide(...))`.

- [ ] **Step 1: Падающие тесты** в `scripts/i18n-check.test.mjs`:

```js
test("руководство: структура, метки, ссылки", () => {
  const en = flatten({ sidebar: { settings: "Settings", conn: "Connection {{name}}" } });
  const ok = "# Start\n\nOpen [[ui:sidebar.settings]].\n\n## Install {#install}\n\nSee [mTLS](connections.md#mtls).\n";
  const conn = "# Connections\n\n## mTLS {#mtls}\n\nText.\n";
  const files = { "en/getting-started.md": ok, "en/connections.md": conn, "ru/getting-started.md": ok, "ru/connections.md": conn };
  const p = (f) => checkGuide(f, ["en", "ru"], en, ["getting-started", "connections"]);
  assert.deepEqual(p(files), []);
  assert.ok(p({ ...files, "ru/connections.md": "# Подключения\n\n## mTLS {#m-tls}\n" }).some((x) => x.includes("ru/connections.md") && x.includes("mtls")));
  assert.ok(p({ ...files, "en/getting-started.md": ok.replace("sidebar.settings", "sidebar.nope") }).some((x) => x.includes("sidebar.nope")));
  assert.ok(p({ ...files, "en/getting-started.md": ok.replace("sidebar.settings", "sidebar.conn") }).some((x) => x.includes("sidebar.conn")));
  assert.ok(p({ ...files, "en/getting-started.md": ok.replace("#mtls", "#nope") }).some((x) => x.includes("#nope")));
  assert.ok(p({ "en/getting-started.md": ok, "en/connections.md": conn, "ru/getting-started.md": ok }).some((x) => x.includes("ru/connections.md")));
  assert.ok(p({ ...files, "en/connections.md": "No title\n" }).some((x) => x.includes("# ")));
  assert.ok(p({ ...files, "en/connections.md": conn + "<b>x</b>\n" }).some((x) => x.includes("HTML")));
  assert.ok(p({ ...files, "en/connections.md": conn + "[x](http://a.b)\n" }).some((x) => x.includes("http://")));
});
```

(Четвёртый параметр `chapters` — для тестов; по умолчанию `GUIDE_CHAPTERS`.) Добавить импорт `checkGuide`.

- [ ] **Step 2: Тесты падают** — `pnpm test:scripts` → нет экспорта `checkGuide`.
- [ ] **Step 3: Реализовать** `parseGuideChapter`/`checkGuide` и вызов в блоке запуска (код в стиле существующих проверок файла: сообщения на русском, путь файла в начале).
- [ ] **Step 4: Заготовки глав.** Для каждого языка и главы — файл с одной строкой заголовка `# <название>` (названия en: Getting started, Connections, Cluster and topics, Browsing messages, Producer, Consumer groups, Consumer sessions (Pro), License and updates, Troubleshooting; ru: Начало работы, Подключения, Кластер и топики, Просмотр сообщений, Producer, Consumer groups, Consumer-сессии (Pro), Лицензия и обновления, Решение проблем; в Pro-языках — пока английские названия).
- [ ] **Step 5: Проверки** — `pnpm test:scripts`, `pnpm i18n:check` (✓), `pnpm test`, `pnpm build`.
- [ ] **Step 6: Commit** — `feat(guide): проверки глав руководства и заготовки`.

### Task 2: Вкладка «Справка», разбор глав, F1

**Files:**
- Create: `src/guide/chapters.ts`, `src/guide/parse.ts` (+ `parse.test.ts`), `src/components/help/HelpPanel.tsx` (+ `HelpPanel.test.tsx`), `src/components/help/helpChapterFor.ts`, `src/lib/useHelpShortcut.ts`
- Modify: `package.json` (`react-markdown@^9`), `src/state/workspace.ts` (вкладка `{ kind: "help"; chapter?: string; anchor?: string }`), `src/components/TabContent.tsx`, `src/components/Layout.tsx` (F1), `src/components/Sidebar.tsx` (пункт «Справка»), заголовок вкладки, `src/styles.css`, словари

**Interfaces:**
- Produces:
  ```ts
  // src/guide/chapters.ts
  export const GUIDE_CHAPTERS = ["getting-started","connections","topics","browse","producer","groups","consumer-sessions","license-updates","troubleshooting"] as const;
  export type ChapterId = (typeof GUIDE_CHAPTERS)[number];
  export function loadGuide(lang: string): Promise<Record<ChapterId, string>>; // главы языка; язык без глав (x-pseudo) → en
  // src/guide/parse.ts
  export interface GuideSection { id: string | null; title: string; markdown: string } // id null — вступление до первого ##
  export interface GuideChapter { id: ChapterId; title: string; sections: GuideSection[] }
  export function parseChapter(id: ChapterId, text: string, label: (key: string) => string): GuideChapter; // метки → **label(key)**, якоря из `{#id}`
  export function searchGuide(chapters: GuideChapter[], query: string): { chapter: ChapterId; section: string | null; title: string; snippet: string }[];
  // src/components/help/helpChapterFor.ts
  export function helpChapterFor(tab: TabSpec | null): ChapterId; // таблица §3 спеки
  ```
- `HelpPanel`: оглавление (главы, внутри — разделы с id), текст текущей главы через `react-markdown` (`skipHtml`), заголовки `##` с `id`; поле поиска (`help.search`), результаты «глава › раздел»; ссылка `x.md#y` → открыть главу `x` и прокрутить к `y`; `https://` → `openUrl` из `@tauri-apps/plugin-opener`; прочие — без перехода. Перерисовка при смене языка (`useTranslation`).
- F1 (`useHelpShortcut` в `Layout`): `keydown` `F1` с `preventDefault` → `open({ kind: "help", chapter: helpChapterFor(activeTab) })`.
- Пункт «Справка» в подвале левой панели перед «О программе».

Тексты (8 словарей): `sidebar.help` «Help» / «Справка»; `tabs.help` «Help» / «Справка»; `help.search` «Search the guide» / «Поиск по руководству»; `help.noResults` «Nothing found» / «Ничего не найдено»; `help.contents` «Contents» / «Содержание»; `help.loadFailed` «Could not load the guide» / «Не удалось загрузить руководство».

- [ ] **Step 1: Падающие тесты.** `parse.test.ts`: разделы и якоря; `[[ui:sidebar.settings]]` → `**Настройки**` (через `label`); текст до первого `##` — раздел с `id: null`; `searchGuide` без учёта регистра находит раздел по тексту после подстановки меток и возвращает фрагмент. `helpChapterFor`: по одному случаю на строку таблицы спеки. `HelpPanel.test.tsx` (мок `loadGuide` с двумя короткими главами): оглавление; клик по ссылке `connections.md#mtls` открывает главу и элемент с `id="mtls"`; ссылка `https://example.com` вызывает `openUrl`; поиск показывает результат и «Ничего не найдено»; смена языка (`i18n.changeLanguage("en")` → ru) перерисовывает метку. Тест F1: в `Layout` нажатие F1 при активной вкладке Producer открывает вкладку справки с `chapter: "producer"`.
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать.** `loadGuide`: `const modules = import.meta.glob("../../docs/guide/*/*.md", { query: "?raw", import: "default" })` — ленивые; выбрать ключи `/<lang>/`, загрузить параллельно. Проверить, что Vite видит каталог `docs` (вне `src`): при необходимости `server.fs.allow` в `vite.config.ts`.
- [ ] **Step 4: Проверки.**
- [ ] **Step 5: Commit** — `feat(guide): вкладка «Справка», поиск и F1`.

### Task 3: Меню Help на macOS

**Files:**
- Create: `src/lib/useAppMenu.ts` (+ `useAppMenu.test.ts`)
- Modify: `src/App.tsx` (вызов хука), `src-tauri/capabilities/default.json` (права меню, если `core:default` их не покрывает), словари

**Interfaces:**
- Consumes: `@tauri-apps/api/menu` (`Menu`, `Submenu`, `MenuItem`, `PredefinedMenuItem`), `@tauri-apps/plugin-os` или `navigator.userAgent` для определения macOS (использовать то, что уже есть в проекте; иначе `navigator.platform`/`userAgent` содержит `Mac`).
- Поведение: только на macOS; строит меню приложения: стандартные подменю (App, File/Edit/View/Window — как в меню по умолчанию Tauri: `Menu.default()`, если доступно в установленной версии API; иначе собрать предопределёнными пунктами) + подменю `Help` (`menu.help`) с пунктами «Руководство Glafira» (`menu.guide`, акселератор `CmdOrCtrl+?`), «Решение проблем» (`menu.troubleshooting`), разделитель, «О программе» (`menu.about`), «Собрать диагностику…» (`menu.diagnostics`). Действия: открыть вкладку справки (главу `troubleshooting`), открыть «О программе», открыть «О программе» и запустить сбор диагностики (через существующую функцию/событие экрана «О программе»; если удобного входа нет — открыть «О программе»). `setAsAppMenu()`; перестраивать при смене языка (`i18n.on("languageChanged")`), снимать подписку при размонтировании.

Тексты (8 словарей): `menu.help` «Help» / «Справка»; `menu.guide` «Glafira Guide» / «Руководство Glafira»; `menu.troubleshooting` «Troubleshooting» / «Решение проблем»; `menu.about` «About Glafira» / «О программе»; `menu.diagnostics` «Collect Diagnostics…» / «Собрать диагностику…».

- [ ] **Step 1: Падающие тесты** (мок `@tauri-apps/api/menu`): на macOS строится подменю Help с подписями ru; после `changeLanguage("en")` меню перестроено с английскими подписями; клик «Руководство» открывает вкладку справки; вне macOS меню не строится.
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать**; права в `capabilities/default.json` — по документации установленной версии (`core:menu:default` и т. п.), проверить запуском `pnpm tauri dev` не требуется — достаточно сборки, но указать в отчёте, что пункт нужно проверить в смоуке.
- [ ] **Step 4: Проверки** (включая cargo, если менялся конфиг).
- [ ] **Step 5: Commit** — `feat(guide): меню Help на macOS`.

### Задачи 4–7: тексты en и ru

Общие правила для каждой задачи:
- Писать en (эталон) и ru (по смыслу то же, естественным русским, как в интерфейсе) для своих глав; в файлы de, fr, es, it, pl, pt-BR тех же глав скопировать английский текст (переводы — задачи 8–9).
- Перед текстом изучить экраны в коде (`src/components/...`) и словари, чтобы описывать реальное поведение; все названия элементов — метками `[[ui:…]]`; если ключа с нужной подписью нет (или он с параметрами) — не писать название текстом, а перефразировать или добавить ключ во все 8 словарей.
- Разделы — ровно с перечисленными якорями (порядок как в списке); внутри раздела — 1–6 абзацев или список шагов; короткие предложения, без маркетинга.
- Факты о Kafka — точные (подсказки `hints.*` и `hints.topicConfig.*` в словарях — хороший источник формулировок).
- Проверки: `pnpm i18n:check` (метки, якоря, ссылки), `pnpm test`.

### Task 4: Главы «Начало работы» и «Подключения»

**Files:** `docs/guide/*/getting-started.md`, `docs/guide/*/connections.md` (+ словари при необходимости)

Якоря `getting-started`: `install-macos`, `install-windows`, `install-linux`, `first-run`, `local-cluster`, `language`, `managed-settings`.
Якоря `connections`: `create`, `test`, `security`, `sasl`, `tls`, `mtls`, `certificate-formats`, `passwords`, `environments`, `read-only`, `folders`, `export`, `import`.

Источники: `README.md` (установка, стенд, сертификаты), `docs/releases-repo/README.md` (снятие карантина — команда `xattr -cr`), `docs/admin.md` (что может задать организация), `ConnectionForm.tsx`, `TlsSection.tsx`, дерево папок и `ImportDialog.tsx`.

- [ ] **Step 1: Написать en**, **Step 2: ru**, **Step 3: скопировать en в Pro-языки**, **Step 4: проверки**, **Step 5: Commit** — `docs(guide): главы «Начало работы» и «Подключения» (en, ru)`.

### Task 5: Главы «Кластер и топики» и «Просмотр сообщений»

**Files:** `docs/guide/*/topics.md`, `docs/guide/*/browse.md`

Якоря `topics`: `cluster-overview`, `topic-list`, `create-topic`, `delete-topics`, `topic-details`, `topic-config`, `edit-config`, `config-parameters`.
Якоря `browse`: `start-reading`, `start-position`, `filters`, `formats`, `message-details`, `export-messages`, `open-in-producer`, `reset-group-here`.

Источники: `src/components/cluster`, `src/components/topics`, `src/components/common/ConfigEditor.tsx`, `src/components/browse`.

- [ ] Шаги 1–5 как в задаче 4; Commit — `docs(guide): главы «Кластер и топики» и «Просмотр сообщений» (en, ru)`.

### Task 6: Главы «Producer», «Consumer groups», «Consumer-сессии»

**Files:** `docs/guide/*/producer.md`, `docs/guide/*/groups.md`, `docs/guide/*/consumer-sessions.md`

Якоря `producer`: `send-message`, `key-value-formats`, `headers`, `placeholders`, `templates`, `history`, `batch-copies`, `batch-file`, `batch-rate`, `producer-settings`.
Якоря `groups`: `group-list`, `lag`, `group-details`, `reset-offsets`, `reset-strategies`, `reset-scope`, `delete-group`.
Якоря `consumer-sessions`: `what-is`, `start-session`, `reading`, `commits`, `pause-partitions`, `rebalance-log`, `limits`.

Источники: `src/components/producer`, `src/components/groups`, `src/components/consumer`; список плейсхолдеров — `ProducerPanel.tsx` (в главе плейсхолдеры писать в `code`, без двойных фигурных скобок вне `code`).

- [ ] Шаги 1–5; Commit — `docs(guide): главы «Producer», «Consumer groups», «Consumer-сессии» (en, ru)`.

### Task 7: Главы «Лицензия и обновления» и «Решение проблем»

**Files:** `docs/guide/*/license-updates.md`, `docs/guide/*/troubleshooting.md`

Якоря `license-updates`: `free-and-pro`, `activate`, `expiry`, `updates`, `about`, `diagnostics`.
Якоря `troubleshooting`: `cannot-connect`, `dns-tcp`, `tls-errors`, `sasl-errors`, `timeouts`, `read-only-blocked`, `logs`, `support`.

Источники: `src/components/license`, `src/components/about`, тексты ошибок `errors.kafka.*`, `errors.tls.*`, `errors.guard.*` в словарях (цитировать ошибки метками `[[ui:errors.…]]`, если ключ без параметров; иначе — пересказом), пути логов из `README.md`.

- [ ] Шаги 1–5; Commit — `docs(guide): главы «Лицензия и обновления» и «Решение проблем» (en, ru)`.

### Задачи 8–9: переводы

Общие правила: перевести каждую главу с en (сверяя смысл с ru) по `docs/i18n/glossary.md` — обращение, термины, кавычки, французские U+00A0 перед `: ; ! ?` и внутри « »; якоря `{#…}`, метки `[[ui:…]]`, ссылки, `code` — без изменений; заголовки — на языке перевода. Проверки: `pnpm i18n:check`, `pnpm test`; отчёт — перечень терминов, которых нет в глоссарии (добавить в глоссарий, если они повторяются).

### Task 8: Переводы de, fr, es

**Files:** `docs/guide/de/*.md`, `docs/guide/fr/*.md`, `docs/guide/es/*.md`, `docs/i18n/glossary.md` (при необходимости)

- [ ] Перевести 9 глав на каждый язык; проверки; Commit — `docs(guide): переводы руководства на de, fr, es`.

### Task 9: Переводы it, pl, pt-BR

**Files:** `docs/guide/it/*.md`, `docs/guide/pl/*.md`, `docs/guide/pt-BR/*.md`, `docs/i18n/glossary.md` (при необходимости)

- [ ] Перевести 9 глав на каждый язык; проверки; Commit — `docs(guide): переводы руководства на it, pl, pt-BR`.

### Task 10: Публикация и смоук

**Files:**
- Create: `scripts/guide-publish.mjs` (+ `scripts/guide-publish.test.mjs`), `docs/smoke/guide.md`
- Modify: `package.json` (`"guide:publish": "node scripts/guide-publish.mjs dist/guide"`, тесты скрипта — в `test:scripts`), `.github/workflows/release.yml`, `docs/releases-repo/README.md`, `docs/i18n/README.md` (раздел «Руководство»)

**Interfaces:**
- `export function publishChapter(text: string, labels: Map<string, string>): string` — метки → `**значение**`; `## Заголовок {#id}` → `<a id="id"></a>\n\n## Заголовок`; ссылки `x.md#y` без изменений.
- `export function publishGuide(root: string, outDir: string): void` — для каждого языка `outDir/<код>/*.md` и `outDir/README.md` (таблица языков — самоназвания из `src/i18n/languages.ts` — и ссылки на главы по `# ` заголовкам en).

- [ ] **Step 1: Падающие тесты** `guide-publish.test.mjs`: метки подставлены значением языка; якоря преобразованы; README содержит все 8 языков; в выходных файлах нет `[[ui:` и `{#`.
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать скрипт.**
- [ ] **Step 4: Workflow.** В job `publish` перед шагом публикации: checkout исходников (если job их не имеет), Node + `pnpm install --frozen-lockfile` (или запуск скрипта без зависимостей — он должен использовать только `node:` модули), `node scripts/guide-publish.mjs guide-out`; затем (только не в dry-run) клонировать `vvnovg/glafira-kafka-releases` с `RELEASES_TOKEN`, заменить каталог `guide/` на `guide-out`, закоммитить «guide: $TAG» и запушить, если есть изменения. В `docs/releases-repo/README.md` — строка «Руководство: [English](guide/en/getting-started.md) · [Русский](guide/ru/getting-started.md) · …» для всех 8 языков.
- [ ] **Step 5: `docs/smoke/guide.md`:**

```markdown
# Смоук D (руководство)

`pnpm tauri dev`.

1. Левая панель → «Справка»: открылась вкладка, слева оглавление 9 глав, справа «Начало работы».
2. Ссылка на другую главу внутри текста — открывает её на нужном разделе; внешняя ссылка — в системном браузере.
3. Поиск «mtls» — раздел «mTLS» в главе «Подключения»; поиск «zzzz» — «Ничего не найдено».
4. F1 на вкладках Producer, топика, группы, формы подключения — открывается соответствующая глава.
5. macOS: меню Help → «Руководство Glafira» (⌘?), «Решение проблем», «О программе», «Собрать диагностику…»; поиск в меню Help находит пункты.
6. Переключить язык на English, Deutsch (Pro), Polski — справка и меню Help на выбранном языке, названия кнопок в тексте совпадают с интерфейсом.
7. Выключить сеть — справка открывается.
8. `node scripts/guide-publish.mjs /tmp/guide && open /tmp/guide/README.md` — Markdown без `[[ui:` и `{#`, ссылки между главами работают на GitHub (проверяется после релиза в репозитории релизов).
```

- [ ] **Step 6: Полный прогон** — все проверки; `pnpm guide:publish` локально без ошибок.
- [ ] **Step 7: Commit** — `feat(guide): публикация руководства в репозиторий релизов и смоук`.

---

## После плана

- Финальное ревью ветки: точность глав относительно реального поведения, единообразие терминов во всех языках, метки и ссылки, вкладка, F1, меню, публикация.
- Ручной смоук пользователем по `docs/smoke/guide.md`.
- Выборочная проверка носителями — по `docs/i18n/review-checklist.md` (дополнить пунктом «руководство»).
