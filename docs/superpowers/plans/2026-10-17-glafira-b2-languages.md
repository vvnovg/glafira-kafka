# B2 «Pro-языки» Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Интерфейс Glafira доступен на немецком, французском, испанском, итальянском, польском и бразильском португальском (Pro). Переводы сделаны по единому глоссарию, проверки CI не пропускают пропущенный ключ, сломанный параметр или тег, забытую непереведённую строку.

**Architecture:** Инфраструктура B1 не меняется: Rust уже знает все восемь кодов, проверяет Pro и отдаёт `effective`. B2 добавляет только данные — словари `src/locales/<code>.json`, — плюс автоматическую загрузку всех словарей во фронтенде (`import.meta.glob`) и новые проверки в `scripts/i18n-check.mjs`. Глоссарий `docs/i18n/glossary.md` — единственный источник решений по терминам и стилю: им пользуются переводчик (Claude) и носитель языка при выборочной проверке.

**Tech Stack:** `i18next` + `react-i18next`, Vite `import.meta.glob`, `Intl.PluralRules`, node:test (скрипты), Vitest (фронтенд).

**Спецификация:** `docs/superpowers/specs/2026-10-16-glafira-localization-design.md` (§1, §3, §5, §6 строка B2). Предыдущий план: `docs/superpowers/plans/2026-10-16-glafira-b1-i18n.md` (выполнен и слит).

## Global Constraints

- Языки и коды (§1 спеки): `de` Deutsch, `fr` Français, `es` Español, `it` Italiano, `pl` Polski, `pt-BR` Português (Brasil) — все Pro. Имена файлов: `src/locales/de.json`, `fr.json`, `es.json`, `it.json`, `pl.json`, `pt-BR.json`.
- `en.json` — эталон ключей и параметров, `ru.json` — эталон смысла (русский текст был исходным). Перевод делается с английского, при сомнении в смысле сверяться с русским.
- Множество ключей во всех словарях одинаковое; плюральные формы — по `Intl.PluralRules` языка (Node 22): `de` — `one`, `other`; `fr`, `es`, `it`, `pt-BR` — `one`, `many`, `other`; `pl` — `one`, `few`, `many`, `other`.
- В каждой строке — тот же набор параметров `{{name}}` и тех же тегов (`<code>`, `</code>`, `<offset/>`, `<total/>`, а также буквальных `<file>`, `<name>`), что в `en.json`. Параметры не переводятся и не склоняются.
- Не переводятся (§3.1 спеки): данные пользователя, ключи и значения конфигов Kafka (`cleanup.policy`, `acks`), команды и пути (`docker compose …`, `keytool …`, `localhost:9092`), названия форматов и протоколов (SASL, TLS, mTLS, PEM, PKCS#12, JKS, JCEKS, CSV, JSON Lines), Kafka, Glafira, Pro, Free. Термины Kafka — по глоссарию.
- Rust не меняется: `message`/`text` ошибок остаются английскими (логи и поддержка).
- Ничего в коде не переводится вручную вне словарей; самоназвания языков уже есть в `src/i18n/languages.ts`.
- Проверки, которые должны проходить после каждой задачи: `pnpm i18n:check`, `pnpm test:scripts`, `pnpm test`, `pnpm build`.
- Коммиты заканчиваются пустой строкой и `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Push только с согласия пользователя.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `docs/i18n/glossary.md` | термины и правила стиля для каждого языка |
| `docs/i18n/review-checklist.md` | чек-лист носителя: экраны, что смотреть, как сообщать о правках |
| `docs/i18n/same-as-en.json` | ключи, где перевод намеренно совпадает с английским (по языкам) |
| `scripts/i18n-check.mjs` (+ `.test.mjs`) | новые проверки: теги, совпадение с en, типографика, языки ↔ словари, отчёт о длине |
| `src/i18n/index.ts` | загрузка всех словарей через `import.meta.glob` |
| `src/i18n/locales.test.ts` | каждый словарь загружается, переключается и даёт плюральные формы |
| `src/locales/{de,fr,es,it,pl,pt-BR}.json` | словари Pro-языков |
| `docs/i18n/README.md`, `docs/smoke/b2.md` | документация и ручной смоук |

---

### Task 1: Глоссарий и чек-лист носителя

**Files:**
- Create: `docs/i18n/glossary.md`
- Create: `docs/i18n/review-checklist.md`

**Interfaces:**
- Produces: `docs/i18n/glossary.md` с разделами «Общие правила», «Не переводится», «Стиль по языкам», «Термины», «Частые слова интерфейса». На него ссылаются задачи 4–9 и `review-checklist.md`.

- [ ] **Step 1: Создать `docs/i18n/glossary.md`** с таким содержимым:

```markdown
# Глоссарий переводов Glafira

Единые решения по терминам и стилю для всех словарей `src/locales/*.json`.
Эталон ключей и параметров — `en.json`, эталон смысла — `ru.json`.
Меняя решение здесь, исправьте его во всех словарях одним коммитом.

## Общие правила
- Коротко и нейтрально, как в системных приложениях. Без восклицаний и шуток.
- Параметры `{{…}}` и теги `<code>…</code>`, `<offset/>`, `<total/>`, `<file>`, `<name>` переносятся без изменений,
  их нельзя переводить, склонять или удалять. Порядок внутри фразы можно менять.
- Значение параметра (имя топика, подключения, группы) не склоняется: ставьте перед ним
  существительное в нужном падеже и берите имя в кавычки языка: pl `topik „{{name}}”`, de `Topic „{{name}}“`.
- Многоточие — один символ `…`, не `...`.
- Тире в паузе — `—` с пробелами (во французском тоже), в диапазонах — `–`.
- Единицы: `ms`, `KB`, `MB` как в en; число и единица разделяются неразрывным пробелом там, где он уже есть в en.
- Плюралы: заполняются все формы языка (см. таблицу в README), даже если формы совпадают по тексту.

## Не переводится
Kafka, Glafira, Pro, Free, SASL, SSL, TLS, mTLS, PEM, PKCS#12, JKS, JCEKS, CSV, JSON, JSON Lines, librdkafka,
DNS, TCP, ID; имена параметров Kafka (`cleanup.policy`, `retention.ms`, `acks`, `enable.idempotence`);
команды, пути, адреса (`docker compose -f dev/docker-compose.yml up -d`, `keytool …`, `localhost:9092`);
название экрана «Producer» (как в ru).

## Стиль по языкам

| Язык | Обращение | Кнопки и пункты меню | Вопрос-подтверждение | Кавычки | Особенности |
|---|---|---|---|---|---|
| de | Sie, по возможности без обращения | инфинитив: Speichern, Löschen | «Topic „{{name}}“ löschen?» | „…“ | составные слова через дефис с англ. терминами: Topic-Name, Consumer-Group-ID |
| fr | vous, по возможности без обращения | инфинитив: Enregistrer, Supprimer | «Supprimer le topic « {{name}} » ?» | « … » с U+00A0 внутри | U+00A0 перед `:` `;` `!` `?` и после `«` / перед `»` |
| es | tú, по возможности без обращения | инфинитив: Guardar, Eliminar | «¿Eliminar el topic “{{name}}”?» | “…” | `¿…?`, `¡…!` |
| it | tu, по возможности без обращения | повелительное как в macOS/Windows: Salva, Elimina | «Eliminare il topic “{{name}}”?» | “…” | артикль перед англ. терминами: il topic, l’offset |
| pl | безличные формы, без «Ty/Pan» | повелительное: Zapisz, Usuń | «Usunąć topik „{{name}}”?» | „…” | значения параметров только в именительном после существительного |
| pt-BR | você, по возможности без обращения | инфинитив: Salvar, Excluir | «Excluir o tópico “{{name}}”?» | “…” | бразильская норма: arquivo, tela, excluir (не ficheiro, ecrã, eliminar) |

## Термины

| en | ru | de | fr | es | it | pl | pt-BR |
|---|---|---|---|---|---|---|---|
| topic | топик | Topic (das, Pl. Topics) | topic (m.) | topic (m.) | topic (m., pl. invar.) | topik (m.: topiku, topiki, topików) | tópico (m.) |
| partition | партиция | Partition (die) | partition (f.) | partición (f.) | partizione (f.) | partycja | partição (f.) |
| offset | оффсет | Offset (der, Pl. Offsets) | offset (m.) | offset (m.) | offset (m., pl. invar.) | offset (m.: offsetu, offsety) | offset (m.) |
| consumer group | consumer group | Consumer Group (die) | consumer group (m.) | consumer group (m.) | consumer group (m.) | grupa konsumentów | consumer group (m.) |
| broker | брокер | Broker (der) | broker (m.) | broker (m.) | broker (m.) | broker (m.: brokera) | broker (m.) |
| producer / consumer (роль) | producer / consumer | Producer / Consumer | producteur / consommateur | productor / consumidor | producer / consumer | producent / konsument | produtor / consumidor |
| lag | лаг | Lag (der) | lag (m.) | lag (m.) | lag (m.) | lag (m.: lagu) | lag (m.) |
| message | сообщение | Nachricht | message (m.) | mensaje | messaggio | wiadomość | mensagem |
| key / value | ключ / значение | Schlüssel / Wert | clé / valeur | clave / valor | chiave / valore | klucz / wartość | chave / valor |
| headers | заголовки | Header (Pl.) | en-têtes | encabezados | intestazioni | nagłówki | cabeçalhos |
| connection | подключение | Verbindung | connexion | conexión | connessione | połączenie | conexão |
| prod connection | prod-подключение | Prod-Verbindung | connexion prod | conexión prod | connessione prod | połączenie prod | conexão prod |
| cluster | кластер | Cluster | cluster | clúster | cluster | klaster | cluster |
| bootstrap server | bootstrap-сервер | Bootstrap-Server | serveur bootstrap | servidor bootstrap | server bootstrap | serwer bootstrap | servidor bootstrap |
| consumer session | consumer-сессия | Consumer-Sitzung | session consumer | sesión de consumer | sessione consumer | sesja konsumenta | sessão de consumer |
| read-only | только чтение | Schreibgeschützt | Lecture seule | Solo lectura | Sola lettura | Tylko do odczytu | Somente leitura |
| reset offsets | сбросить оффсеты | Offsets zurücksetzen | Réinitialiser les offsets | Restablecer offsets | Reimposta gli offset | Zresetuj offsety | Redefinir offsets |
| retention | хранение (retention) | Aufbewahrung | rétention | retención | conservazione | retencja | retenção |
| replication factor | фактор репликации | Replikationsfaktor | facteur de réplication | factor de replicación | fattore di replica | współczynnik replikacji | fator de replicação |
| under-replicated | недореплицированные | unterrepliziert | sous-répliquées | subreplicadas | sotto-replicate | niedoreplikowane | sub-replicadas |
| trusted CA | доверенный CA | vertrauenswürdige CA | CA de confiance | CA de confianza | CA attendibile | zaufany CA | CA confiável |
| client certificate / key | сертификат / ключ клиента | Client-Zertifikat / -Schlüssel | certificat / clé client | certificado / clave de cliente | certificato / chiave client | certyfikat / klucz klienta | certificado / chave do cliente |
| system secret store | системное хранилище секретов | Schlüsselbund des Systems | trousseau du système | almacén de secretos del sistema | archivio dei segreti di sistema | systemowy magazyn sekretów | cofre de segredos do sistema |
| license | лицензия | Lizenz | licence | licencia | licenza | licencja | licença |
| settings | настройки | Einstellungen | Paramètres | Ajustes | Impostazioni | Ustawienia | Configurações |
| about | о программе | Über Glafira | À propos | Acerca de | Informazioni | O programie | Sobre |
| update | обновление | Update | mise à jour | actualización | aggiornamento | aktualizacja | atualização |
| diagnostics archive | архив диагностики | Diagnosearchiv | archive de diagnostic | archivo de diagnóstico | archivio diagnostico | archiwum diagnostyczne | arquivo de diagnóstico |
| set by your organization | задан вашей организацией | von Ihrer Organisation festgelegt | défini par votre organisation | establecido por tu organización | impostato dalla tua organizzazione | ustawione przez organizację | definido pela sua organização |
| System default (…) | Как в системе (…) | Systemsprache ({{name}}) | Langue du système ({{name}}) | Idioma del sistema ({{name}}) | Lingua di sistema ({{name}}) | Język systemu ({{name}}) | Idioma do sistema ({{name}}) |

## Частые слова интерфейса

| en | de | fr | es | it | pl | pt-BR |
|---|---|---|---|---|---|---|
| Save | Speichern | Enregistrer | Guardar | Salva | Zapisz | Salvar |
| Cancel | Abbrechen | Annuler | Cancelar | Annulla | Anuluj | Cancelar |
| Delete | Löschen | Supprimer | Eliminar | Elimina | Usuń | Excluir |
| Close | Schließen | Fermer | Cerrar | Chiudi | Zamknij | Fechar |
| Retry | Erneut versuchen | Réessayer | Reintentar | Riprova | Spróbuj ponownie | Tentar novamente |
| Loading… | Wird geladen… | Chargement… | Cargando… | Caricamento… | Wczytywanie… | Carregando… |
| Details | Details | Détails | Detalles | Dettagli | Szczegóły | Detalhes |
| Apply | Anwenden | Appliquer | Aplicar | Applica | Zastosuj | Aplicar |
| Back | Zurück | Retour | Atrás | Indietro | Wstecz | Voltar |
| Search | Suchen | Rechercher | Buscar | Cerca | Szukaj | Pesquisar |
| Refresh | Aktualisieren | Actualiser | Actualizar | Aggiorna | Odśwież | Atualizar |
| Create | Erstellen | Créer | Crear | Crea | Utwórz | Criar |
| Edit | Bearbeiten | Modifier | Editar | Modifica | Edytuj | Editar |
| Send | Senden | Envoyer | Enviar | Invia | Wyślij | Enviar |
| Copy | Kopieren | Copier | Copiar | Copia | Kopiuj | Copiar |
| Export | Exportieren | Exporter | Exportar | Esporta | Eksportuj | Exportar |
| Import | Importieren | Importer | Importar | Importa | Importuj | Importar |
```

- [ ] **Step 2: Создать `docs/i18n/review-checklist.md`** с таким содержимым:

```markdown
# Чек-лист носителя языка

Для выборочной проверки Pro-языка перед выпуском. Термины и стиль — в `glossary.md`;
если решение глоссария кажется неверным, предложите правку глоссария, а не отдельной строки.

## Подготовка
- Сборка с Pro-лицензией (ключ — `docs/licensing.md`), «Настройки» → язык → проверяемый язык.
- Kafka для примеров: `docker compose -f dev/docker-compose.yml up -d`, подключение `localhost:9092`.
- Окно 1400 px (по умолчанию) и минимальное 900 px.

## Экраны
1. Левая панель, пустое состояние (подсказка с командой docker), «Настройки», «Лицензия», «О программе».
2. Форма подключения: все виды безопасности (PLAINTEXT, SASL, mTLS), шаги «Проверить подключение», ошибки полей.
3. Обзор кластера, список топиков (фильтры, колонки), создание и удаление топика (одного и нескольких), конфигурация топика.
4. Consumer groups: список, детали, сброс оффсетов (1, 2, 5, 21 партиция), удаление.
5. Просмотр сообщений: фильтры, детали сообщения, экспорт.
6. Producer: одиночная отправка, пакетная из файла, прогресс, ошибки.
7. Consumer-сессии.
8. Диалоги подтверждения (prod-подключение — ввод имени), баннеры ошибок (подключение к `localhost:1`).

## Что смотреть
- Смысл совпадает с английским (и русским) текстом; нет буквального перевода, звучит как системное приложение.
- Термины — по глоссарию, одинаково на всех экранах.
- Склонения и числа: 1, 2, 5, 21, 1 000 000 элементов.
- Текст не обрезается и не ломает вёрстку на 1400 px и 900 px (кнопки, колонки таблиц, вкладки, левая панель).
- Типографика языка: кавычки, пробелы во французском, `¿` в испанском.

## Как сообщать о правках
Таблица или issue: ключ (виден в `src/locales/<код>.json` поиском по тексту) · сейчас · предлагается · почему.
Скриншот — если проблема в вёрстке.
```

- [ ] **Step 3: Проверить, что Markdown читается** (таблицы ровные, по 8 и 7 колонок):

Run: `awk -F'|' '/^\| (topic|partition|Save|Cancel) /{print NF-2}' docs/i18n/glossary.md`
Expected: `8`, `8`, `7`, `7` (по строке на каждый образец).

- [ ] **Step 4: Commit**

```bash
git add docs/i18n/glossary.md docs/i18n/review-checklist.md
git commit -m "docs(i18n): глоссарий переводов и чек-лист носителя

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Новые проверки в `i18n-check`

**Files:**
- Modify: `scripts/i18n-check.mjs`
- Modify: `scripts/i18n-check.test.mjs`
- Create: `docs/i18n/same-as-en.json`

**Interfaces:**
- Consumes: существующие `flatten`, `placeholders`, `checkDictionaries` из `scripts/i18n-check.mjs`.
- Produces (экспорт из `scripts/i18n-check.mjs`):
  - `tags(text: string): string[]` — отсортированные теги вида `<code>`, `</code>`, `<offset/>`;
  - `checkDictionaries(dicts)` — дополнительно сверяет теги с en;
  - `checkSameAsEn(dicts, allow: Record<string, string[]>): string[]`;
  - `checkTypography(dicts): string[]`;
  - `languageCodes(languagesTs: string): string[]` — коды из `src/i18n/languages.ts`;
  - `checkLanguageFiles(codes: string[], dictCodes: string[], { requireAll: boolean }): string[]`;
  - `lengthReport(dicts, { ratio = 1.6, minLength = 6, top = 15 } = {}): string[]`;
  - флаги командной строки `--require-all-languages` (включается в Task 10) и `--lengths` (отчёт, не падает).
- Produces: `docs/i18n/same-as-en.json` — `{ "<code>": ["ключ", …] }`.

- [ ] **Step 1: Написать падающие тесты** — дописать в конец `scripts/i18n-check.test.mjs` и расширить импорт в начале файла:

```js
// в строке import добавить: checkLanguageFiles, checkSameAsEn, checkTypography, languageCodes, lengthReport, tags
```

```js
test("теги совпадают с en", () => {
  assert.deepEqual(tags("a <code>{{x}}</code> <offset/> <file>"), ["</code>", "<code>", "<file>", "<offset/>"]);
  const en = { a: "Type <code>{{name}}</code>", n_one: "<total/> {{count}}", n_other: "<total/> {{count}}s" };
  const ok = { a: "Tippen <code>{{name}}</code>", n_one: "<total/> {{count}}", n_other: "<total/> {{count}}" };
  assert.deepEqual(checkDictionaries({ en, de: ok }), []);
  const p = checkDictionaries({ en, de: { ...ok, a: "Tippen {{name}}", n_other: "{{count}}" } });
  assert.ok(p.some((x) => x.includes("de: a") && x.includes("<code>")), p.join("\n"));
  assert.ok(p.some((x) => x.includes("de: n_other") && x.includes("<total/>")), p.join("\n"));
});

test("строка совпадает с en без разрешения — ошибка", () => {
  const en = { a: "Save", b: "Kafka", c: "{{message}}", n_one: "{{count}} topic", n_other: "{{count}} topics" };
  const de = { a: "Save", b: "Kafka", c: "{{message}}", n_one: "{{count}} Topic", n_other: "{{count}} topics" };
  const p = checkSameAsEn({ en, de }, { de: ["b"] });
  assert.ok(p.some((x) => x.includes("de: a ")), p.join("\n"));
  assert.ok(!p.some((x) => x.includes("de: b ")), "b разрешён");
  assert.ok(!p.some((x) => x.includes("de: c ")), "без букв вне параметров — не ошибка");
  assert.ok(p.some((x) => x.includes("de: n_other")), "плюральная форма сравнивается с en той же формы или _other");
  const stale = checkSameAsEn({ en, de: { ...de, a: "Speichern", n_other: "{{count}} Topics" } }, { de: ["a", "b"] });
  assert.deepEqual(stale, ["de: a в docs/i18n/same-as-en.json, но перевод отличается от en — уберите из списка"]);
});

test("типографика: многоточие и пробелы во французском", () => {
  const p = checkTypography({
    en: { a: "Loading...", b: "Loading…" },
    fr: { a: "Chargement…", b: "Supprimer ?", c: "Nom : x", d: "« x »", e: "Supprimer ?", f: "« x »", g: "localhost:9092" },
  });
  assert.deepEqual(p.sort(), [
    "en: a — «...» вместо «…»",
    "fr: b — обычный пробел вместо U+00A0 перед «?»",
    "fr: c — обычный пробел вместо U+00A0 перед «:»",
    "fr: d — обычный пробел вместо U+00A0 у кавычек « »",
  ]);
});

test("языки из languages.ts и файлы словарей", () => {
  const ts = 'export const LANGUAGES = [\n  { code: "en", nativeName: "English", pro: false },\n  { code: "pt-BR", nativeName: "Português (Brasil)", pro: true },\n];';
  assert.deepEqual(languageCodes(ts), ["en", "pt-BR"]);
  assert.deepEqual(checkLanguageFiles(["en", "ru", "de"], ["en", "ru"], { requireAll: false }), []);
  assert.deepEqual(checkLanguageFiles(["en", "ru", "de"], ["en", "ru"], { requireAll: true }), ["нет словаря src/locales/de.json"]);
  assert.deepEqual(checkLanguageFiles(["en", "ru"], ["en", "ru", "xx"], { requireAll: false }), ["словарь src/locales/xx.json — языка xx нет в src/i18n/languages.ts"]);
});

test("отчёт о длине: только заметно длиннее en", () => {
  const r = lengthReport({ en: { a: "Delete topic", b: "OK", c: "Save" }, de: { a: "Das Topic endgültig löschen", b: "Einverstanden", c: "Speich" } });
  assert.deepEqual(r, ["de: a — 27 / 12 (×2.3) «Das Topic endgültig löschen»"]);
});
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `pnpm test:scripts`
Expected: FAIL — `SyntaxError: The requested module './i18n-check.mjs' does not provide an export named 'checkLanguageFiles'`.

- [ ] **Step 3: Реализовать.** В `scripts/i18n-check.mjs`:

3a. После `placeholders` добавить:

```js
const TAG = /<\/?[A-Za-z][\w-]*\s*\/?>/g;

/** Теги в строке (`<code>`, `</code>`, `<offset/>`, буквальные `<file>`) — отсортированный список с повторами. */
export function tags(text) {
  return [...text.matchAll(TAG)].map((m) => m[0].replace(/\s+/g, "")).sort();
}
```

3b. В `checkDictionaries` запоминать теги en для основы ключа и сверять их. Заменить блок построения `baseKeys` и проверку параметров:

```js
  const baseKeys = new Map();
  for (const [k, v] of base) {
    const stem = k.replace(PLURAL, "");
    const params = new Set(placeholders(v));
    const prev = baseKeys.get(stem);
    baseKeys.set(stem, {
      plural: PLURAL.test(k),
      params: prev ? new Set([...prev.params, ...params]) : params,
      tags: prev ? prev.tags : tags(v),
    });
  }
```

и внутри цикла `for (const key of keys)` после проверок параметров:

```js
        const gotTags = tags(map.get(key));
        if (gotTags.join(" ") !== info.tags.join(" "))
          problems.push(`${code}: ${key} — теги ${gotTags.join(" ") || "(нет)"}, в en ${info.tags.join(" ") || "(нет)"}`);
```

3c. После `checkDictionaries` добавить:

```js
const hasLetters = (v) => /[A-Za-zÀ-ž]/.test(v.replace(/\{\{[^}]*\}\}|<[^>]*>/g, ""));

/** Строки, совпадающие с en буква в букву (вероятно, забыли перевести). allow: { code: [ключ] } — намеренные совпадения. */
export function checkSameAsEn(dicts, allow = {}) {
  const problems = [];
  const en = flatten(dicts.en);
  const enOf = (k) => en.get(k) ?? en.get(`${k.replace(PLURAL, "")}_other`);
  for (const [code, dict] of Object.entries(dicts)) {
    if (code === "en") continue;
    const map = flatten(dict);
    const ok = new Set(allow[code] ?? []);
    for (const [k, v] of map) {
      if (v === enOf(k) && hasLetters(v) && !ok.has(k))
        problems.push(`${code}: ${k} совпадает с en «${v}» — переведите или добавьте ключ в docs/i18n/same-as-en.json`);
    }
    for (const k of ok) {
      if (map.has(k) && map.get(k) !== enOf(k))
        problems.push(`${code}: ${k} в docs/i18n/same-as-en.json, но перевод отличается от en — уберите из списка`);
    }
  }
  return problems;
}

/** Типографика: «...» вместо «…» во всех языках; во французском — U+00A0 перед : ; ! ? и внутри « ». */
export function checkTypography(dicts) {
  const problems = [];
  for (const [code, dict] of Object.entries(dicts)) {
    for (const [k, v] of flatten(dict)) {
      if (v.includes("...")) problems.push(`${code}: ${k} — «...» вместо «…»`);
      if (code !== "fr") continue;
      for (const ch of new Set([...v.matchAll(/ ([:;!?])/g)].map((m) => m[1])))
        problems.push(`fr: ${k} — обычный пробел вместо U+00A0 перед «${ch}»`);
      if (/« | »/.test(v)) problems.push(`fr: ${k} — обычный пробел вместо U+00A0 у кавычек « »`);
    }
  }
  return problems;
}

/** Коды языков из src/i18n/languages.ts (поля `code: "…"`). */
export function languageCodes(languagesTs) {
  return [...languagesTs.matchAll(/\bcode:\s*"([\w-]+)"/g)].map((m) => m[1]);
}

/** Каждый словарь — язык из списка; с requireAll — у каждого языка из списка есть словарь. */
export function checkLanguageFiles(codes, dictCodes, { requireAll }) {
  const problems = [];
  for (const d of dictCodes) if (!codes.includes(d)) problems.push(`словарь src/locales/${d}.json — языка ${d} нет в src/i18n/languages.ts`);
  if (requireAll) for (const c of codes) if (!dictCodes.includes(c)) problems.push(`нет словаря src/locales/${c}.json`);
  return problems;
}

/** Строки, заметно длиннее английских, — кандидаты на обрезание в вёрстке (для ручной проверки, не ошибка). */
export function lengthReport(dicts, { ratio = 1.6, minLength = 6, top = 15 } = {}) {
  const en = flatten(dicts.en);
  const out = [];
  for (const [code, dict] of Object.entries(dicts)) {
    if (code === "en") continue;
    const rows = [];
    for (const [k, v] of flatten(dict)) {
      const base = en.get(k) ?? en.get(`${k.replace(PLURAL, "")}_other`);
      if (!base || base.length < minLength || v.length / base.length < ratio) continue;
      rows.push({ k, v, r: v.length / base.length, len: base.length });
    }
    rows.sort((a, b) => b.r - a.r);
    for (const { k, v, r, len } of rows.slice(0, top))
      out.push(`${code}: ${k} — ${v.length} / ${len} (×${r.toFixed(1)}) «${v}»`);
  }
  return out;
}
```

3d. В блоке `if (process.argv[1] === …)` после `const problems = checkDictionaries(dicts);` добавить:

```js
  const allowPath = join(root, "docs/i18n/same-as-en.json");
  const allow = JSON.parse(readFileSync(allowPath, "utf8"));
  problems.push(...checkSameAsEn(dicts, allow), ...checkTypography(dicts));
  const codes = languageCodes(readFileSync(join(root, "src/i18n/languages.ts"), "utf8"));
  problems.push(...checkLanguageFiles(codes, Object.keys(dicts), { requireAll: process.argv.includes("--require-all-languages") }));
  if (process.argv.includes("--lengths")) for (const line of lengthReport(dicts)) console.log(`↔ ${line}`);
```

Порядок словарей: `en` должен идти первым в `dicts` не обязательно — все функции берут `dicts.en` по имени.

- [ ] **Step 4: Создать `docs/i18n/same-as-en.json`** с намеренными совпадениями `ru` (их нашёл прогон по текущим словарям):

```json
{
  "ru": [
    "connections.form.prodPro",
    "connections.form.step.tcp",
    "errors.source.csv",
    "groups.list.title",
    "settings.language.proSuffix",
    "sidebar.groups",
    "sidebar.producer",
    "tabs.producer",
    "topics.details.tabGroups",
    "topics.list.colUnderRepl"
  ]
}
```

- [ ] **Step 5: Тесты и проверка проходят**

Run: `pnpm test:scripts && pnpm i18n:check && node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | tail -3`
Expected: все тесты PASS; `✓ словари en, ru согласованы, вызовов из Rust: …`. Если `checkTypography` или `checkSameAsEn` нашли что-то в текущих `en`/`ru`, исправить строку в словаре (например, `...` → `…`), а не ослаблять проверку; `topics.list.colUnderRepl` в ru — сознательное сокращение, оставлено в списке.

- [ ] **Step 6: Commit**

```bash
git add scripts/i18n-check.mjs scripts/i18n-check.test.mjs docs/i18n/same-as-en.json src/locales
git commit -m "feat(i18n): проверки тегов, совпадений с en, типографики и списка языков

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Автоматическая загрузка словарей

**Files:**
- Modify: `src/i18n/index.ts`
- Create: `src/i18n/locales.test.ts`

**Interfaces:**
- Produces: `export const DICTIONARIES: Record<string, Record<string, unknown>>` из `src/i18n/index.ts` — все `src/locales/*.json` по коду (`"pt-BR"` для `pt-BR.json`). Новый словарь подключается только появлением файла.
- Consumes: `LANGUAGES` из `src/i18n/languages.ts`, `setLanguage` из `src/i18n/index.ts`.

- [ ] **Step 1: Написать тест** `src/i18n/locales.test.ts`:

```ts
import { afterAll, describe, expect, it } from "vitest";
import { DICTIONARIES, i18n, setLanguage } from "./index";
import { LANGUAGES } from "./languages";

type Dict = Record<string, unknown>;
function get(dict: Dict, key: string): unknown {
  return key.split(".").reduce<unknown>((o, k) => (o && typeof o === "object" ? (o as Dict)[k] : undefined), dict);
}
function flatKeys(dict: Dict, prefix = ""): string[] {
  return Object.entries(dict).flatMap(([k, v]) =>
    v && typeof v === "object" ? flatKeys(v as Dict, `${prefix}${k}.`) : [`${prefix}${k}`],
  );
}
const pluralStems = flatKeys(DICTIONARIES.en).filter((k) => k.endsWith("_one")).map((k) => k.slice(0, -4));

afterAll(() => i18n.changeLanguage("ru"));

describe("словари", () => {
  it("загружены en и ru, каждый словарь — язык из списка", () => {
    expect(Object.keys(DICTIONARIES)).toEqual(expect.arrayContaining(["en", "ru"]));
    for (const code of Object.keys(DICTIONARIES)) expect(LANGUAGES.map((l) => l.code)).toContain(code);
  });

  for (const code of Object.keys(DICTIONARIES)) {
    it(`${code}: переключение и плюральные формы`, async () => {
      await setLanguage(code);
      expect(i18n.language).toBe(code);
      expect(i18n.t("common.save")).toBe(get(DICTIONARIES[code], "common.save"));
      const rules = new Intl.PluralRules(code);
      for (const stem of pluralStems) {
        for (const n of [0, 1, 2, 5, 21, 1_000_000]) {
          const form = `${stem}_${rules.select(n)}`;
          expect(get(DICTIONARIES[code], form), `${code}: ${form}`).toBeTypeOf("string");
        }
      }
    });
  }
});
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `pnpm vitest run src/i18n/locales.test.ts`
Expected: FAIL — `DICTIONARIES` не экспортируется (`undefined`), `Object.keys(undefined)` бросает `TypeError`.

- [ ] **Step 3: Реализовать.** В `src/i18n/index.ts` заменить импорты словарей и ресурсы:

```ts
import i18next from "i18next";
import { initReactI18next } from "react-i18next";
import { LANGUAGES } from "./languages";

const modules = import.meta.glob<{ default: Record<string, unknown> }>("../locales/*.json", { eager: true });

/** Все словари src/locales/<код>.json по коду языка. Новый язык подключается появлением файла. */
export const DICTIONARIES: Record<string, Record<string, unknown>> = Object.fromEntries(
  Object.entries(modules).map(([path, m]) => [path.replace(/^.*\/([^/]+)\.json$/, "$1"), m.default]),
);
```

и в `instance.init({...})`:

```ts
  resources: {
    ...Object.fromEntries(Object.entries(DICTIONARIES).map(([code, d]) => [code, { translation: d }])),
    ...(import.meta.env.DEV ? { [PSEUDO]: { translation: DICTIONARIES.en } } : {}),
  },
```

Остальной файл (псевдоязык, `setLanguage`, `currentLocale`, `syncDocumentLang`) не меняется. Строки `import en from "../locales/en.json";` и `import ru from "../locales/ru.json";` удаляются. Тип `import.meta.glob` уже даёт `src/vite-env.d.ts` (`/// <reference types="vite/client" />`).

- [ ] **Step 4: Тесты и сборка проходят**

Run: `pnpm vitest run src/i18n && pnpm test && pnpm build`
Expected: PASS; в `pnpm build` нет ошибок типов. Тест `en` и `ru` проходит для всех чисел.

- [ ] **Step 5: Commit**

```bash
git add src/i18n/index.ts src/i18n/locales.test.ts
git commit -m "feat(i18n): словари подключаются автоматически из src/locales

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Задачи 4–9: словари Pro-языков

Каждая задача 4–9 — один язык, выполняется по одной и той же процедуре (повторена в каждой задаче целиком). Порядок: de → fr → es → it → pl → pt-BR (pl позже, потому что у него четыре плюральные формы; к тому моменту процедура обкатана).

Общие правила для переводчика (Claude):
- Переводить `src/locales/en.json` целиком, ключ в ключ, сохраняя структуру и порядок ключей. Смысл сверять с `ru.json`.
- Глоссарий `docs/i18n/glossary.md` обязателен: термины, обращение, кнопки, кавычки.
- Переводить разделами верхнего уровня (`common`, `errors`, `sidebar`, `settings`, `connections`, `topics`, `groups`, `messages`, `producer`, …), после каждого раздела — `pnpm i18n:check` (он покажет недостающие ключи следующих разделов, это нормально до конца задачи).
- Не трогать `en.json`, `ru.json`, код. Если в en найдена ошибка — сообщить в отчёте задачи, не исправлять.

### Task 4: Немецкий (`de`)

**Files:**
- Create: `src/locales/de.json`
- Modify: `docs/i18n/same-as-en.json` (ключ `"de"`)

**Interfaces:**
- Consumes: `docs/i18n/glossary.md` (Task 1), проверки `checkSameAsEn`/`checkTypography`/теги (Task 2), автозагрузка `DICTIONARIES` и тест `src/i18n/locales.test.ts` (Task 3).
- Produces: `src/locales/de.json` с тем же множеством ключей, что `en.json`; плюральные формы `_one`, `_other`.

- [ ] **Step 1: Убедиться, что словаря нет и тест его не видит**

Run: `test ! -f src/locales/de.json && pnpm vitest run src/i18n/locales.test.ts 2>&1 | grep -c "de:"`
Expected: `0`.

- [ ] **Step 2: Перевести `en.json` → `de.json`** по общим правилам и глоссарию. Обязательные контрольные строки (должны совпасть дословно):

| Ключ | Значение |
|---|---|
| `common.save` | `Speichern` |
| `common.cancel` | `Abbrechen` |
| `common.loading` | `Wird geladen…` |
| `common.readOnly` | `Schreibgeschützt` |
| `sidebar.settings` | `Einstellungen` |
| `sidebar.groups` | `Consumer Groups` |
| `settings.language.system` | `Systemsprache ({{name}})` |
| `topics.delete.titleMany_one` | `{{count}} Topic löschen?` |
| `topics.delete.titleMany_other` | `{{count}} Topics löschen?` |

- [ ] **Step 3: Запустить проверку и исправить всё, кроме намеренных совпадений**

Run: `pnpm i18n:check`
Expected: остаются только сообщения `de: … совпадает с en …` для строк, которые по глоссарию одинаковы (например `sidebar.producer` = `Producer`, `connections.form.step.tcp` = `DNS / TCP`, `common.details` = `Details`). Ошибок про ключи, параметры и теги — нет.

- [ ] **Step 4: Внести намеренные совпадения в `docs/i18n/same-as-en.json`** под ключом `"de"` (массив ключей из сообщений шага 3, по алфавиту). Каждый ключ — только если по глоссарию слово действительно не переводится; иначе перевести строку.

- [ ] **Step 5: Все проверки проходят**

Run: `pnpm i18n:check && pnpm vitest run src/i18n/locales.test.ts && node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | grep "^↔ de"`
Expected: `✓ словари de, en, ru согласованы …`; тест `de: переключение и плюральные формы` PASS; отчёт о длине выводит до 15 строк `↔ de: …` — для каждой с коэффициентом ≥ 2 попытаться найти более короткий вариант, не теряя смысла (решение — на усмотрение переводчика, перечислить их в отчёте задачи для ручной проверки ширины).

- [ ] **Step 6: Commit**

```bash
git add src/locales/de.json docs/i18n/same-as-en.json
git commit -m "feat(i18n): немецкий словарь

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Французский (`fr`)

**Files:**
- Create: `src/locales/fr.json`
- Modify: `docs/i18n/same-as-en.json` (ключ `"fr"`)

**Interfaces:**
- Consumes: `docs/i18n/glossary.md` (Task 1), проверки `checkSameAsEn`/`checkTypography`/теги (Task 2), автозагрузка `DICTIONARIES` и тест `src/i18n/locales.test.ts` (Task 3).
- Produces: `src/locales/fr.json`; плюральные формы `_one`, `_many`, `_other` (`_many` — для 1 000 000 и больше: «1 000 000 de topics»).

- [ ] **Step 1: Убедиться, что словаря нет**

Run: `test ! -f src/locales/fr.json && echo absent`
Expected: `absent`.

- [ ] **Step 2: Перевести `en.json` → `fr.json`** по общим правилам и глоссарию. Неразрывный пробел U+00A0 — перед `:` `;` `!` `?`, после `«` и перед `»` (в JSON можно писать сам символ или ` `). Обязательные контрольные строки:

| Ключ | Значение |
|---|---|
| `common.save` | `Enregistrer` |
| `common.cancel` | `Annuler` |
| `common.loading` | `Chargement…` |
| `common.readOnly` | `Lecture seule` |
| `sidebar.settings` | `Paramètres` |
| `settings.language.system` | `Langue du système ({{name}})` |
| `topics.delete.titleMany_one` | `Supprimer {{count}} topic ?` |
| `topics.delete.titleMany_many` | `Supprimer {{count}} de topics ?` |
| `topics.delete.titleMany_other` | `Supprimer {{count}} topics ?` |

(` ` в таблице — неразрывный пробел.)

- [ ] **Step 3: Запустить проверку и исправить всё, кроме намеренных совпадений**

Run: `pnpm i18n:check`
Expected: нет ошибок про ключи, параметры, теги и типографику (`fr: … обычный пробел …`); остаются только `fr: … совпадает с en …` для строк, одинаковых по глоссарию (например `sidebar.producer`, `connections.form.step.tcp`).

- [ ] **Step 4: Внести намеренные совпадения в `docs/i18n/same-as-en.json`** под ключом `"fr"` (по алфавиту), остальные — перевести.

- [ ] **Step 5: Все проверки проходят**

Run: `pnpm i18n:check && pnpm vitest run src/i18n/locales.test.ts && node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | grep "^↔ fr"`
Expected: `✓ словари de, en, fr, ru согласованы …`; тест `fr: …` PASS; длинные строки (×2 и больше) — попытаться сократить, перечислить в отчёте задачи.

- [ ] **Step 6: Commit**

```bash
git add src/locales/fr.json docs/i18n/same-as-en.json
git commit -m "feat(i18n): французский словарь

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: Испанский (`es`)

**Files:**
- Create: `src/locales/es.json`
- Modify: `docs/i18n/same-as-en.json` (ключ `"es"`)

**Interfaces:**
- Consumes: `docs/i18n/glossary.md` (Task 1), проверки Task 2, автозагрузка и тест Task 3.
- Produces: `src/locales/es.json`; плюральные формы `_one`, `_many`, `_other` (`_many` — «1 000 000 de topics»).

- [ ] **Step 1: Убедиться, что словаря нет**

Run: `test ! -f src/locales/es.json && echo absent`
Expected: `absent`.

- [ ] **Step 2: Перевести `en.json` → `es.json`** по общим правилам и глоссарию (обращение на «tú», вопросы с `¿…?`). Обязательные контрольные строки:

| Ключ | Значение |
|---|---|
| `common.save` | `Guardar` |
| `common.cancel` | `Cancelar` |
| `common.loading` | `Cargando…` |
| `common.readOnly` | `Solo lectura` |
| `sidebar.settings` | `Ajustes` |
| `settings.language.system` | `Idioma del sistema ({{name}})` |
| `topics.delete.titleMany_one` | `¿Eliminar {{count}} topic?` |
| `topics.delete.titleMany_many` | `¿Eliminar {{count}} de topics?` |
| `topics.delete.titleMany_other` | `¿Eliminar {{count}} topics?` |

- [ ] **Step 3: Запустить проверку и исправить всё, кроме намеренных совпадений**

Run: `pnpm i18n:check`
Expected: нет ошибок про ключи, параметры, теги, многоточие; остаются только `es: … совпадает с en …` для строк, одинаковых по глоссарию.

- [ ] **Step 4: Внести намеренные совпадения в `docs/i18n/same-as-en.json`** под ключом `"es"` (по алфавиту), остальные — перевести.

- [ ] **Step 5: Все проверки проходят**

Run: `pnpm i18n:check && pnpm vitest run src/i18n/locales.test.ts && node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | grep "^↔ es"`
Expected: `✓ словари de, en, es, fr, ru согласованы …`; тест `es: …` PASS; длинные строки — попытаться сократить, перечислить в отчёте.

- [ ] **Step 6: Commit**

```bash
git add src/locales/es.json docs/i18n/same-as-en.json
git commit -m "feat(i18n): испанский словарь

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: Итальянский (`it`)

**Files:**
- Create: `src/locales/it.json`
- Modify: `docs/i18n/same-as-en.json` (ключ `"it"`)

**Interfaces:**
- Consumes: `docs/i18n/glossary.md` (Task 1), проверки Task 2, автозагрузка и тест Task 3.
- Produces: `src/locales/it.json`; плюральные формы `_one`, `_many`, `_other` (`_many` — «1.000.000 di topic»).

- [ ] **Step 1: Убедиться, что словаря нет**

Run: `test ! -f src/locales/it.json && echo absent`
Expected: `absent`.

- [ ] **Step 2: Перевести `en.json` → `it.json`** по общим правилам и глоссарию (артикли перед английскими терминами: il topic, l’offset, i broker; апостроф — типографский `’`). Обязательные контрольные строки:

| Ключ | Значение |
|---|---|
| `common.save` | `Salva` |
| `common.cancel` | `Annulla` |
| `common.loading` | `Caricamento…` |
| `common.readOnly` | `Sola lettura` |
| `sidebar.settings` | `Impostazioni` |
| `settings.language.system` | `Lingua di sistema ({{name}})` |
| `topics.delete.titleMany_one` | `Eliminare {{count}} topic?` |
| `topics.delete.titleMany_many` | `Eliminare {{count}} di topic?` |
| `topics.delete.titleMany_other` | `Eliminare {{count}} topic?` |

- [ ] **Step 3: Запустить проверку и исправить всё, кроме намеренных совпадений**

Run: `pnpm i18n:check`
Expected: нет ошибок про ключи, параметры, теги, многоточие; остаются только `it: … совпадает с en …` для строк, одинаковых по глоссарию (у итальянского их больше: topic, offset, broker, consumer group неизменяемы).

- [ ] **Step 4: Внести намеренные совпадения в `docs/i18n/same-as-en.json`** под ключом `"it"` (по алфавиту), остальные — перевести.

- [ ] **Step 5: Все проверки проходят**

Run: `pnpm i18n:check && pnpm vitest run src/i18n/locales.test.ts && node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | grep "^↔ it"`
Expected: `✓ словари de, en, es, fr, it, ru согласованы …`; тест `it: …` PASS; длинные строки — попытаться сократить, перечислить в отчёте.

- [ ] **Step 6: Commit**

```bash
git add src/locales/it.json docs/i18n/same-as-en.json
git commit -m "feat(i18n): итальянский словарь

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Польский (`pl`)

**Files:**
- Create: `src/locales/pl.json`
- Modify: `docs/i18n/same-as-en.json` (ключ `"pl"`)

**Interfaces:**
- Consumes: `docs/i18n/glossary.md` (Task 1), проверки Task 2, автозагрузка и тест Task 3.
- Produces: `src/locales/pl.json`; плюральные формы `_one` (1), `_few` (2–4, 22–24), `_many` (0, 5–21, 25…), `_other` (дроби).

- [ ] **Step 1: Убедиться, что словаря нет**

Run: `test ! -f src/locales/pl.json && echo absent`
Expected: `absent`.

- [ ] **Step 2: Перевести `en.json` → `pl.json`** по общим правилам и глоссарию (безличные формы, кнопки в повелительном, «topik» склоняется, значения параметров — в именительном после существительного: `topik „{{name}}”`; consumer group → «grupa konsumentów»). Обязательные контрольные строки:

| Ключ | Значение |
|---|---|
| `common.save` | `Zapisz` |
| `common.cancel` | `Anuluj` |
| `common.loading` | `Wczytywanie…` |
| `common.readOnly` | `Tylko do odczytu` |
| `sidebar.settings` | `Ustawienia` |
| `sidebar.groups` | `Grupy konsumentów` |
| `settings.language.system` | `Język systemu ({{name}})` |
| `topics.delete.titleMany_one` | `Usunąć {{count}} topik?` |
| `topics.delete.titleMany_few` | `Usunąć {{count}} topiki?` |
| `topics.delete.titleMany_many` | `Usunąć {{count}} topików?` |
| `topics.delete.titleMany_other` | `Usunąć {{count}} topiku?` |

- [ ] **Step 3: Запустить проверку и исправить всё, кроме намеренных совпадений**

Run: `pnpm i18n:check`
Expected: нет ошибок про ключи (в том числе `_few`/`_many` у каждого плюрального ключа), параметры, теги; остаются только `pl: … совпадает с en …` для строк, одинаковых по глоссарию.

- [ ] **Step 4: Внести намеренные совпадения в `docs/i18n/same-as-en.json`** под ключом `"pl"` (по алфавиту), остальные — перевести.

- [ ] **Step 5: Все проверки проходят**

Run: `pnpm i18n:check && pnpm vitest run src/i18n/locales.test.ts && node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | grep "^↔ pl"`
Expected: `✓ словари de, en, es, fr, it, pl, ru согласованы …`; тест `pl: …` PASS (числа 0, 1, 2, 5, 21, 1 000 000 дают существующие формы); длинные строки — попытаться сократить, перечислить в отчёте.

- [ ] **Step 6: Commit**

```bash
git add src/locales/pl.json docs/i18n/same-as-en.json
git commit -m "feat(i18n): польский словарь

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Бразильский португальский (`pt-BR`)

**Files:**
- Create: `src/locales/pt-BR.json`
- Modify: `docs/i18n/same-as-en.json` (ключ `"pt-BR"`)

**Interfaces:**
- Consumes: `docs/i18n/glossary.md` (Task 1), проверки Task 2, автозагрузка и тест Task 3 (код словаря из имени файла — `pt-BR`, совпадает с `LANGUAGES`).
- Produces: `src/locales/pt-BR.json`; плюральные формы `_one` (0 и 1), `_many` (1 000 000…: «1.000.000 de tópicos»), `_other`.

- [ ] **Step 1: Убедиться, что словаря нет**

Run: `test ! -f src/locales/pt-BR.json && echo absent`
Expected: `absent`.

- [ ] **Step 2: Перевести `en.json` → `pt-BR.json`** по общим правилам и глоссарию (бразильская норма: arquivo, tela, excluir; topic → «tópico»). Обязательные контрольные строки:

| Ключ | Значение |
|---|---|
| `common.save` | `Salvar` |
| `common.cancel` | `Cancelar` |
| `common.loading` | `Carregando…` |
| `common.readOnly` | `Somente leitura` |
| `sidebar.settings` | `Configurações` |
| `settings.language.system` | `Idioma do sistema ({{name}})` |
| `topics.delete.titleMany_one` | `Excluir {{count}} tópico?` |
| `topics.delete.titleMany_many` | `Excluir {{count}} de tópicos?` |
| `topics.delete.titleMany_other` | `Excluir {{count}} tópicos?` |

- [ ] **Step 3: Запустить проверку и исправить всё, кроме намеренных совпадений**

Run: `pnpm i18n:check`
Expected: нет ошибок про ключи, параметры, теги, многоточие; остаются только `pt-BR: … совпадает с en …` для строк, одинаковых по глоссарию.

- [ ] **Step 4: Внести намеренные совпадения в `docs/i18n/same-as-en.json`** под ключом `"pt-BR"` (по алфавиту), остальные — перевести.

- [ ] **Step 5: Все проверки проходят**

Run: `pnpm i18n:check && pnpm vitest run src/i18n/locales.test.ts && node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | grep "^↔ pt-BR"`
Expected: `✓ словари de, en, es, fr, it, pl, pt-BR, ru согласованы …`; тест `pt-BR: …` PASS; длинные строки — попытаться сократить, перечислить в отчёте.

- [ ] **Step 6: Commit**

```bash
git add src/locales/pt-BR.json docs/i18n/same-as-en.json
git commit -m "feat(i18n): бразильский португальский словарь

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Все языки обязательны, документация, смоук B2

**Files:**
- Modify: `package.json` (скрипт `i18n:check`)
- Modify: `docs/i18n/README.md`
- Create: `docs/smoke/b2.md`

**Interfaces:**
- Consumes: флаг `--require-all-languages` (Task 2), все восемь словарей (Tasks 4–9), `glossary.md` и `review-checklist.md` (Task 1).

- [ ] **Step 1: Проверить, что флаг ловит отсутствующий словарь** (временно убрать файл, не коммитить):

Run: `mv src/locales/pt-BR.json src/locales/pt-BR.json.bak && node scripts/i18n-check.mjs --require-all-languages; mv src/locales/pt-BR.json.bak src/locales/pt-BR.json`
Expected: `✗ нет словаря src/locales/pt-BR.json`, код выхода 1; файл возвращён (`git status` — без изменений в `src/locales`).

- [ ] **Step 2: Включить флаг в CI.** В `package.json`:

```json
    "i18n:check": "node scripts/i18n-check.mjs --require-all-languages --rust-cyrillic src-tauri/src"
```

Run: `pnpm i18n:check`
Expected: `✓ словари de, en, es, fr, it, pl, pt-BR, ru согласованы, вызовов из Rust: …`.

- [ ] **Step 3: Обновить `docs/i18n/README.md`.** Заменить первые две строки после заголовка на:

```markdown
Интерфейс и тексты Rust локализуются через словари `src/locales/<код>.json`: `en` — эталон ключей и параметров,
`ru` — эталон смысла, `de`, `fr`, `es`, `it`, `pl`, `pt-BR` — Pro-языки (без Pro-лицензии действует `en`).
Термины и стиль переводов — [glossary.md](glossary.md), выборочная проверка носителем — [review-checklist.md](review-checklist.md).
```

В разделе «Как добавить строку» заменить пункт 2 на:

```markdown
2. Добавить ключ во все словари `src/locales/*.json`: `en` — эталон, `ru` — по-русски, Pro-языки — перевод по глоссарию.
   Если перевод намеренно совпадает с английским (`Producer`, `DNS / TCP`), добавить ключ в `docs/i18n/same-as-en.json`.
```

В разделе «Плюралы» заменить строку про набор форм на:

```markdown
(набор форм — по `Intl.PluralRules`: en, de — `one`/`other`; fr, es, it, pt-BR — `one`/`many`/`other`;
ru, pl — `one`/`few`/`many`/`other`). Пример: `topics.delete.titleMany_*`.
```

Раздел «Проверки» заменить на:

```markdown
## Проверки
`pnpm i18n:check` = `node scripts/i18n-check.mjs --require-all-languages --rust-cyrillic src-tauri/src`:
словарь есть у каждого языка из `src/i18n/languages.ts`; одинаковые ключи, параметры и теги во всех словарях;
плюральные формы под язык; строки, совпадающие с en, — только из `docs/i18n/same-as-en.json`;
многоточие `…` и неразрывные пробелы во французском; ключи `ltext!`/`app_err!`;
кириллица в коде фронтенда и Rust (кроме словарей и тестов). Запускается в CI. Тесты скриптов — `pnpm test:scripts`.

`node scripts/i18n-check.mjs --lengths` дополнительно печатает строки, заметно длиннее английских, —
что проверить на ширине окна 1400 px и 900 px.

## Новый язык
1. Код и самоназвание — в `src/i18n/languages.ts` и в списке языков Rust (`src-tauri/src/settings.rs`).
2. Столбец в `glossary.md`, затем `src/locales/<код>.json` переводом `en.json`.
3. `pnpm i18n:check`, `pnpm vitest run src/i18n/locales.test.ts`, смоук по `docs/smoke/b2.md`.
```

- [ ] **Step 4: Создать `docs/smoke/b2.md`:**

```markdown
# Смоук B2 (Pro-языки)

`pnpm tauri dev`, Pro-лицензия (ключ — `docs/licensing.md`), Kafka: `docker compose -f dev/docker-compose.yml up -d`.

1. «Настройки» → по очереди Deutsch, Français, Español, Italiano, Polski, Português (Brasil): интерфейс сразу
   на выбранном языке без перезапуска, `document.documentElement.lang` = код языка (`pt-BR` для бразильского).
2. Для каждого языка — проход по экранам из `docs/i18n/review-checklist.md` (раздел «Экраны»): нет английских строк,
   кроме намеренных (`docs/i18n/same-as-en.json`), нет имён ключей вида `topics.list.empty`.
3. Ширина: окно 1400 px и минимальное 900 px. Строки из `node scripts/i18n-check.mjs --lengths` проверить особо:
   кнопки, заголовки колонок списка топиков и групп, вкладки, левая панель. Обрезанное — сократить перевод.
4. Склонения (pl): удаление 1, 2, 5, 22 топиков — «topik», «topiki», «topików», «topiki»;
   сброс оффсетов 1 / 2 / 5 партиций; «Лицензия истекает через 1 / 2 / 5 дней».
5. Числа и размеры по языку: в списке топиков количество сообщений — «1.234.567» (de, it, pt-BR, es),
   «1 234 567» (fr, pl — узкий неразрывный пробел).
6. Ошибки: подключение к `localhost:1` — текст ошибки на выбранном языке, «Подробности» — исходный текст librdkafka.
7. Без Pro: удалить ключ лицензии при выбранном Deutsch → интерфейс на английском,
   «Настройки» показывают причину; вернуть ключ → снова немецкий.
8. Язык ОС: в системных настройках macOS основной язык Deutsch (Österreich), «Как в системе» → немецкий (с Pro);
   Português (Portugal) → бразильский португальский. Вернуть язык ОС.
9. Лог `~/Library/Logs/io.github.vvnovg.glafira.kafka/app.log` — сообщения на английском при любом языке интерфейса.
```

- [ ] **Step 5: Полный прогон**

Run: `pnpm test && pnpm test:scripts && pnpm i18n:check && pnpm build`
Expected: всё PASS, `pnpm i18n:check` перечисляет восемь словарей.

- [ ] **Step 6: Commit**

```bash
git add package.json docs/i18n/README.md docs/smoke/b2.md
git commit -m "feat(i18n): все языки обязательны в i18n-check, документация и смоук B2

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## После плана

- Финальное ревью ветки (`superpowers:requesting-code-review`): глоссарий соблюдён во всех словарях, контрольные строки на месте, `same-as-en.json` не содержит переводимых строк.
- Ручной смоук пользователем по `docs/smoke/b2.md`.
- Выборочная проверка носителями по `docs/i18n/review-checklist.md` — до выпуска каждого языка; правки идут отдельными коммитами в словарь (и в глоссарий, если меняется решение по термину).
