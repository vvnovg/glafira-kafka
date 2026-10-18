# E «Азиатские языки, все языки бесплатно» Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Все языки интерфейса бесплатны; добавлены `zh-CN`, `zh-TW`, `ja`, `ko` — интерфейс, подсказки и руководство.

**Architecture:** Концепция «Pro-язык» удаляется из Rust (`settings.rs`, команда выбора языка) и фронтенда. Новые языки подключаются как в B2: код в `src/i18n/languages.ts` и в списке Rust, словарь `src/locales/<код>.json`, главы `docs/guide/<код>/*.md`. Для китайского и японского `scripts/i18n-check.mjs` получает проверки типографики, а отчёт о длине — видимую ширину символов.

**Tech Stack:** Rust, React 19, i18next, `Intl.PluralRules`, node:test, Vitest.

**Спецификация:** `docs/superpowers/specs/2026-10-18-glafira-asian-languages-design.md`.

## Global Constraints

- Языки после E (порядок списка): en, ru, de, fr, es, it, pl, pt-BR, zh-CN, zh-TW, ja, ko; самоназвания: 简体中文, 繁體中文, 日本語, 한국어. Все — Free.
- Сопоставление языка ОС: `zh-Hans*`, `zh-CN`, `zh-SG`, `zh` → `zh-CN`; `zh-Hant*`, `zh-TW`, `zh-HK`, `zh-MO` → `zh-TW` (явный `Hans` важнее региона); `ja*` → `ja`; `ko*` → `ko`.
- Правила письма — `docs/i18n/glossary.md` (Task 2): zh — полноширинная пунктуация «，。：；？！（）», кавычки “…” (zh-CN) / 「…」 (zh-TW), пробел между иероглифами и латиницей/цифрами, без пробела между иероглифами, zh-TW — тайваньская лексика; ja — です/ます, катакана по глоссарию, полноширинная пунктуация, 「…」, без пробелов; ko — -습니다, пробелы между словами, ASCII-пунктуация.
- Множественное число zh/ja/ko — только `other`.
- Руководство: для zh-CN, zh-TW, ja метки `[[ui:…]]` ставятся без пробелов вокруг; остальные правила глав (якоря, метки без параметров, ссылки, без HTML) — как в D.
- Перевод — с en (эталон ключей, тегов, меток), сверка смысла по ru, термины по глоссарию и словарю языка; Kafka-факты не меняются.
- Rust-тексты ошибок остаются английскими (`message`), переводятся только ключи словаря.
- Проверки после каждой задачи: `pnpm test`, `pnpm test:scripts`, `pnpm i18n:check`, `pnpm build`; при изменении Rust — `cargo test --lib`, clippy, fmt (из `src-tauri`, `CARGO_INCREMENTAL=0`).
- Коммиты — на русском в стиле репозитория, строка атрибуции модели исполнителя. Push только с согласия пользователя.

---

### Task 1: Все языки бесплатно

**Files:**
- Modify: `src-tauri/src/settings.rs`, `src-tauri/src/commands/settings.rs`, `src/i18n/languages.ts`, `src/components/settings/SettingsPanel.tsx` (+ тест), `src/api/types.ts` (если есть `proLanguages`/`proRequired`), `src/locales/*.json`, `src/components/license/*` (если перечисляет языки), `docs/guide/*/getting-started.md`, `docs/guide/*/license-updates.md` (и любые другие главы с Pro-языками — `grep -rn -i "pro" docs/guide/en/*.md` для поиска), `README.md`, `docs/licensing.md`, `docs/admin.md`, `docs/i18n/README.md`, `docs/i18n/review-checklist.md`, смоук-документы с Pro-языками

**Interfaces:**
- Produces (Rust): `pub const LANGUAGES: [&str; 8]` (без признака Pro); `resolve(system, chosen, policy) -> (String, LanguageReason)` (без параметра `pro`); `LanguageReason` без `ProRequired`; `LanguageSettings` без `pro_languages`; `set_language` не проверяет лицензию.
- Produces (TS): `LANGUAGES: { code; nativeName }[]` без `pro`.

- [ ] **Step 1: Падающие тесты.** Rust (`settings.rs` / `commands/settings.rs`): Free выбирает `de` → действующий язык `de`; `resolve(None, Some("fr"), None)` → `("fr", User)`; заменить тесты `free_can_pick_free_language_only`, `pro_can_pick_pro_language`, `priority_and_pro` на проверки без Pro. Фронтенд (`SettingsPanel.test.tsx`): все языки выбираемы без суффикса «— Pro», подсказки о Pro нет.
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать** код; удалить ключи `settings.language.proSuffix` и `settings.language.proRequired` из всех 8 словарей (и из `docs/i18n/same-as-en.json`, если упомянуты).
- [ ] **Step 4: Тексты.** Во всех языках руководства и в документации заменить утверждения «дополнительные языки — Pro» на «все языки доступны бесплатно»; на экране «Лицензия» список Pro-функций языки не содержит (проверить). Якоря и метки глав не менять; если удаляется метка удалённого ключа — перефразировать.
- [ ] **Step 5: Проверки** (все из Global Constraints, включая cargo).
- [ ] **Step 6: Commit** — `feat(i18n): все языки интерфейса бесплатны`.

### Task 2: Инфраструктура азиатских языков

**Files:**
- Modify: `src-tauri/src/settings.rs`, `scripts/i18n-check.mjs` (+ тест), `src/guide/parse.test.ts`, `scripts/guide-publish.test.mjs`, `src/i18n/locales.test.ts`, `docs/i18n/glossary.md`, `docs/i18n/README.md`

**Interfaces:**
- Produces (Rust):
  ```rust
  pub fn match_language(tag: &str) -> Option<&'static str> { match_language_in(tag, &LANGUAGES) }

  /// Язык ОС → код из `langs`. Китайский: явный Hans → zh-CN; Hant/TW/HK/MO → zh-TW; иначе zh-CN.
  fn match_language_in(tag: &str, langs: &[&'static str]) -> Option<&'static str> {
      let lower = tag.replace('_', "-").to_ascii_lowercase();
      let mut parts = lower.split('-');
      let primary = parts.next().unwrap_or("");
      let rest: Vec<&str> = parts.collect();
      let pick = |code: &'static str| langs.iter().copied().find(|c| *c == code);
      match primary {
          "pt" => pick("pt-BR"),
          "zh" => {
              let traditional = !rest.contains(&"hans") && rest.iter().any(|p| matches!(*p, "hant" | "tw" | "hk" | "mo"));
              pick(if traditional { "zh-TW" } else { "zh-CN" })
          }
          _ => langs.iter().copied().find(|c| c.eq_ignore_ascii_case(primary)),
      }
  }
  ```
- Produces (`scripts/i18n-check.mjs`):
  - `CJK_TYPO_LANGS = ["zh-CN", "zh-TW", "ja"]`, `HAN_SPACE_LANGS = ["zh-CN", "zh-TW"]`;
  - `cjkProblems(text: string, code: string): string[]` — вне `` `code` ``, `{{…}}`, тегов: полуширинная `, . : ; ? !` сразу после символа `\p{Script=Han}|\p{Script=Hiragana}|\p{Script=Katakana}` → «полуширинная пунктуация после иероглифа»; для `HAN_SPACE_LANGS` — пробел между двумя `\p{Script=Han}` → «пробел между иероглифами»;
  - `checkTypography` вызывает `cjkProblems` для строк словарей этих языков; `checkGuide` — для прозы глав (вне fenced/inline code и URL);
  - `visualWidth(s: string): number` — символы Han, Hiragana, Katakana, Hangul и полноширинные формы (`\u{FF00}-\u{FFEF}`) считаются за 2; `lengthReport` сравнивает `visualWidth`, а не `length`.

- [ ] **Step 1: Падающие тесты.**
  - Rust `match_language_in` с полным списком 12 кодов: `zh-Hans-CN`, `zh-CN`, `zh-SG`, `zh` → zh-CN; `zh-Hant-TW`, `zh-TW`, `zh-HK`, `zh-MO`, `zh_TW` → zh-TW; `zh-Hans-HK` → zh-CN; `ja-JP` → ja; `ko-KR` → ko; `pt-PT` → pt-BR; `xx` → None. С текущим списком без азиатских кодов: `zh-CN` → None.
  - `scripts/i18n-check.test.mjs`: `cjkProblems("设置,保存", "zh-CN")` → проблема; `"设置，保存"` → нет; `"连接 Kafka 集群"` → нет; `"连接 集群"` (zh) → «пробел между иероглифами»; `"トピック."` (ja) → проблема; `"토픽."` (ko не в списке) → нет; внутри `` `a,b` `` → нет; `visualWidth("设置") === 4`, `visualWidth("ab") === 2`.
  - `parse.test.ts`: `parseChapter` строки `文本[[ui:sidebar.settings]]文本` с меткой «设置» даёт `文本**设置**文本`; рендер (если в файле есть рендер-тесты) — `<strong>` присутствует. `guide-publish.test.mjs`: `publishChapter` той же строки → `文本**设置**文本`.
  - `locales.test.ts`: для каждого словаря после `setLanguage(code)` — `document.documentElement.lang === code`.
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать** (CJK-проверки активны только для языков, у которых есть словарь/главы; пока их нет — правила проверены тестами).
- [ ] **Step 4: Глоссарий.** Добавить в таблицы «Термины» и «Частые слова интерфейса» столбцы zh-CN, zh-TW, ja, ko (все существующие строки) и раздел «Правила письма: китайский, японский, корейский» по Global Constraints. Примеры терминов: topic — 主题 (zh-CN) / 主題 (zh-TW) / トピック (ja) / 토픽 (ko); offset — 偏移量 / 偏移量 / オフセット / 오프셋; consumer group — 消费者组 / 消費者群組 / コンシューマーグループ / 컨슈머 그룹; partition — 分区 / 分割區 / パーティション / 파티션; broker — Broker (латиницей во всех четырёх) — окончательные значения выбрать по устоявшейся терминологии документации Kafka на этих языках и зафиксировать.
- [ ] **Step 5: `docs/i18n/README.md`** — новые языки в списке форм множественного числа (only `other`), правило меток без пробелов для zh/ja.
- [ ] **Step 6: Проверки**; Commit — `feat(i18n): инфраструктура китайского, японского и корейского`.

### Задачи 3–6: словари интерфейса

Каждая задача — один язык: добавить код в `src/i18n/languages.ts` и в `LANGUAGES` Rust (порядок — как в Global Constraints; размер массива увеличить), тест сопоставления языка ОС для этого кода с реальным списком; перевести `src/locales/en.json` → `src/locales/<код>.json` целиком (ключ в ключ, порядок как в en, все `hints.*` включая `hints.topicConfig.*`); заполнить `docs/i18n/same-as-en.json` под этим кодом только намеренными совпадениями; скопировать `docs/guide/en/*.md` в `docs/guide/<код>/*.md` (перевод — задачи 7–10). Отчёт о длине (`node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | grep "<код>"`) — строки с коэффициентом ≥ 1.6 по видимой ширине перечислить в отчёте и по возможности сократить.

### Task 3: Китайский упрощённый (`zh-CN`)
- Files: `src/i18n/languages.ts`, `src-tauri/src/settings.rs`, `src/locales/zh-CN.json`, `docs/i18n/same-as-en.json`, `docs/guide/zh-CN/*.md`
- Контрольные строки: `common.save` = `保存`; `common.cancel` = `取消`; `common.loading` = `加载中…`; `sidebar.settings` = `设置`; `settings.language.system` = `跟随系统（{{name}}）`.
- [ ] Шаги по общей процедуре; проверки; Commit — `feat(i18n): китайский упрощённый`.

### Task 4: Китайский традиционный (`zh-TW`)
- Files: те же для `zh-TW`
- Контрольные строки: `common.save` = `儲存`; `common.cancel` = `取消`; `common.loading` = `載入中…`; `sidebar.settings` = `設定`; `settings.language.system` = `跟隨系統（{{name}}）`.
- Перевод — с en, с тайваньской лексикой; zh-CN — только справочно, не источник для конвертации.
- [ ] Шаги по общей процедуре; Commit — `feat(i18n): китайский традиционный`.

### Task 5: Японский (`ja`)
- Files: те же для `ja`
- Контрольные строки: `common.save` = `保存`; `common.cancel` = `キャンセル`; `common.loading` = `読み込み中…`; `sidebar.settings` = `設定`; `settings.language.system` = `システムの言語（{{name}}）`.
- [ ] Шаги по общей процедуре; Commit — `feat(i18n): японский`.

### Task 6: Корейский (`ko`)
- Files: те же для `ko`
- Контрольные строки: `common.save` = `저장`; `common.cancel` = `취소`; `common.loading` = `불러오는 중…`; `sidebar.settings` = `설정`; `settings.language.system` = `시스템 언어({{name}})`.
- [ ] Шаги по общей процедуре; Commit — `feat(i18n): корейский`.

### Задачи 7–10: руководство

Каждая задача — один язык: перевести 9 глав `docs/guide/<код>/*.md` с en (структура байт в байт: якоря, метки, ссылки, код; заголовки и проза — на языке перевода; для zh/ja метки без пробелов вокруг); устойчивые термины — в раздел «Слова руководства (<код>)» глоссария; проверки `pnpm i18n:check` (включая CJK-типографику глав), `pnpm test`.

### Task 7: Руководство на `zh-CN` — Commit `docs(guide): перевод руководства на китайский упрощённый`.
### Task 8: Руководство на `zh-TW` — Commit `docs(guide): перевод руководства на китайский традиционный`.
### Task 9: Руководство на `ja` — Commit `docs(guide): перевод руководства на японский`.
### Task 10: Руководство на `ko` — Commit `docs(guide): перевод руководства на корейский`.

### Task 11: Документация и смоук

**Files:** `README.md` (раздел «Языки»: 12 языков, все бесплатно), `docs/releases-repo/README.md` (строка руководства — все 12 языков), `docs/i18n/README.md`, `docs/i18n/review-checklist.md` (особенности проверки zh/ja/ko), Create `docs/smoke/e.md`.

- [ ] **Step 1: `docs/smoke/e.md`:**

```markdown
# Смоук E (азиатские языки, все языки бесплатно)

`pnpm tauri dev`, без ключа Pro (Free).

1. «Настройки» → в списке 12 языков без «— Pro»; выбор Deutsch без лицензии — интерфейс на немецком.
2. По очереди 简体中文, 繁體中文, 日本語, 한국어: интерфейс сразу на выбранном языке; иероглифы отображаются без «квадратов»; `document.documentElement.lang` = `zh-CN` / `zh-TW` / `ja` / `ko`.
3. Ширина: окно 1400 px и 900 px — кнопки, колонки списка топиков и групп, вкладки, левая панель, диалог импорта; ничего не обрезано.
4. Подсказки «?» и подсказки к параметрам топика на каждом новом языке.
5. «Справка» на каждом новом языке: жирные названия кнопок в тексте совпадают с интерфейсом; поиск по иероглифам находит разделы.
6. Язык ОС: основной язык macOS — 简体中文, 繁體中文 (Тайвань), 日本語, 한국어 → «Как в системе» выбирает соответствующий язык. Вернуть язык ОС.
7. Числа в списке топиков — разделители по языку.
```

- [ ] **Step 2: Полный прогон** — все проверки, включая cargo и `pnpm guide:publish` во временный каталог (12 языков в README).
- [ ] **Step 3: Commit** — `docs(i18n): документация и смоук азиатских языков`.

---

## После плана

- Финальное ревью ветки: удалены все следы «Pro-языков», сопоставление языка ОС, CJK-проверки без ложных срабатываний, качество четырёх переводов (выборочно), руководство.
- Ручной смоук пользователем по `docs/smoke/e.md`.
- Выборочная проверка носителями (особенно `ja`) — до выпуска.
