# C «Контекстные подсказки» Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Значок «?» с короткой всплывающей подсказкой у 43 непонятных полей и кнопок Glafira, на всех 8 языках.

**Architecture:** Один компонент `Hint` (`src/components/common/Hint.tsx`) на `@floating-ui/react`: наведение, фокус, клик-закрепление, Esc, одна открытая подсказка, портал. Тексты — раздел `hints` в словарях `src/locales/*.json`; `scripts/i18n-check.mjs` проверяет, что каждый использованный ключ существует, каждый ключ используется и en-текст не длиннее 200 символов. Задачи 3–8 подключают подсказки по экранам сразу на 8 языках.

**Tech Stack:** React 19, react-i18next 15 (`Trans`), `@floating-ui/react` 0.27, Vitest + Testing Library + user-event, node:test (скрипты).

**Спецификация:** `docs/superpowers/specs/2026-10-17-glafira-hints-design.md`.

## Global Constraints

- Подсказка — 1–2 фразы: что это и когда что выбирать. Без ссылок. en — не длиннее 200 символов (без учёта тегов).
- Ключи: `hints.<экран>.<поле>`; общий ключ `hints.label` = «Подсказка: {{name}}» (aria-label значка). Раздел `hints` — последний ключ верхнего уровня в каждом словаре.
- Названия параметров и значений Kafka (`acks`, `all`, `earliest`, `compact`, `host:port`, `key=value`) — в `<code>…</code>`, не переводятся.
- Двойные фигурные скобки в текстах подсказок — только параметры i18next; имена плейсхолдеров Producer в подсказках не пишутся.
- en — эталон ключей и тегов, ru — эталон смысла, Pro-языки (`de`, `fr`, `es`, `it`, `pl`, `pt-BR`) — по `docs/i18n/glossary.md`. Во всех 8 словарях одинаковый набор ключей и тегов.
- Значок: `<button type="button" className="hint-icon">?</button>`; показ при наведении через 300 мс, при фокусе сразу; скрытие через 100 мс; клик закрепляет; Esc и клик вне закрывают; одна открытая подсказка; `role="tooltip"` + `aria-describedby`.
- Поведение существующих элементов не меняется; строки `…Hint` под полями и системные `title` остаются.
- Проверки после каждой задачи: `pnpm i18n:check`, `pnpm test:scripts`, `pnpm test`, `pnpm build`.
- Коммиты заканчиваются пустой строкой и строкой атрибуции модели исполнителя (`Co-Authored-By: …`). Push только с согласия пользователя.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `src/components/common/Hint.tsx` (+ `.test.tsx`) | компонент подсказки |
| `src/styles.css` | `.hint-icon`, `.hint-tooltip` |
| `scripts/i18n-check.mjs` (+ `.test.mjs`) | `hintKeys`, `checkHints` |
| `src/locales/*.json` | раздел `hints` |
| `docs/i18n/glossary.md` | новые термины |
| экраны: `connections/ConnectionForm.tsx`, `connections/TlsSection.tsx`, `topics/CreateTopicForm.tsx`, `common/ConfigEditor.tsx`, `groups/ResetOffsetsPanel.tsx`, `groups/GroupList.tsx`, `browse/BrowseForm.tsx`, `browse/BrowsePanel.tsx`, `producer/ProducerPanel.tsx`, `producer/BatchPanel.tsx`, `consumer/ConsumerPanel.tsx`, `consumer/AssignmentPanel.tsx` | значки `<Hint>` |
| `docs/i18n/README.md`, `docs/smoke/c.md` | документация и смоук |

---

### Task 1: Компонент `Hint`

**Files:**
- Create: `src/components/common/Hint.tsx`, `src/components/common/Hint.test.tsx`
- Modify: `package.json` (зависимость), `src/styles.css`, `src/locales/{en,ru,de,fr,es,it,pl,pt-BR}.json` (ключ `hints.label`), `docs/i18n/glossary.md`

**Interfaces:**
- Produces: `export function Hint(props: { k: string; label: string }): JSX.Element` — `k` — ключ текста (`hints.…`), `label` — видимая подпись элемента для `aria-label` («Подсказка: {label}»). Текст рендерится через `<Trans i18nKey={k} components={{ code: <code /> }} />`.

- [ ] **Step 1: Зависимость**

Run: `pnpm add @floating-ui/react@^0.27`
Expected: `package.json` → `"@floating-ui/react": "^0.27.<x>"` в `dependencies`.

- [ ] **Step 2: Ключ `hints.label` во всех словарях** — добавить последним ключом верхнего уровня:

| Словарь | Значение `hints.label` |
|---|---|
| en | `Hint: {{name}}` |
| ru | `Подсказка: {{name}}` |
| de | `Hinweis: {{name}}` |
| fr | `Aide : {{name}}` (` ` — неразрывный пробел) |
| es | `Ayuda: {{name}}` |
| it | `Suggerimento: {{name}}` |
| pl | `Podpowiedź: {{name}}` |
| pt-BR | `Dica: {{name}}` |

То есть в конце каждого файла: `"hints": { "label": "…" }`.

- [ ] **Step 3: Глоссарий.** В `docs/i18n/glossary.md` в таблицу «Термины» добавить строки (перед строкой `| license |`):

```markdown
| idempotence | идемпотентность | Idempotenz | idempotence | idempotencia | idempotenza | idempotencja | idempotência |
| compression | сжатие | Komprimierung | compression | compresión | compressione | kompresja | compressão |
| rebalance | ребалансировка | Rebalance (das) | rééquilibrage | rebalanceo | ribilanciamento | rebalans | rebalanceamento |
| placeholder (Producer) | плейсхолдер | Platzhalter | variable | marcador | segnaposto | symbol zastępczy | marcador |
| hint (значок «?») | подсказка | Hinweis | aide | ayuda | suggerimento | podpowiedź | dica |
```

и в раздел «Не переводится» дописать в конец первого абзаца: `значения acks (0, 1, all), стратегии earliest/latest, пресеты delete/compact/test`.

- [ ] **Step 4: Написать падающий тест** `src/components/common/Hint.test.tsx`:

```tsx
import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { i18n } from "../../i18n";
import { Hint } from "./Hint";

beforeAll(() => {
  i18n.addResourceBundle("ru", "translation", { hints: { test: { a: "Первая <code>acks</code>", b: "Вторая" } } }, true, true);
});
beforeEach(() => vi.useFakeTimers({ shouldAdvanceTime: true }));
afterEach(() => vi.useRealTimers());

const setup = () => userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

describe("Hint", () => {
  it("значок с aria-label из подписи", () => {
    render(<Hint k="hints.test.a" label="acks" />);
    expect(screen.getByRole("button", { name: "Подсказка: acks" })).toHaveTextContent("?");
  });

  it("наведение: показ через 300 мс, уход курсора скрывает", async () => {
    const user = setup();
    render(<Hint k="hints.test.a" label="acks" />);
    const btn = screen.getByRole("button", { name: "Подсказка: acks" });
    await user.hover(btn);
    expect(screen.queryByRole("tooltip")).toBeNull();
    act(() => vi.advanceTimersByTime(300));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Первая acks");
    await user.unhover(btn);
    act(() => vi.advanceTimersByTime(150));
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("фокус с клавиатуры — сразу; Esc закрывает", async () => {
    const user = setup();
    render(<Hint k="hints.test.a" label="acks" />);
    await user.tab();
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("клик закрепляет, второй клик закрывает", async () => {
    const user = setup();
    render(<Hint k="hints.test.a" label="acks" />);
    const btn = screen.getByRole("button", { name: "Подсказка: acks" });
    await user.click(btn);
    await user.unhover(btn);
    act(() => vi.advanceTimersByTime(500));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    await user.click(btn);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("открыта только одна подсказка", async () => {
    const user = setup();
    render(
      <>
        <Hint k="hints.test.a" label="A" />
        <Hint k="hints.test.b" label="B" />
      </>,
    );
    await user.click(screen.getByRole("button", { name: "Подсказка: A" }));
    await user.click(screen.getByRole("button", { name: "Подсказка: B" }));
    const tips = screen.getAllByRole("tooltip");
    expect(tips).toHaveLength(1);
    expect(tips[0]).toHaveTextContent("Вторая");
  });

  it("aria-describedby указывает на подсказку, <code> сохраняется", async () => {
    const user = setup();
    render(<Hint k="hints.test.a" label="acks" />);
    const btn = screen.getByRole("button", { name: "Подсказка: acks" });
    await user.click(btn);
    const tip = screen.getByRole("tooltip");
    expect(btn).toHaveAttribute("aria-describedby", tip.id);
    expect(tip.querySelector("code")).toHaveTextContent("acks");
  });
});
```

- [ ] **Step 5: Убедиться, что тест падает**

Run: `pnpm vitest run src/components/common/Hint.test.tsx`
Expected: FAIL — `Failed to resolve import "./Hint"`.

- [ ] **Step 6: Реализовать** `src/components/common/Hint.tsx`:

```tsx
import { useEffect, useRef, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import {
  FloatingPortal,
  autoUpdate,
  flip,
  offset,
  safePolygon,
  shift,
  useDismiss,
  useFloating,
  useFocus,
  useHover,
  useInteractions,
  useRole,
} from "@floating-ui/react";

/** Закрывает открытую сейчас подсказку: в каждый момент открыта не больше одной. */
let closeOpenHint: (() => void) | null = null;

/** Значок «?» с короткой подсказкой: наведение (300 мс), фокус, клик закрепляет; Esc и клик вне закрывают. */
export function Hint({ k, label }: { k: string; label: string }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [pinned, setPinned] = useState(false);
  const close = () => {
    setPinned(false);
    setOpen(false);
  };
  const closeRef = useRef(close);
  closeRef.current = close;
  const self = useRef(() => closeRef.current());

  const { refs, floatingStyles, context } = useFloating({
    open,
    onOpenChange: (next, _event, reason) => {
      if (!next && pinned && (reason === "hover" || reason === "focus" || reason === "safe-polygon")) return;
      if (next) setOpen(true);
      else close();
    },
    placement: "top",
    middleware: [offset(6), flip(), shift({ padding: 8 })],
    whileElementsMounted: autoUpdate,
  });
  const hover = useHover(context, { delay: { open: 300, close: 100 }, handleClose: safePolygon() });
  const focus = useFocus(context);
  const dismiss = useDismiss(context);
  const role = useRole(context, { role: "tooltip" });
  const { getReferenceProps, getFloatingProps } = useInteractions([hover, focus, dismiss, role]);

  useEffect(() => {
    if (!open) return;
    const me = self.current;
    if (closeOpenHint && closeOpenHint !== me) closeOpenHint();
    closeOpenHint = me;
    return () => {
      if (closeOpenHint === me) closeOpenHint = null;
    };
  }, [open]);

  const onClick = () => {
    if (pinned) close();
    else {
      setPinned(true);
      setOpen(true);
    }
  };

  return (
    <>
      <button
        type="button"
        className="hint-icon"
        ref={refs.setReference}
        aria-label={t("hints.label", { name: label })}
        {...getReferenceProps({ onClick })}
      >
        ?
      </button>
      {open && (
        <FloatingPortal>
          <div ref={refs.setFloating} className="hint-tooltip" style={floatingStyles} {...getFloatingProps()}>
            <Trans i18nKey={k} components={{ code: <code /> }} />
          </div>
        </FloatingPortal>
      )}
    </>
  );
}
```

Если какой-то тест не проходит из-за особенностей jsdom (например, `safePolygon` закрывает сразу при `unhover`), исправлять реализацию, а не ослаблять тест; ожидания тестов — требования спеки §2.

- [ ] **Step 7: Стили** — дописать в конец `src/styles.css`:

```css
.hint-icon { display: inline-flex; align-items: center; justify-content: center; width: 14px; height: 14px; padding: 0; margin-left: 4px; border: 1px solid var(--muted); border-radius: 50%; background: none; color: var(--muted); font-size: 10px; line-height: 1; cursor: help; vertical-align: middle; }
.hint-icon:hover, .hint-icon:focus-visible { color: var(--accent); border-color: var(--accent); outline: none; }
.hint-tooltip { z-index: 1000; max-width: 320px; padding: 6px 8px; background: var(--bg); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15); font-size: 12px; line-height: 1.4; }
.hint-tooltip code { font-size: 11px; }
```

- [ ] **Step 8: Проверки**

Run: `pnpm vitest run src/components/common/Hint.test.tsx && pnpm test && pnpm i18n:check && pnpm build`
Expected: все PASS; `i18n:check` — `✓ словари de, en, es, fr, it, pl, pt-BR, ru согласованы …`.

- [ ] **Step 9: Commit**

```bash
git add package.json pnpm-lock.yaml src/components/common/Hint.tsx src/components/common/Hint.test.tsx src/styles.css src/locales docs/i18n/glossary.md
git commit -m "feat(hints): компонент подсказки «?» на Floating UI

Co-Authored-By: <модель исполнителя>"
```

---

### Task 2: Проверки подсказок в `i18n-check`

**Files:**
- Modify: `scripts/i18n-check.mjs`, `scripts/i18n-check.test.mjs`

**Interfaces:**
- Consumes: `flatten` из `scripts/i18n-check.mjs`; `srcFiles` в блоке запуска.
- Produces:
  - `hintKeys(files: Record<string, string>): { key: string; where: string }[]` — все строковые литералы `"hints.…"` / `'hints.…'` в нетестовых файлах (так находятся и `k="hints.x"`, и ключи в таблицах вида `{ earliest: "hints.groups.strategyEarliest" }`);
  - `checkHints(uses, enFlat, { maxLength = 200 } = {}): string[]`.

- [ ] **Step 1: Написать падающий тест** — дописать в `scripts/i18n-check.test.mjs` (и добавить `checkHints, hintKeys` в импорт):

```js
test("подсказки: ключи есть, все используются, длина en", () => {
  const files = {
    "src/a.tsx": '<Hint k="hints.a.x" label="X" />\nconst M = { s: "hints.a.y" };',
    "src/a.test.tsx": '<Hint k="hints.zzz" label="Z" />',
  };
  const uses = hintKeys(files);
  assert.deepEqual(uses.map((u) => u.key), ["hints.a.x", "hints.a.y"]);
  assert.equal(uses[0].where, "src/a.tsx:1");
  assert.equal(uses[1].where, "src/a.tsx:2");
  const en = flatten({ hints: { label: "Hint: {{name}}", a: { x: "Short <code>acks</code>", y: "y".repeat(201), z: "unused" } } });
  const p = checkHints([...uses, { key: "hints.missing", where: "src/b.tsx:3" }], en);
  assert.deepEqual(p.sort(), [
    "en: hints.a.y — 201 символов, больше 200",
    "en: hints.a.z не используется ни в одном <Hint> — удалите или подключите",
    "en: нет ключа hints.missing (подсказка, src/b.tsx:3)",
  ].sort());
  assert.deepEqual(checkHints([], flatten({ hints: { label: "Hint: {{name}}" } })), [], "hints.label не требует использования в <Hint>");
});
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `pnpm test:scripts`
Expected: FAIL — `does not provide an export named 'checkHints'`.

- [ ] **Step 3: Реализовать** — в `scripts/i18n-check.mjs` после `checkFrontendKeys`:

```js
const HINT_LITERAL = /["'](hints\.[\w.]+)["']/g;

/** Ключи подсказок во фронтенде: любой строковый литерал "hints.…" вне тестов. Возвращает [{ key, where }]. */
export function hintKeys(files) {
  const out = [];
  for (const [path, content] of Object.entries(files)) {
    if (/\.test\.[tj]sx?$/.test(path) || path.includes("/test/")) continue;
    for (const m of content.matchAll(HINT_LITERAL)) {
      out.push({ key: m[1], where: `${path}:${content.slice(0, m.index).split("\n").length}` });
    }
  }
  return out;
}

/** Каждая подсказка из кода есть в en; каждая подсказка en (кроме hints.label) используется; en не длиннее maxLength без тегов. */
export function checkHints(uses, enFlat, { maxLength = 200 } = {}) {
  const problems = [];
  const used = new Set(uses.map((u) => u.key));
  for (const { key, where } of uses) if (!enFlat.has(key)) problems.push(`en: нет ключа ${key} (подсказка, ${where})`);
  for (const [k, v] of enFlat) {
    if (!k.startsWith("hints.") || k === "hints.label") continue;
    if (!used.has(k)) problems.push(`en: ${k} не используется ни в одном <Hint> — удалите или подключите`);
    const plain = v.replace(/<[^>]+>/g, "");
    if (plain.length > maxLength) problems.push(`en: ${k} — ${plain.length} символов, больше ${maxLength}`);
  }
  return problems;
}
```

и в блоке запуска сразу после строки `problems.push(...checkFrontendKeys(frontendKeys(srcFiles), flatten(dicts.en)));`:

```js
  problems.push(...checkHints(hintKeys(srcFiles), flatten(dicts.en)));
```

- [ ] **Step 4: Тесты и проверка проходят**

Run: `pnpm test:scripts && pnpm i18n:check`
Expected: PASS; в en пока только `hints.label`, ошибок нет.

- [ ] **Step 5: Commit**

```bash
git add scripts/i18n-check.mjs scripts/i18n-check.test.mjs
git commit -m "feat(i18n): проверка ключей и длины подсказок

Co-Authored-By: <модель исполнителя>"
```

---

### Задачи 3–8: подсказки по экранам

Каждая задача — один экран (или пара близких), выполняется по одной процедуре:

1. Добавить в раздел `hints` всех 8 словарей ключи задачи (en и ru — дословно из таблицы задачи; de, fr, es, it, pl, pt-BR — перевод en по `docs/i18n/glossary.md` со сверкой смысла по ru; теги `<code>` — те же, что в en; во `fr` — U+00A0 перед `: ; ! ?` и внутри « »). Подраздел экрана (`hints.connections` и т. п.) — в порядке таблицы.
2. Поставить `<Hint k="…" label={…} />` в указанные места. `label` — видимая подпись элемента через существующий ключ (`t("…")`), как указано в таблице.
3. Дописать в тест экрана: для каждого значка, видимого в отрисованном состоянии теста, — `screen.getByRole("button", { name: "Подсказка: <ru-подпись>" })`; одну подсказку открыть кликом и проверить её ru-текст (`await screen.findByRole("tooltip")` → `toHaveTextContent`, `<code>` в `toHaveTextContent` даёт текст без тегов). Если значок появляется только в другом состоянии формы (например, TLS-поля при протоколе SSL), переключить состояние в тесте.
4. Проверки: `pnpm i18n:check` (в том числе «не используется ни в одном <Hint>» — значит, ключ забыт в коде), `pnpm vitest run <тесты экрана>`, `pnpm test`, `pnpm build`.
5. Отчёт о длине: `node scripts/i18n-check.mjs --lengths --no-cyrillic-scan | grep "hints\."` — подсказки длиннее en в ×1.6 и больше перечислить в отчёте задачи (сокращать, только если смысл не теряется).

Значок **никогда не ставится внутри `<label>`**: кнопка внутри подписи попадает в доступное имя поля (скринридер прочитает «Bootstrap-серверы Подсказка: …», а существующие `getByLabelText` сломаются). Правила:
- подпись в сетке формы (`<label htmlFor>` + поле) — обернуть: `<span className="field-label"><label htmlFor="cf-bootstrap">{t("connections.form.bootstrap")}</label><Hint k="hints.connections.bootstrap" label={t("connections.form.bootstrap")} /></span>` (обёртка занимает ячейку сетки вместо `<label>`);
- подпись, оборачивающая поле (`<label><input …/> текст</label>` или `<label>текст <select/></label>`), — значок сразу после закрывающего `</label>`;
- `<legend>`, `<h4>`, заголовок колонки, абзац — значок внутри после текста;
- кнопка — значок сразу после кнопки.
Где в таблицах задач написано «label …, после текста» или «внутри label», читать по этим правилам. Существующие тесты с `getByLabelText` должны проходить без изменений.

### Task 3: Подключение (14 подсказок)

**Files:**
- Modify: `src/components/connections/ConnectionForm.tsx`, `src/components/connections/TlsSection.tsx`, `src/components/connections/ConnectionForm.test.tsx`, `src/locales/*.json`

**Interfaces:**
- Consumes: `Hint` (Task 1), `checkHints` (Task 2).
- В `TlsSection.tsx` у `PathRow`, `FormatRow`, `PasswordRow` добавить необязательный проп `hint?: ReactNode`, который рендерится рядом с их `<label>` (не внутри: через `RowLabel` с обёрткой `.field-label`), чтобы не менять доступное имя поля.

| Ключ `hints.connections.…` | Где (label) | en | ru |
|---|---|---|---|
| `environment` | `ConnectionForm.tsx`, label `cf-env` (`connections.form.environment`) | Marks the connection with a color. prod connections (Pro) ask you to type the connection name before any change. | Помечает подключение цветом. Для prod-подключений (Pro) любое изменение нужно подтвердить вводом имени подключения. |
| `readOnly` | label `cf-ro` (`common.readOnly`) | Blocks everything that changes the cluster: sending, offset resets, deleting topics and groups, consumer sessions. | Запрещает всё, что меняет кластер: отправку, сброс оффсетов, удаление топиков и групп, consumer-сессии. |
| `bootstrap` | label `cf-bootstrap` (`connections.form.bootstrap`) | One or more <code>host:port</code>, comma-separated. The client discovers the other brokers by itself. | Один или несколько <code>host:port</code> через запятую. Остальные брокеры клиент найдёт сам. |
| `protocol` | label `cf-protocol` (`connections.form.protocol`) | PLAINTEXT — no encryption or login. SSL — TLS encryption. SASL_* — login with a password; SASL_SSL also encrypts. | PLAINTEXT — без шифрования и входа. SSL — шифрование TLS. SASL_* — вход по логину и паролю; SASL_SSL ещё и шифрует. |
| `saslMechanism` | label `cf-mech` (`connections.form.saslMechanism`) | Your Kafka admins know which one the cluster uses. SCRAM never sends the password itself; PLAIN without TLS sends it in clear text. | Какой механизм использует кластер, знают администраторы Kafka. SCRAM не передаёт сам пароль; PLAIN без TLS передаёт его открытым текстом. |
| `timeout` | label `cf-timeout` (`connections.form.timeout`) | How long to wait for a broker response, in milliseconds. Increase it for slow networks and VPNs. | Сколько ждать ответа брокера, в миллисекундах. Увеличьте для медленной сети или VPN. |
| `props` | label `cf-props` (`connections.form.props`) | Any librdkafka setting as <code>key=value</code>, one per line. Passwords and keys are not allowed here. | Любые параметры librdkafka в виде <code>key=value</code>, по одному на строку. Пароли и ключи сюда указывать нельзя. |
| `test` | сразу после кнопки «Проверить подключение» (`connections.form.test`), label — тот же ключ | Checks the settings, DNS and TCP, then Kafka (TLS, SASL, metadata). Nothing is saved or changed. | Проверяет настройки, DNS и TCP, затем Kafka (TLS, SASL, метаданные). Ничего не сохраняет и не меняет. |
| `ca` | `TlsSection.tsx`, `PathRow` CA, проп `hint` (`connections.tls.ca`) | The CA that signed the brokers' certificates. Leave empty if the system already trusts it. | CA, которым подписаны сертификаты брокеров. Оставьте пустым, если система уже доверяет этому CA. |
| `caFormat` | `FormatRow` CA (`connections.tls.caFormat`) | File format: PEM (text, <code>-----BEGIN</code>), PKCS#12 (<code>.p12</code>, <code>.pfx</code>) or JKS (Java keystore). | Формат файла: PEM (текст, <code>-----BEGIN</code>), PKCS#12 (<code>.p12</code>, <code>.pfx</code>) или JKS (хранилище Java). |
| `verifyHostname` | label `…-verify` (`connections.tls.verifyHostname`) | Checks that the broker certificate matches its host name. Turn it off only in test environments. | Проверять, что сертификат брокера выдан на его имя хоста. Отключайте только в тестовых средах. |
| `mtls` | label `…-mtls` (`connections.tls.mtls`) | Needed if the brokers authenticate clients by certificate. Your Kafka admins issue the certificate and key. | Нужен, если брокеры проверяют клиентов по сертификату. Сертификат и ключ выдают администраторы Kafka. |
| `alias` | label `…-alias` (`connections.tls.alias`) | The key entry name in the JKS or PKCS#12 file. Needed only if the file holds several keys. | Имя записи ключа в файле JKS или PKCS#12. Нужно, только если ключей в файле несколько. |
| `keyPassword` | `PasswordRow` пароля ключа (подпись `connections.tls.keyPasswordJks` / `keyPasswordPem` — та, что показана) | Only if the key has its own password: different from the keystore password, or an encrypted PEM key. | Только если у ключа свой пароль: отличается от пароля хранилища или PEM-ключ зашифрован. |

- [ ] **Step 1: Убедиться, что подсказок экрана нет**

Run: `grep -c "hints.connections" src/components/connections/ConnectionForm.tsx src/components/connections/TlsSection.tsx`
Expected: `0` для обоих файлов.

- [ ] **Step 2: Тексты во всех 8 словарях** (процедура п. 1).
- [ ] **Step 3: Значки в коде** (процедура п. 2, таблица выше; проп `hint` в `PathRow`/`FormatRow`/`PasswordRow`).
- [ ] **Step 4: Тест** (процедура п. 3) в `ConnectionForm.test.tsx`: значки формы по умолчанию + TLS-значки после выбора протокола `SSL` и включения mTLS; открыть `bootstrap` и проверить ru-текст «Один или несколько host:port через запятую. Остальные брокеры клиент найдёт сам.»
- [ ] **Step 5: Проверки** (процедура п. 4–5).

Run: `pnpm i18n:check && pnpm vitest run src/components/connections && pnpm test && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/components/connections src/locales
git commit -m "feat(hints): подсказки формы подключения

Co-Authored-By: <модель исполнителя>"
```

### Task 4: Топики и конфигурация (5 подсказок)

**Files:**
- Modify: `src/components/topics/CreateTopicForm.tsx`, `src/components/topics/CreateTopicForm.test.tsx`, `src/components/common/ConfigEditor.tsx`, `src/components/common/ConfigEditor.test.tsx`, `src/locales/*.json`

**Interfaces:**
- Consumes: `Hint` (Task 1), `checkHints` (Task 2).

| Ключ `hints.topics.…` | Где (label) | en | ru |
|---|---|---|---|
| `partitions` | `CreateTopicForm.tsx`, label `ct-partitions` (`topics.create.partitions`) | Partitions are read in parallel; order is guaranteed only within one partition. The count can be increased later but never decreased. | Партиции читаются параллельно; порядок гарантирован только внутри одной партиции. Число можно увеличить позже, но не уменьшить. |
| `rf` | label `ct-rf` (`topics.create.rf`) | How many copies of each partition the brokers keep. At most the number of brokers; 3 is the usual choice for prod. | Сколько копий каждой партиции хранят брокеры. Не больше числа брокеров; для prod обычно 3. |
| `preset` | label `ct-preset` (`topics.create.preset`) | <code>delete</code> removes old messages by time, <code>compact</code> keeps the latest value per key, <code>compact + delete</code> does both, <code>test</code> keeps messages for 1 hour. | <code>delete</code> удаляет старые сообщения по времени, <code>compact</code> хранит последнее значение каждого ключа, <code>compact + delete</code> — и то и другое, <code>test</code> хранит сообщения 1 час. |
| `extraConfigs` | label `ct-configs` (`topics.create.extraConfigs`) | Any topic setting as <code>key=value</code>, one per line, for example <code>retention.ms=86400000</code>. Overrides the preset. | Любые параметры топика в виде <code>key=value</code>, по одному на строку, например <code>retention.ms=86400000</code>. Перекрывают пресет. |
| `config` | `ConfigEditor.tsx`, сразу после кнопки «Проверить изменения» (`common.config.review`), label — тот же ключ | “reset to default” removes the override, and the broker default applies. Nothing changes until you review and confirm the changes. | «к default» удаляет переопределение — действует значение брокера. Ничего не меняется, пока вы не проверите и не подтвердите изменения. |

Перевод `config`: в кавычках — точная подпись ссылки `common.config.reset` этого языка.

- [ ] **Step 1: Убедиться, что подсказок нет**

Run: `grep -c "hints.topics" src/components/topics/CreateTopicForm.tsx src/components/common/ConfigEditor.tsx`
Expected: `0` для обоих.

- [ ] **Step 2: Тексты во всех 8 словарях** (процедура п. 1).
- [ ] **Step 3: Значки в коде** (процедура п. 2).
- [ ] **Step 4: Тесты** (процедура п. 3): `CreateTopicForm.test.tsx` — 4 значка, открыть `rf`, ru-текст «Сколько копий каждой партиции хранят брокеры. Не больше числа брокеров; для prod обычно 3.»; `ConfigEditor.test.tsx` — значок `config` рядом с «Проверить изменения».
- [ ] **Step 5: Проверки**

Run: `pnpm i18n:check && pnpm vitest run src/components/topics src/components/common && pnpm test && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/components/topics src/components/common src/locales
git commit -m "feat(hints): подсказки создания топика и конфигурации

Co-Authored-By: <модель исполнителя>"
```

### Task 5: Группы и сброс оффсетов (9 подсказок)

**Files:**
- Modify: `src/components/groups/ResetOffsetsPanel.tsx`, `src/components/groups/ResetOffsetsPanel.test.tsx`, `src/components/groups/GroupList.tsx`, `src/components/groups/GroupList.test.tsx`, `src/locales/*.json`

**Interfaces:**
- Consumes: `Hint` (Task 1), `checkHints` (Task 2).
- Стратегия выбирается в `<select id="rs-strategy">`, у `<option>` значков быть не может. Поэтому у подписи `rs-strategy` один значок, а его текст зависит от выбранной стратегии — через таблицу статических ключей в `ResetOffsetsPanel.tsx` (статические литералы нужны `hintKeys`):

```tsx
const STRATEGY_HINTS: Record<string, string> = {
  earliest: "hints.groups.strategyEarliest",
  latest: "hints.groups.strategyLatest",
  timestamp: "hints.groups.strategyTimestamp",
  offset: "hints.groups.strategyOffset",
  shift: "hints.groups.strategyShift",
};
```

Ключи таблицы — значения `form.strategy` этого компонента (сверить с типом стратегии в `reset.ts` и при расхождении взять настоящие значения). Значок: `<Hint k={STRATEGY_HINTS[form.strategy]} label={t("groups.reset.strategy")} />`.

| Ключ `hints.groups.…` | Где (label) | en | ru |
|---|---|---|---|
| `strategyEarliest` | label `rs-strategy`, стратегия earliest | The group will reread everything the topic still keeps. | Группа перечитает всё, что ещё хранится в топике. |
| `strategyLatest` | то же, latest | The group skips everything accumulated and starts with new messages. | Группа пропустит всё накопленное и начнёт с новых сообщений. |
| `strategyTimestamp` | то же, по времени | Each partition moves to the first message written at or after the given time. | Каждая партиция переходит к первому сообщению, записанному в указанное время или позже. |
| `strategyOffset` | то же, конкретный оффсет | Sets the exact offset for every selected partition. | Задаёт точный оффсет для каждой выбранной партиции. |
| `strategyShift` | то же, сдвиг ±N | Moves the current offset: −N rereads N messages back, +N skips N messages. | Сдвигает текущий оффсет: −N — перечитать N сообщений назад, +N — пропустить N сообщений. |
| `scope` | label `rs-scope` (`groups.reset.scope`) | Whole group, selected topics, or specific partitions as <code>topic:partition</code>. | Вся группа, выбранные топики или конкретные партиции в виде <code>topic:partition</code>. |
| `onlyEmpty` | в конце абзаца `groups.reset.onlyEmpty`, label `groups.reset.title` | Active members would overwrite the new offsets with their own commits. Stop the group's applications first. | Активные участники перезапишут новые оффсеты своими коммитами. Сначала остановите приложения группы. |
| `preview` | сразу после кнопки «Предпросмотр» (`groups.reset.preview`) | Shows current and new offsets per partition. Nothing changes until you apply. | Показывает текущие и новые оффсеты по партициям. Ничего не меняется, пока вы не нажмёте «Применить». |
| `lag` | `GroupList.tsx`, заголовок колонки (`groups.list.colLag`) — единственное исключение из правила «без значков в заголовках колонок» | How many messages the group has not read yet: the end of the log minus the committed offset. | Сколько сообщений группа ещё не прочитала: конец лога минус закоммиченный оффсет. |

- [ ] **Step 1: Убедиться, что подсказок нет**

Run: `grep -c "hints.groups" src/components/groups/ResetOffsetsPanel.tsx src/components/groups/GroupList.tsx`
Expected: `0` для обоих.

- [ ] **Step 2: Тексты во всех 8 словарях** (процедура п. 1).
- [ ] **Step 3: Значки в коде** (процедура п. 2; `STRATEGY_HINTS`).
- [ ] **Step 4: Тесты** (процедура п. 3): `ResetOffsetsPanel.test.tsx` — значок стратегии; при earliest открыть и проверить «Группа перечитает всё, что ещё хранится в топике.», переключить на latest — текст «Группа пропустит всё накопленное и начнёт с новых сообщений.»; значки scope, onlyEmpty, preview. `GroupList.test.tsx` — значок в колонке лага.
- [ ] **Step 5: Проверки**

Run: `pnpm i18n:check && pnpm vitest run src/components/groups && pnpm test && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/components/groups src/locales
git commit -m "feat(hints): подсказки сброса оффсетов и лага

Co-Authored-By: <модель исполнителя>"
```

### Task 6: Просмотр сообщений (4 подсказки)

**Files:**
- Modify: `src/components/browse/BrowseForm.tsx`, `src/components/browse/BrowseForm.test.tsx`, `src/components/browse/BrowsePanel.tsx`, `src/locales/*.json`

**Interfaces:**
- Consumes: `Hint` (Task 1), `checkHints` (Task 2).

| Ключ `hints.browse.…` | Где (label) | en | ru |
|---|---|---|---|
| `startPosition` | `BrowseForm.tsx`, сразу после `<select aria-label={t("browse.form.startPosition")}>`; label `browse.form.startPosition` | Where to start reading: the latest N messages, from the beginning, from an offset, from a point in time, or the last X minutes. | Откуда начать чтение: последние N сообщений, с начала, с оффсета, с момента времени или за последние X минут. |
| `limit` | внутри label с `browse.form.limit`, после текста | How many messages to show. Large values make reading slower. | Сколько сообщений показать. Большие значения замедляют чтение. |
| `format` | в `<legend>` с `browse.form.format`, после текста | How to decode keys and values: string, JSON, hex or base64. Only the display changes, not the data. | Как декодировать ключи и значения: строка, JSON, hex или base64. Меняется только отображение, не данные. |
| `readerNotGroup` | `BrowsePanel.tsx`, сразу после кнопки «Читать» (`browse.panel.read`), label — тот же ключ | Browsing does not join a consumer group and commits nothing, so the cluster is not changed. | Просмотр не вступает в consumer group и ничего не коммитит — кластер не меняется. |

- [ ] **Step 1: Убедиться, что подсказок нет**

Run: `grep -c "hints.browse" src/components/browse/BrowseForm.tsx src/components/browse/BrowsePanel.tsx`
Expected: `0` для обоих.

- [ ] **Step 2: Тексты во всех 8 словарях** (процедура п. 1).
- [ ] **Step 3: Значки в коде** (процедура п. 2).
- [ ] **Step 4: Тест** (процедура п. 3) в `BrowseForm.test.tsx`: значки `startPosition`, `limit`, `format`; открыть `limit` — «Сколько сообщений показать. Большие значения замедляют чтение.» Значок `readerNotGroup` проверяется тестом, который уже рендерит `BrowsePanel` (найти по `grep -l "BrowsePanel" src/components/**/*.test.tsx`); если такого нет — добавить в `BrowseForm.test.tsx` отдельный рендер `BrowsePanel` с теми же моками, что использует `useBrowseSession.test.tsx`.
- [ ] **Step 5: Проверки**

Run: `pnpm i18n:check && pnpm vitest run src/components/browse && pnpm test && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/components/browse src/locales
git commit -m "feat(hints): подсказки просмотра сообщений

Co-Authored-By: <модель исполнителя>"
```

### Task 7: Producer (7 подсказок)

**Files:**
- Modify: `src/components/producer/ProducerPanel.tsx`, `src/components/producer/ProducerPanel.test.tsx`, `src/components/producer/BatchPanel.tsx`, `src/components/producer/BatchPanel.test.tsx`, `src/locales/*.json`

**Interfaces:**
- Consumes: `Hint` (Task 1), `checkHints` (Task 2).

| Ключ `hints.producer.…` | Где (label) | en | ru |
|---|---|---|---|
| `partition` | `ProducerPanel.tsx`, label `…-partition` (`producer.partition`) | Leave empty to let Kafka choose the partition by key. Enter a number to send to a specific partition. | Оставьте пустым — партицию выберет Kafka по ключу. Укажите номер, чтобы отправить в конкретную партицию. |
| `formats` | `PayloadField`, label формата (`producer.keyFormat` / `producer.valueFormat` — какой показан); значок есть у ключа и у значения | How to encode the text before sending: string as is, JSON (validated), hex or base64 for binary data. | Как закодировать текст перед отправкой: строка как есть, JSON (с проверкой), hex или base64 для двоичных данных. |
| `placeholders` | в конце абзаца `producer.placeholders`; label `producer.settings.legend` | Placeholders are replaced on every send: a new UUID, the current time, the copy number and so on. | Плейсхолдеры подставляются заново при каждой отправке: новый UUID, текущее время, номер копии и т. д. |
| `acks` | label с `acks`, после текста `acks`; label `"acks"` (строка, не переводится) | How many replicas must confirm a write. <code>all</code> is the safest, <code>1</code> is faster, <code>0</code> does not wait and messages can be lost. | Сколько реплик должны подтвердить запись. <code>all</code> — надёжнее всего, <code>1</code> — быстрее, <code>0</code> — без ожидания, сообщения могут теряться. |
| `compression` | label с `producer.settings.compression`, после текста | Compresses batches: less traffic and disk space, a bit more CPU. <code>zstd</code> and <code>lz4</code> are common choices. | Сжимает пакеты: меньше трафика и места на диске, немного больше CPU. Частый выбор — <code>zstd</code> и <code>lz4</code>. |
| `idempotence` | label с чекбоксом `producer.settings.idempotence`, после текста | Prevents duplicates when the client retries a send. Requires <code>acks = all</code>. | Защищает от дублей при повторной отправке. Требует <code>acks = all</code>. |
| `batchRate` | `BatchPanel.tsx`, label с `producer.batch.rate`, после текста | Limits batch sending to N messages per second so the cluster is not overloaded. | Ограничивает пакетную отправку N сообщениями в секунду, чтобы не перегрузить кластер. |

Подпись `acks` — литерал `"acks"`: значок получит aria-label «Подсказка: acks» на всех языках.

- [ ] **Step 1: Убедиться, что подсказок нет**

Run: `grep -c "hints.producer" src/components/producer/ProducerPanel.tsx src/components/producer/BatchPanel.tsx`
Expected: `0` для обоих.

- [ ] **Step 2: Тексты во всех 8 словарях** (процедура п. 1).
- [ ] **Step 3: Значки в коде** (процедура п. 2).
- [ ] **Step 4: Тесты** (процедура п. 3): `ProducerPanel.test.tsx` — значки partition, formats (2 шт., `getAllByRole`), placeholders, acks, compression, idempotence; открыть `acks` — «Сколько реплик должны подтвердить запись. all — надёжнее всего, 1 — быстрее, 0 — без ожидания, сообщения могут теряться.»; `BatchPanel.test.tsx` — значок batchRate.
- [ ] **Step 5: Проверки**

Run: `pnpm i18n:check && pnpm vitest run src/components/producer && pnpm test && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/components/producer src/locales
git commit -m "feat(hints): подсказки Producer

Co-Authored-By: <модель исполнителя>"
```

### Task 8: Consumer-сессии (4 подсказки)

**Files:**
- Modify: `src/components/consumer/ConsumerPanel.tsx`, `src/components/consumer/ConsumerPanel.test.tsx`, `src/components/consumer/AssignmentPanel.tsx`, `src/locales/*.json`

**Interfaces:**
- Consumes: `Hint` (Task 1), `checkHints` (Task 2).

| Ключ `hints.consumer.…` | Где (label) | en | ru |
|---|---|---|---|
| `join` | `ConsumerPanel.tsx`, в конце предупреждения `consumer.warn`; label `consumer.form.groupTopics` | Unlike browsing, a session is a real group member: the group rebalances, and commits change its offsets. | В отличие от просмотра, сессия — настоящий участник группы: группа ребалансируется, а коммиты меняют её оффсеты. |
| `autoCommit` | label с чекбоксом `consumer.form.autoCommit`, после текста | Commits read offsets automatically in the background. Without it, commit manually with the commit buttons. | Автоматически коммитит прочитанные оффсеты в фоне. Без него — вручную, кнопками коммита. |
| `commit` | сразу после кнопки `consumer.actions.commitAll`; label `consumer.actions.commitAll` | “Commit up to selected” commits the offset after the selected message; “Commit everything read” commits all messages read so far. | «Закоммитить до выбранного» коммитит оффсет после выбранного сообщения; «Закоммитить всё прочитанное» — все прочитанные сообщения. |
| `pause` | `AssignmentPanel.tsx`, в `<h4>` с `consumer.assignment.title`, после текста; label `consumer.assignment.pause` | Pausing stops reading a partition without leaving the group, so no rebalance happens. | Пауза останавливает чтение партиции, не выходя из группы, — ребалансировки не будет. |

Перевод `commit`: в кавычках — точные подписи кнопок `consumer.actions.commitSelected` и `consumer.actions.commitAll` этого языка.

- [ ] **Step 1: Убедиться, что подсказок нет**

Run: `grep -c "hints.consumer" src/components/consumer/ConsumerPanel.tsx src/components/consumer/AssignmentPanel.tsx`
Expected: `0` для обоих.

- [ ] **Step 2: Тексты во всех 8 словарях** (процедура п. 1).
- [ ] **Step 3: Значки в коде** (процедура п. 2).
- [ ] **Step 4: Тесты** (процедура п. 3) в `ConsumerPanel.test.tsx`: значки join и autoCommit в форме; commit и pause — в состоянии запущенной сессии (как в существующих тестах сессии); открыть `autoCommit` — «Автоматически коммитит прочитанные оффсеты в фоне. Без него — вручную, кнопками коммита.»
- [ ] **Step 5: Проверки**

Run: `pnpm i18n:check && pnpm vitest run src/components/consumer && pnpm test && pnpm build`
Expected: PASS; `grep -o '"hints\.[a-z]*\.[A-Za-z]*"' -rh src/components | sort -u | wc -l` → `43`.

- [ ] **Step 6: Commit**

```bash
git add src/components/consumer src/locales
git commit -m "feat(hints): подсказки consumer-сессий

Co-Authored-By: <модель исполнителя>"
```

---

### Task 9: Документация и смоук

**Files:**
- Modify: `docs/i18n/README.md`
- Create: `docs/smoke/c.md`

- [ ] **Step 1: Раздел в `docs/i18n/README.md`** — добавить перед разделом «## Псевдоязык»:

```markdown
## Подсказки
Значок «?» — компонент `<Hint k="hints.<экран>.<поле>" label={t("…подпись…")} />` (`src/components/common/Hint.tsx`).
Текст — 1–2 фразы: что это и когда что выбирать; en не длиннее 200 символов; параметры и значения Kafka — в `<code>`.
Новая подсказка: ключ во всех словарях (раздел `hints`, последний ключ верхнего уровня) и значок в коде.
`pnpm i18n:check` не пропустит ключ без значка, значок без ключа и слишком длинный en-текст.
Ключ, выбираемый по состоянию (как стратегия сброса оффсетов), — статическими литералами в таблице, а не шаблонной строкой.
```

- [ ] **Step 2: Создать `docs/smoke/c.md`:**

```markdown
# Смоук C (подсказки)

`pnpm tauri dev`, Kafka: `docker compose -f dev/docker-compose.yml up -d`, подключение `localhost:9092`.

1. Подключение (новое и существующее): значки «?» у окружения, read-only, bootstrap, протокола, таймаута, доп. свойств,
   кнопки «Проверить подключение»; протокол SASL_SSL — механизм SASL и TLS-поля (CA, формат, проверка имени, mTLS, псевдоним, пароль ключа).
2. Наведение — подсказка через ~0,3 с; уход курсора — исчезает; курсор на подсказку — остаётся, текст выделяется.
3. Клик по «?» — подсказка закреплена; второй клик, Esc или клик в стороне — закрыта. Открыть одну, затем другую — первая закрылась.
4. Tab до «?» — подсказка сразу; Shift+Tab — исчезает.
5. Края: «?» у правого края окна и в левой панели — подсказка не обрезана, сдвинута внутрь; у верхнего края — открывается вниз.
6. Создание топика (партиции, RF, пресет, доп. конфиги), конфигурация топика («Проверить изменения»), сброс оффсетов
   (смена стратегии меняет текст подсказки; область, только пустая группа, предпросмотр), колонка «Лаг» в списке групп.
7. Просмотр (позиция, лимит, формат, «Читать»), Producer (партиция, форматы ключа и значения, плейсхолдеры, acks, сжатие,
   идемпотентность, скорость пакетной отправки), consumer-сессия (предупреждение, автокоммит, кнопки коммита, назначение партиций).
8. Тёмная тема macOS — подсказка читается, рамка видна. Окно 900 px — ничего не обрезано.
9. Язык Deutsch (Pro) и English — тексты подсказок на языке интерфейса; `acks`, `all`, `earliest` — моноширинным шрифтом.
```

- [ ] **Step 3: Полный прогон**

Run: `pnpm test && pnpm test:scripts && pnpm i18n:check && pnpm build`
Expected: всё PASS.

- [ ] **Step 4: Commit**

```bash
git add docs/i18n/README.md docs/smoke/c.md
git commit -m "docs(hints): раздел о подсказках и смоук C

Co-Authored-By: <модель исполнителя>"
```

---

### Задачи 10–11: подсказки к параметрам топика

Общая процедура — как в задачах 3–8 (п. 1, 3–5), с отличиями:
- ключи — в подразделе `hints.topicConfig` (после `consumer`), имя ключа — имя параметра с `_` вместо `.` (`retention.ms` → `retention_ms`), в порядке таблицы задачи;
- значки ставит не каждая задача отдельно, а таблица `TOPIC_CONFIG_HINTS` (Task 10 создаёт механизм, Task 11 только дополняет таблицу и словари);
- `<code>` — для значений и имён параметров, как в en; числа, единицы (`ms`, `MB`, `GiB`) и названия Kafka-версий — как в en.

Глоссарий (Task 10, шаг 1) — добавить в таблицу «Термины» перед строкой `| license |`:

```markdown
| segment (лога) | сегмент | Segment | segment | segmento | segmento | segment | segmento |
| compaction | compaction | Compaction (die) | compaction (f.) | compactación | compattazione | kompaktowanie | compactação |
| tiered storage | tiered storage | Tiered Storage | tiered storage | tiered storage | tiered storage | tiered storage | tiered storage |
| follower (реплика) | фолловер | Follower | follower | follower | follower | follower | follower |
```

и в «Не переводится» дописать: `значения CreateTime / LogAppendTime, producer / uncompressed в compression.type`.

### Task 10: Механизм и параметры хранения (16 подсказок)

**Files:**
- Create: `src/components/common/topicConfigHints.ts`
- Modify: `src/components/common/ConfigTable.tsx`, `src/components/common/ConfigTable.test.tsx`, `src/components/common/ConfigEditor.tsx`, `src/components/common/ConfigEditor.test.tsx`, `src/components/topics/TopicDetails.tsx`, `src/locales/*.json`, `docs/i18n/glossary.md`

**Interfaces:**
- Consumes: `Hint` (Task 1), `checkHints`/`hintKeys` (Task 2 — литералы в таблице считаются использованием).
- Produces: `export const TOPIC_CONFIG_HINTS: Record<string, string>`; проп `hints?: Record<string, string>` у `ConfigTable` и `ConfigEditor`.

- [ ] **Step 1: Глоссарий** (блок выше).

- [ ] **Step 2: Падающие тесты.** В `ConfigTable.test.tsx`:

```tsx
it("подсказки параметров: значок только у известных, только при переданном hints", async () => {
  const entries = [
    { name: "retention.ms", value: "604800000", isDefault: true, isReadOnly: false, isSensitive: false, source: "DEFAULT_CONFIG" },
    { name: "x.unknown", value: "1", isDefault: true, isReadOnly: false, isSensitive: false, source: "DEFAULT_CONFIG" },
  ];
  const { unmount } = render(<ConfigTable entries={entries} hints={TOPIC_CONFIG_HINTS} />);
  expect(screen.getByRole("button", { name: "Подсказка: retention.ms" })).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Подсказка: x.unknown" })).toBeNull();
  await userEvent.click(screen.getByRole("button", { name: "Подсказка: retention.ms" }));
  expect(await screen.findByRole("tooltip")).toHaveTextContent("Сколько хранить сообщения, в мс.");
  unmount();
  render(<ConfigTable entries={entries} />);
  expect(screen.queryByRole("button", { name: /^Подсказка:/ })).toBeNull();
});
```

(поля `ConfigEntry` сверить с `src/api/types.ts` и существующими тестами файла; импорт `TOPIC_CONFIG_HINTS` из `./topicConfigHints`). В `ConfigEditor.test.tsx` — тот же сценарий для режима редактирования (значок у `retention.ms`, нет у неизвестного).

- [ ] **Step 3: Убедиться, что тесты падают**

Run: `pnpm vitest run src/components/common/ConfigTable.test.tsx src/components/common/ConfigEditor.test.tsx`
Expected: FAIL — нет модуля `./topicConfigHints`.

- [ ] **Step 4: Реализовать.** `src/components/common/topicConfigHints.ts`:

```ts
/** Подсказки к параметрам уровня топика: имя параметра → ключ словаря. Параметр не из списка — без значка. */
export const TOPIC_CONFIG_HINTS: Record<string, string> = {
  "cleanup.policy": "hints.topicConfig.cleanup_policy",
  "retention.ms": "hints.topicConfig.retention_ms",
  "retention.bytes": "hints.topicConfig.retention_bytes",
  "delete.retention.ms": "hints.topicConfig.delete_retention_ms",
  "min.compaction.lag.ms": "hints.topicConfig.min_compaction_lag_ms",
  "max.compaction.lag.ms": "hints.topicConfig.max_compaction_lag_ms",
  "min.cleanable.dirty.ratio": "hints.topicConfig.min_cleanable_dirty_ratio",
  "segment.ms": "hints.topicConfig.segment_ms",
  "segment.bytes": "hints.topicConfig.segment_bytes",
  "segment.jitter.ms": "hints.topicConfig.segment_jitter_ms",
  "file.delete.delay.ms": "hints.topicConfig.file_delete_delay_ms",
  "local.retention.ms": "hints.topicConfig.local_retention_ms",
  "local.retention.bytes": "hints.topicConfig.local_retention_bytes",
  "remote.storage.enable": "hints.topicConfig.remote_storage_enable",
  "remote.log.copy.disable": "hints.topicConfig.remote_log_copy_disable",
  "remote.log.delete.on.disable": "hints.topicConfig.remote_log_delete_on_disable",
};
```

В `ConfigTable` и `ConfigEditor` — проп `hints?: Record<string, string>`; в ячейке имени после `{e.name}` (и после значка 🔒 в `ConfigTable`): `{hints?.[e.name] && <Hint k={hints[e.name]} label={e.name} />}`. В `TopicDetails.tsx` передать `hints={TOPIC_CONFIG_HINTS}` обоим (строки с `<ConfigEditor` и `<ConfigTable entries={q.data} />`). `ClusterOverview.tsx` не менять.

- [ ] **Step 5: Тексты во всех 8 словарях** (`hints.topicConfig`, порядок таблицы):

| Ключ | en | ru |
|---|---|---|
| `cleanup_policy` | <code>delete</code> removes old segments by time or size, <code>compact</code> keeps only the latest value per key, <code>compact,delete</code> does both. | <code>delete</code> удаляет старые сегменты по времени или размеру, <code>compact</code> хранит только последнее значение каждого ключа, <code>compact,delete</code> — и то и другое. |
| `retention_ms` | How long messages are kept, in ms. Default 604800000 (7 days); <code>-1</code> keeps them forever. Applies when <code>cleanup.policy</code> includes <code>delete</code>. | Сколько хранить сообщения, в мс. По умолчанию 604800000 (7 дней); <code>-1</code> — бессрочно. Действует, если <code>cleanup.policy</code> содержит <code>delete</code>. |
| `retention_bytes` | Maximum size of each partition before old segments are deleted. Default <code>-1</code>: no size limit, only time. | Максимальный размер каждой партиции, после которого удаляются старые сегменты. По умолчанию <code>-1</code> — без ограничения, только по времени. |
| `delete_retention_ms` | How long delete markers (null values) stay in a compacted topic so that readers can see the deletion. Default 1 day. | Сколько хранить метки удаления (значения null) в compacted-топике, чтобы читающие приложения успели увидеть удаление. По умолчанию 1 день. |
| `min_compaction_lag_ms` | The minimum age of a message before compaction may remove it. Default 0. | Минимальный возраст сообщения, раньше которого compaction его не удалит. По умолчанию 0. |
| `max_compaction_lag_ms` | The maximum time a message may stay uncompacted, to guarantee that old values are removed in time. Default: unlimited. | Максимальное время до compaction сообщения — чтобы старые значения гарантированно удалялись вовремя. По умолчанию не ограничено. |
| `min_cleanable_dirty_ratio` | Compaction starts when this share of the log is uncompacted. Lower means more frequent compaction and fewer duplicates, but more disk load. Default 0.5. | Compaction запускается, когда такая доля лога ещё не обработана. Меньше — чаще compaction и меньше дублей, но больше нагрузка на диск. По умолчанию 0.5. |
| `segment_ms` | How often a new log segment is started, even if the current one is not full. Retention and compaction work on whole segments. Default 7 days. | Как часто начинать новый сегмент лога, даже если текущий не заполнен. Хранение и compaction работают целыми сегментами. По умолчанию 7 дней. |
| `segment_bytes` | Size of one log segment file. Retention and compaction work on whole segments. Default 1 GiB. | Размер одного файла сегмента лога. Хранение и compaction работают целыми сегментами. По умолчанию 1 GiB. |
| `segment_jitter_ms` | Up to this much random time is subtracted from <code>segment.ms</code>, so that many partitions do not start new segments at the same moment. Default 0. | Случайное время до этого значения вычитается из <code>segment.ms</code>, чтобы множество партиций не начинали новый сегмент одновременно. По умолчанию 0. |
| `file_delete_delay_ms` | How long a deleted segment file stays on disk before it is actually removed. Default 1 minute. | Сколько удалённый файл сегмента остаётся на диске до физического удаления. По умолчанию 1 минута. |
| `local_retention_ms` | With tiered storage: how long segments stay on the broker's local disk before only the remote copy remains. <code>-2</code> means the same as <code>retention.ms</code>. | При tiered storage: сколько сегменты хранятся на локальном диске брокера, прежде чем останется только удалённая копия. <code>-2</code> — как <code>retention.ms</code>. |
| `local_retention_bytes` | With tiered storage: local size limit per partition; older segments remain only in remote storage. <code>-2</code> means the same as <code>retention.bytes</code>. | При tiered storage: предел локального размера партиции; более старые сегменты остаются только во внешнем хранилище. <code>-2</code> — как <code>retention.bytes</code>. |
| `remote_storage_enable` | Enables tiered storage: closed segments are copied to remote storage (for example, S3) and later removed locally. The cluster must support it. | Включает tiered storage: закрытые сегменты копируются во внешнее хранилище (например, S3), а локально позже удаляются. Кластер должен это поддерживать. |
| `remote_log_copy_disable` | Stops copying new segments to remote storage; data already there stays readable. Used to turn tiered storage off step by step. | Прекращает копирование новых сегментов во внешнее хранилище; уже перенесённые данные остаются доступны. Нужен для поэтапного отключения tiered storage. |
| `remote_log_delete_on_disable` | When tiered storage is turned off for the topic, delete its data in remote storage. Must be <code>true</code> to turn tiered storage off. | При отключении tiered storage для топика удалить его данные во внешнем хранилище. Для отключения tiered storage должен быть <code>true</code>. |

- [ ] **Step 6: Проверки**

Run: `pnpm i18n:check && pnpm vitest run src/components/common src/components/topics && pnpm test && pnpm build`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/components/common src/components/topics src/locales docs/i18n/glossary.md
git commit -m "feat(hints): подсказки к параметрам топика — механизм, хранение и compaction

Co-Authored-By: <модель исполнителя>"
```

### Task 11: Остальные параметры топика (20 подсказок)

**Files:**
- Modify: `src/components/common/topicConfigHints.ts`, `src/components/common/ConfigTable.test.tsx`, `src/locales/*.json`

**Interfaces:**
- Consumes: `TOPIC_CONFIG_HINTS` и проп `hints` (Task 10).

- [ ] **Step 1: Дополнить таблицу** `TOPIC_CONFIG_HINTS` (после `remote.log.delete.on.disable`, в этом порядке):

```ts
  "compression.type": "hints.topicConfig.compression_type",
  "compression.gzip.level": "hints.topicConfig.compression_gzip_level",
  "compression.lz4.level": "hints.topicConfig.compression_lz4_level",
  "compression.zstd.level": "hints.topicConfig.compression_zstd_level",
  "max.message.bytes": "hints.topicConfig.max_message_bytes",
  "min.insync.replicas": "hints.topicConfig.min_insync_replicas",
  "unclean.leader.election.enable": "hints.topicConfig.unclean_leader_election_enable",
  "message.timestamp.type": "hints.topicConfig.message_timestamp_type",
  "message.timestamp.before.max.ms": "hints.topicConfig.message_timestamp_before_max_ms",
  "message.timestamp.after.max.ms": "hints.topicConfig.message_timestamp_after_max_ms",
  "message.timestamp.difference.max.ms": "hints.topicConfig.message_timestamp_difference_max_ms",
  "message.format.version": "hints.topicConfig.message_format_version",
  "message.downconversion.enable": "hints.topicConfig.message_downconversion_enable",
  "flush.messages": "hints.topicConfig.flush_messages",
  "flush.ms": "hints.topicConfig.flush_ms",
  "index.interval.bytes": "hints.topicConfig.index_interval_bytes",
  "segment.index.bytes": "hints.topicConfig.segment_index_bytes",
  "preallocate": "hints.topicConfig.preallocate",
  "leader.replication.throttled.replicas": "hints.topicConfig.leader_replication_throttled_replicas",
  "follower.replication.throttled.replicas": "hints.topicConfig.follower_replication_throttled_replicas",
```

- [ ] **Step 2: Тексты во всех 8 словарях** (после ключей Task 10):

| Ключ | en | ru |
|---|---|---|
| `compression_type` | How the broker stores messages: <code>producer</code> keeps the producer's compression; otherwise it recompresses to <code>gzip</code>, <code>snappy</code>, <code>lz4</code>, <code>zstd</code> or <code>uncompressed</code>. | Как брокер хранит сообщения: <code>producer</code> — сжатие как у producer; иначе пересжимает в <code>gzip</code>, <code>snappy</code>, <code>lz4</code>, <code>zstd</code> или <code>uncompressed</code>. |
| `compression_gzip_level` | gzip level when the broker recompresses to gzip: 1 is the fastest, 9 the smallest. Kafka 3.8+. | Уровень gzip при пересжатии брокером: 1 — быстрее, 9 — компактнее. Kafka 3.8+. |
| `compression_lz4_level` | lz4 level when the broker recompresses to lz4: higher is smaller and slower. Kafka 3.8+. | Уровень lz4 при пересжатии брокером: выше — компактнее и медленнее. Kafka 3.8+. |
| `compression_zstd_level` | zstd level when the broker recompresses to zstd: higher is smaller and slower. Kafka 3.8+. | Уровень zstd при пересжатии брокером: выше — компактнее и медленнее. Kafka 3.8+. |
| `max_message_bytes` | Largest batch the broker accepts for this topic; larger messages are rejected. Producers and consumers must allow the same size. Default about 1 MB. | Максимальный пакет, который брокер примет для топика; более крупные сообщения отклоняются. Producer и consumer должны допускать такой же размер. По умолчанию около 1 MB. |
| `min_insync_replicas` | How many replicas must confirm a write with <code>acks=all</code>, otherwise the producer gets an error. Usually 2 with replication factor 3. | Сколько реплик должны подтвердить запись при <code>acks=all</code>, иначе producer получит ошибку. Обычно 2 при факторе репликации 3. |
| `unclean_leader_election_enable` | Lets an out-of-sync replica become leader when no in-sync replica is left. The topic stays available, but messages may be lost. | Разрешает несинхронной реплике стать лидером, если синхронных не осталось. Топик остаётся доступен, но сообщения могут потеряться. |
| `message_timestamp_type` | <code>CreateTime</code>: the timestamp set by the producer. <code>LogAppendTime</code>: the time the broker wrote the message. | <code>CreateTime</code> — метка времени от producer, <code>LogAppendTime</code> — время записи сообщения брокером. |
| `message_timestamp_before_max_ms` | How far in the past a producer timestamp may be with <code>CreateTime</code>; older messages are rejected. | Насколько в прошлом может быть метка времени producer при <code>CreateTime</code>; более старые сообщения отклоняются. |
| `message_timestamp_after_max_ms` | How far in the future a producer timestamp may be with <code>CreateTime</code>; messages beyond it are rejected. | Насколько в будущем может быть метка времени producer при <code>CreateTime</code>; сообщения дальше отклоняются. |
| `message_timestamp_difference_max_ms` | Deprecated, replaced by the two <code>message.timestamp.*.max.ms</code> settings: the allowed gap between producer and broker time. Removed in Kafka 4.0. | Устаревший, заменён двумя <code>message.timestamp.*.max.ms</code>: допустимое расхождение времени producer и брокера. Удалён в Kafka 4.0. |
| `message_format_version` | Deprecated: the message format for old clients. Ignored since Kafka 3.0 and removed in 4.0. | Устаревший: формат сообщений для старых клиентов. Игнорируется с Kafka 3.0, удалён в 4.0. |
| `message_downconversion_enable` | Deprecated: lets the broker convert messages for very old consumers. Costs CPU; removed in Kafka 4.0. | Устаревший: брокер переводит сообщения в формат очень старых consumer. Расходует CPU; удалён в Kafka 4.0. |
| `flush_messages` | Force a disk flush every N messages. Usually left at the default: Kafka relies on replication and the OS page cache. | Принудительно сбрасывать данные на диск каждые N сообщений. Обычно не меняют: Kafka полагается на репликацию и кэш ОС. |
| `flush_ms` | Force a disk flush every N milliseconds. Usually left at the default: Kafka relies on replication and the OS page cache. | Принудительно сбрасывать данные на диск каждые N миллисекунд. Обычно не меняют: Kafka полагается на репликацию и кэш ОС. |
| `index_interval_bytes` | How often an offset index entry is written, in bytes of log. Smaller means faster seeks but a larger index. Rarely changed. | Как часто добавлять запись в индекс оффсетов, в байтах лога. Меньше — быстрее поиск, но больше индекс. Меняют редко. |
| `segment_index_bytes` | Maximum size of a segment's offset index file. Rarely changed. | Максимальный размер файла индекса оффсетов сегмента. Меняют редко. |
| `preallocate` | Preallocate the segment file on disk when it is created. May help on some file systems; rarely changed. | Заранее выделять место под файл сегмента при его создании. Может помочь на некоторых файловых системах; меняют редко. |
| `leader_replication_throttled_replicas` | Replicas whose replication is throttled on the leader side, as <code>partition:broker</code> or <code>*</code>. Set by reassignment tools; usually not edited by hand. | Реплики, для которых ограничена скорость репликации на стороне лидера: <code>partition:broker</code> или <code>*</code>. Задают инструменты переназначения; вручную обычно не меняют. |
| `follower_replication_throttled_replicas` | Replicas whose replication is throttled on the follower side, as <code>partition:broker</code> or <code>*</code>. Set by reassignment tools; usually not edited by hand. | Реплики, для которых ограничена скорость репликации на стороне фолловера: <code>partition:broker</code> или <code>*</code>. Задают инструменты переназначения; вручную обычно не меняют. |

- [ ] **Step 3: Тест.** В `ConfigTable.test.tsx` добавить проверку: все 36 ключей таблицы есть у `TOPIC_CONFIG_HINTS` (`expect(Object.keys(TOPIC_CONFIG_HINTS)).toHaveLength(36)`), и значок у `min.insync.replicas` открывает ru-текст «Сколько реплик должны подтвердить запись при acks=all, иначе producer получит ошибку. Обычно 2 при факторе репликации 3.»

- [ ] **Step 4: Проверки**

Run: `pnpm i18n:check && pnpm vitest run src/components/common && pnpm test && pnpm build`
Expected: PASS; отчёт `--lengths` по `hints.topicConfig` — в отчёт задачи.

- [ ] **Step 5: Commit**

```bash
git add src/components/common src/locales
git commit -m "feat(hints): подсказки к остальным параметрам топика

Co-Authored-By: <модель исполнителя>"
```

---

## После плана

- Финальное ревью ветки (`superpowers:requesting-code-review`): поведение `Hint` по спеке §2, 43 подсказки на месте, тексты en/ru из плана, переводы по глоссарию, проверки `i18n-check`.
- Ручной смоук пользователем по `docs/smoke/c.md` (п. 11 — подсказки параметров топика: вкладка конфигурации топика, просмотр и редактирование; на обзоре кластера значков нет).
