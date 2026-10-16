# Чек-лист релиза

## Один раз (владелец)
1. Боевые ключи лицензий: `docs/licensing.md` → «Один раз». Заменить `src-tauri/license-public.key`, закоммитить.
2. Ключ updater: `pnpm tauri signer generate -w ~/.tauri/glafira-kafka-updater.key`. Открытый — в `tauri.conf.json` → `plugins.updater.pubkey`.
3. Публичный репозиторий `vvnovg/glafira-kafka-releases` с README из `docs/releases-repo/README.md`.
4. Секреты приватного репозитория (Settings → Secrets and variables → Actions):
   `TAURI_SIGNING_PRIVATE_KEY` (содержимое файла ключа), `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`,
   `RELEASES_TOKEN` (fine-grained PAT: только `vvnovg/glafira-kafka-releases`, Contents: Read and write).
   Позже, при наличии: `APPLE_CERTIFICATE`, `APPLE_CERTIFICATE_PASSWORD`, `APPLE_SIGNING_IDENTITY`,
   `APPLE_ID`, `APPLE_PASSWORD`, `APPLE_TEAM_ID`, `APPLE_INSTALLER_IDENTITY`.
5. Резервные копии закрытых ключей (лицензии и updater) вне компьютера разработки.

## Пробный прогон
Нужен `release.yml` в ветке `main`. `gh workflow run release -f dry_run=true` собирает все платформы и `latest.json`
без публикации. Релизная сборка отказывается использовать тестовый ключ лицензий (`build.rs`); в пробном прогоне это
разрешает `KE_ALLOW_DEV_LICENSE_KEY=1`. `node scripts/check-release.mjs --dry-run vX.Y.Z` превращает проблемы с ключами
в предупреждения; сборка по тегу всегда строгая.

## Каждый релиз
1. Версия в `package.json`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`; раздел в `CHANGELOG.md`.
2. `node scripts/check-release.mjs vX.Y.Z` → «можно выпускать».
3. `git tag vX.Y.Z && git push origin vX.Y.Z` → workflow `release`. Релиз создаётся черновиком со всеми ассетами
   и публикуется атомарно (`--draft=false --latest`); updater читает `releases/latest/download/latest.json`
   (артефакты updater — формат v2).
4. Смоук на трёх ОС (ниже) по установщикам из релиза.

## Никогда не менять
- `bundle.windows.wix.upgradeCode` в `src-tauri/tauri.conf.json` (`88add133-7053-419a-bc78-7d7ccb39333c`):
  по нему MSI новой версии обновляет установленную, а не ставится рядом. Без явного значения WiX выводит код из
  имени продукта, поэтому он задан явно и не меняется даже при переименовании продукта.
- `mainBinaryName` (`glafira-kafka`) и `identifier` (`io.github.vvnovg.glafira.kafka`): от них зависят путь
  установки, имя процесса, данные, keychain и политика.
- Ключ updater (`plugins.updater.pubkey`): со сменой ключа все установленные версии перестанут принимать обновления.

## Известные ограничения
- `.pkg` пока не нотаризуется отдельно (доработка на потом); без сертификатов Apple сборки неподписанные.

## Смоук установщиков
- [ ] macOS (`.dmg` и `.pkg`): установка, запуск, подключение PLAINTEXT и mTLS, «О программе» — версия.
- [ ] Windows (`.msi`, `msiexec /qn` из администраторской консоли): то же; в «Программах и компонентах» одна запись.
- [ ] Linux (`.deb`, `.rpm`, `.AppImage`): то же; бинарь `/usr/bin/glafira-kafka`.
- [ ] Обновление: установить предыдущую версию → «Проверить обновления» → «Установить и перезапустить» → новая версия, данные на месте
      (для `.deb` и `.rpm` — отдельно: ставится пакет того же типа, запрашивается пароль администратора).
- [ ] Политика `updatesEnabled = false` (по `docs/admin.md`): «Обновления управляются вашей организацией», в логе нет запросов к серверу обновлений.
- [ ] Политика с `updateUrl: "http://…"`: в «О программе» ошибка политики, обновления выключены.
- [ ] «Собрать диагностику» → архив открывается, паролей нет.
- [ ] Пути данных и логов из `docs/admin.md` совпадают с «О программе» на каждой ОС.
- [ ] Миграция со старого идентификатора (macOS, реальный Keychain): данные и пароли из `kafka-explorer`
      появились в новой версии; при отказе в доступе к записи Keychain приложение запускается.

## Сквозная проверка обновления (обязательна для первого релиза с updater, затем — при изменениях updater)
Цель — до тега убедиться, что выпущенная версия сможет обновиться. Нужны боевой ключ updater (pubkey уже в
`tauri.conf.json`) и секреты `TAURI_SIGNING_PRIVATE_KEY*` — шаги выполняет владелец.

1. **Старая версия (A).** Ветка с версией выпуска, например `0.2.0`. Пробный прогон: `gh workflow run release -f dry_run=true`;
   скачать артефакт `release-v0.2.0` (установщики A).
2. **Новая версия (B).** Временная ветка от той же ревизии, версия `0.2.1` в `package.json`, `src-tauri/Cargo.toml`,
   `src-tauri/tauri.conf.json` и раздел `## 0.2.1` в `CHANGELOG.md` (ветку не сливать). Пробный прогон из неё
   (`gh workflow run release --ref <ветка> -f dry_run=true`, workflow должен быть в `main`) → артефакт `release-v0.2.1`.
3. **Тестовое зеркало (https).** Один из вариантов:
   - тестовый релиз в отдельном публичном репозитории (например, `vvnovg/glafira-kafka-updater-test`):
     `gh release create v0.2.1 <ассеты B> --repo …`, затем пересобрать `latest.json` с адресами этого репозитория:
     `node scripts/make-latest-json.mjs --version 0.2.1 --notes-file notes.md --base-url https://github.com/<repo>/releases/download/v0.2.1 --dir <ассеты B> --require-all`;
   - статический https-сервер в локальной сети (с доверенным сертификатом) с теми же файлами и `latest.json`,
     пересобранным с его `--base-url`.
   Проверить: `curl -fsSL <url>/latest.json` открывается, `curl -fsSI` по каждому `url` из него — 200.
4. **Политика с зеркалом** на каждой тестовой машине (`docs/admin.md` → «Политика»): `updateUrl` = адрес
   `latest.json` зеркала (только `https://`).
5. **На каждой ОС и каждом типе установки** — macOS `.dmg` и `.pkg` (aarch64; x64 — если есть машина),
   Windows `.msi`, Linux `.deb`, `.rpm`, `.AppImage`:
   - [ ] установить A, создать подключение с паролем, открыть чтение топика и **активную consumer-сессию**;
   - [ ] «О программе» → «Проверить обновления» → «Доступна версия 0.2.1»;
   - [ ] «Установить и перезапустить» → подтверждение про активные сессии → «Остановить сессии и обновить»;
   - [ ] после перезапуска «О программе» показывает 0.2.1; подключения, пароли, история и настройки на месте;
   - [ ] брокер: consumer group сессии вышла из группы (нет «зависшего» участника до session.timeout) —
         `kafka-consumer-groups --describe --group <группа>`; на **Windows** это проверка хука выхода
         (`on_before_exit` завершает сессии перед запуском MSI);
   - [ ] Windows: в «Программах и компонентах» одна запись версии 0.2.1 (upgradeCode сработал), установка
         требует UAC; под пользователем без прав администратора — понятная ошибка, A продолжает работать;
   - [ ] macOS `.pkg` (приложение в `/Applications` принадлежит root): запрос пароля администратора, после
         установки владелец и права `.app` не изменились неожиданно (`ls -ld /Applications/Glafira*.app`);
         отказ от ввода пароля — ошибка, A продолжает работать. macOS `.dmg` (приложение перетащено
         пользователем): обновление без пароля;
   - [ ] Linux `.deb`/`.rpm`: запрос пароля (pkexec), `dpkg -l | grep glafira` / `rpm -q` показывает 0.2.1;
         `.AppImage`: файл заменён на месте, права на исполнение сохранены.
6. **Отрицательные проверки:** `latest.json` с испорченной подписью → ошибка установки, A работает;
   политика `updatesEnabled = false` → проверки нет.
7. Удалить временную ветку и тестовый релиз/зеркало. Только после этого ставить тег.
