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

## Известные ограничения
- `.pkg` пока не нотаризуется отдельно (доработка на потом); без сертификатов Apple сборки неподписанные.

## Смоук установщиков
- [ ] macOS (`.dmg` и `.pkg`): установка, запуск, подключение PLAINTEXT и mTLS, «О программе» — версия.
- [ ] Windows (`.msi`, `msiexec /qn` из администраторской консоли): то же.
- [ ] Linux (`.deb` или `.AppImage`): то же.
- [ ] Обновление: установить предыдущую версию → «Проверить обновления» → «Установить и перезапустить» → новая версия, данные на месте.
- [ ] Политика `updatesEnabled = false` (по `docs/admin.md`): «Обновления управляются вашей организацией», в логе нет запросов к серверу обновлений.
- [ ] «Собрать диагностику» → архив открывается, паролей нет.
- [ ] Пути данных и логов из `docs/admin.md` совпадают с «О программе» на каждой ОС.
