# Лицензии: генерация ключей и выдача

Лицензионный ключ — подписанный Ed25519 текст: `base64url(JSON).base64url(подпись)`.
Приложение проверяет его офлайн открытым ключом из `src-tauri/license-public.key`.

## Один раз: пара ключей
```bash
cargo run --manifest-path tools/issue-license/Cargo.toml -- keygen --out ~/.glafira-kafka-license
cp ~/.glafira-kafka-license/license-public.key src-tauri/license-public.key
```
`license-signing.key` — секрет. Держите вне репозитория (менеджер паролей, зашифрованный диск) и сделайте резервную копию:
без него нельзя выдать новые ключи, а ключи, подписанные потерянным ключом, нельзя отозвать.
В репозитории лежит только тестовая пара `crates/license-format/testdata/dev-*`, и сейчас `src-tauri/license-public.key` — это её
открытый ключ: сборка с ним принимает ключи, подписанные общедоступным `dev-signing.key`. Проверка в релизном конвейере
появится в M3b. Пока действует защита в `src-tauri/build.rs`: сборка с профилем `release` (`pnpm tauri build`,
`cargo build --release`) падает, если `license-public.key` совпадает с тестовым. Чтобы локально проверить релизную сборку
с тестовым ключом, задайте `KE_ALLOW_DEV_LICENSE_KEY=1` (сборка выведет предупреждение); такую сборку нельзя раздавать.

## Выдать ключ
```bash
# организации (раздаётся через политику licenseKey)
cargo run --manifest-path tools/issue-license/Cargo.toml -- issue --key ~/.glafira-kafka-license/license-signing.key \
  --kind organization --licensee "ACME Corp" --expires 2027-03-31 --seats 500
# человеку
cargo run --manifest-path tools/issue-license/Cargo.toml -- issue --key ~/.glafira-kafka-license/license-signing.key \
  --kind personal --licensee user@example.com --expires 2027-03-31
```
`--expires` — последний день действия (включительно), дата в прошлом отклоняется. Без него ключ бессрочный.
`--seats` необязателен и только информационный.

## Проверить ключ
```bash
cargo run --manifest-path tools/issue-license/Cargo.toml -- verify --public src-tauri/license-public.key '<ключ>'
```

## Что происходит при окончании срока
За 14 дней — предупреждение. После — функции Pro отключаются, данные и существующие prod-профили (с подтверждениями) сохраняются.

## Раздача через политику
Ключ организации задаётся параметром `licenseKey` политики администратора (вместе с `updatesEnabled`). Расположение:
- macOS: `/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist`
- Windows: реестр `HKLM\Software\Policies\Glafira\Kafka`
- Linux: `/etc/io.github.vvnovg.glafira.kafka/policy.json`

Лицензия из политики имеет приоритет над введённой вручную и не меняется из интерфейса.
