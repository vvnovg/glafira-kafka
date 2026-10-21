# Смоук M3a

Подготовка: `docker compose -f dev/docker-compose.yml up -d --force-recreate`, затем `pnpm tauri dev`.
Файлы сертификатов — `dev/certs/out/` (выбирайте через кнопку «Выбрать…» или вставляйте абсолютный путь).
Пароли: хранилища — `storepass`, отдельного ключа (`client-enc.key`, `client-keypass.jks`) — `keypass`.
Новое подключение: кнопка «+» (подсказка «Новое подключение») рядом с заголовком «Подключения».
Результат «Проверить подключение» — три шага: «Конфигурация», «DNS / TCP», «Kafka (TLS, SASL, метаданные)»; ✅ — успех, ❌ — ошибка.

## mTLS
1. Новое подключение: «Имя» `mtls`, «Bootstrap-серверы» `localhost:9095`, «Протокол» `SSL`.
   «Доверенный CA» → `dev/certs/out/ca.pem`. Включить «Клиентский сертификат (mTLS)».
   «Сертификат клиента» → `dev/certs/out/client.pem` (формат определится как PEM сам),
   «Ключ клиента» → `dev/certs/out/client.key`, все пароли пустые.
   «Проверить подключение» → все три шага ✅. «Сохранить», в дереве открыть «Топики» — список загружается.
2. «Ключ клиента» → `client-enc.key` (файл зашифрован), «Пароль ключа (если зашифрован)» пустой →
   «Проверить»: шаг «Конфигурация» ❌, текст «…ключ зашифрован — укажите пароль ключа».
   Ввести пароль `keypass` → все шаги ✅. Неверный пароль (например `nope`) → «…неверный пароль ключа».
3. PKCS#12: «Сертификат клиента» → `client.p12` (формат сам станет «PKCS#12 (.p12, .pfx)», поле ключа исчезнет),
   «Пароль хранилища клиента» `storepass` → ✅. Пароль `nope` → ❌ «…неверный пароль хранилища PKCS#12».
4. JKS: «Сертификат клиента» → `client-keypass.jks` (формат JKS), «Пароль хранилища клиента» `storepass`,
   «Пароль ключа (если отличается)» пустой → ❌ «Неверный пароль ключа в хранилище JKS — укажите пароль ключа…».
   «Пароль ключа» `keypass` → ✅.
   Затем «Доверенный CA» → `truststore.jks` (формат JKS), «Пароль хранилища CA» `storepass` → ✅.
5. Выключить «Клиентский сертификат (mTLS)» (протокол SSL, CA `ca.pem`) → шаги «Конфигурация» и «DNS / TCP» ✅,
   «Kafka (TLS, SASL, метаданные)» ❌ с сообщением об ошибке TLS / подключения (брокер требует клиентский сертификат).
6. Сохранённый JKS-профиль: настроить как в п. 4 (с `keypass`) и «Сохранить»; открыть подключение через ✎ (поля паролей показывают
   «оставьте пустым, чтобы не менять»). Отметить «удалить сохранённый» у «Пароль ключа (если отличается)» → «Сохранить» →
   снова ✎ → «Проверить подключение» → ❌ «Неверный пароль ключа в хранилище JKS…».

## Лицензия
7–10. Лицензионных ключей больше нет (с 0.3.0 все функции бесплатны); шаг устарел.

## Политика (macOS)
11. Политика организации. `defaults write` в «Managed Preferences» на современных macOS файл не создаёт
    (туда пишет только MDM), поэтому plist собирается и копируется вручную — всё в одной вкладке терминала:
    ```bash
    rm -f /tmp/ke-policy.plist && plutil -create xml1 /tmp/ke-policy.plist
    plutil -insert updatesEnabled -bool NO /tmp/ke-policy.plist
    sudo mkdir -p "/Library/Managed Preferences"
    sudo cp /tmp/ke-policy.plist "/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist"
    sudo chmod 644 "/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist"
    plutil -p "/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist"   # updatesEnabled => false
    ```
    Перезапуск `pnpm tauri dev` → в логе `политика managed=true updates=false`; блок «Политика организации» с путём.
12. `sudo rm "/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist"`, перезапуск → блок политики исчез.
