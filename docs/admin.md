# Glafira for Apache Kafka: руководство администратора

## Установка
| ОС | Файл | Тихая установка |
|---|---|---|
| macOS | `Glafira_for_Apache_Kafka_<версия>_aarch64.pkg` / `_x64.pkg` | `sudo installer -pkg <файл>.pkg -target /` (Jamf, Intune, Kandji) |
| Windows | `Glafira_for_Apache_Kafka_<версия>_x64_en-US.msi` | `msiexec /i <файл>.msi /qn` (SCCM, Intune) — установка на машину |
| Linux | `Glafira_for_Apache_Kafka_<версия>_amd64.deb`, `Glafira_for_Apache_Kafka-<версия>-1.x86_64.rpm`, `Glafira_for_Apache_Kafka_<версия>_amd64.AppImage` | `sudo apt install ./<файл>.deb`, `sudo dnf install ./<файл>.rpm` |

Установщики — в разделе релизов `https://github.com/vvnovg/glafira-kafka-releases/releases`.
Пробелы в именах файлов заменены на `_` при публикации; точные имена смотрите в релизе.
Исполняемый файл приложения — `glafira-kafka` (`glafira-kafka.exe` на Windows, `/usr/bin/glafira-kafka` на Linux).

### Обновление установленного приложения
Встроенное обновление («О программе» → «Проверить обновления») скачивает установщик того же типа, что установлен:

| Установлено из | Что делает обновление | Права |
|---|---|---|
| `.pkg` / `.dmg` (macOS) | заменяет `.app` на месте | если `.app` принадлежит root (установка из `.pkg` в `/Applications`) — запрос пароля администратора |
| `.msi` (Windows, на машину) | запускает новый MSI | UAC; у пользователя без прав администратора обновление не установится |
| `.deb` / `.rpm` (Linux) | ставит новый пакет через `dpkg -i` / `rpm -U` | пароль администратора (pkexec, иначе zenity/kdialog + sudo) |
| `.AppImage` | заменяет файл AppImage | запись в каталог с AppImage |

**Управляемые парки** (SCCM, Intune, Jamf, Kandji, репозиторий пакетов): обычные пользователи без прав
администратора не смогут установить обновление сами. Рекомендуется политика `updatesEnabled = false` и раздача
новых версий теми же средствами, что и установка (на Linux — через свой репозиторий apt/dnf или `apt install ./<файл>.deb`).

## Политика
Приложение читает политику при запуске. Пользователь не может её изменить.

| ОС | Где |
|---|---|
| macOS | профиль конфигурации MDM, домен `io.github.vvnovg.glafira.kafka` (файл попадает в `/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist`) — пример `admin/policy.mobileconfig` |
| Windows | `HKLM\SOFTWARE\Policies\Glafira\Kafka` (GPO, Intune) — пример `admin/policy.reg` |
| Linux | `/etc/io.github.vvnovg.glafira.kafka/policy.json` — пример `admin/policy.json` |

| Ключ | Тип | По умолчанию | Смысл |
|---|---|---|---|
| `updatesEnabled` | bool (`dword` 0/1 в реестре) | `true` | `false` — приложение не обращается к серверу обновлений |
| `updateUrl` | строка, только `https://` | публичный `latest.json` | зеркало с одобренными версиями (формат — `latest.json` Tauri). Адрес `http://` считается ошибкой политики: обновления выключаются |
| `licenseKey` | строка | — | ключ организации: Pro у всех пользователей |
| `language` | строка: `en`, `ru`, `de`, `fr`, `es`, `it`, `pl`, `pt-BR` | язык ОС или выбор пользователя | язык интерфейса для всех пользователей; пользователь его не меняет. Неизвестный код — ошибка политики (показывается в «О программе»), язык не задаётся, обновления не выключаются |

Если политика есть, но не читается или содержит недопустимое значение, обновления выключаются, а в «О программе»
и «Лицензия» показывается ошибка политики. Исключение — `language`: неверное значение только показывается как ошибка.

### Проверка политики вручную (macOS)
`defaults write` в `/Library/Managed Preferences` на современной macOS не работает (туда пишет только MDM).
Для проверки соберите plist и скопируйте его от имени администратора:

```bash
rm -f /tmp/policy.plist && plutil -create xml1 /tmp/policy.plist
plutil -insert updatesEnabled -bool NO /tmp/policy.plist
sudo mkdir -p "/Library/Managed Preferences"
sudo cp /tmp/policy.plist "/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist"
sudo chmod 644 "/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist"
plutil -p "/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist"
```
Удалить: `sudo rm "/Library/Managed Preferences/io.github.vvnovg.glafira.kafka.plist"`.

## Внутреннее зеркало обновлений
Скопируйте из релиза установщики, `*.sig` и `latest.json`, замените в `latest.json` адреса `url` на адреса зеркала
и укажите адрес `latest.json` в `updateUrl`. Подписи остаются прежними: приложение проверяет их встроенным ключом.
Зеркало должно отдаваться по `https://` (по `http` приложение обновления не загружает).

В `latest.json` по записи на каждый тип установки: `darwin-aarch64`, `darwin-x86_64`, `windows-x86_64`,
`linux-x86_64-deb`, `linux-x86_64-rpm`, `linux-x86_64-appimage` и `linux-x86_64` (тоже AppImage — запасная запись).
Сохраните все записи: установка из `.deb` скачивает только `.deb`, из `.rpm` — только `.rpm`.

## Сеть
Приложение подключается только к:
- брокерам Kafka из профилей пользователя;
- серверу обновлений, если обновления не выключены: `github.com` и хосты, куда GitHub перенаправляет загрузку ассетов
  релизов (`release-assets.githubusercontent.com`, `objects.githubusercontent.com`), — или хост из `updateUrl`.

Телеметрии и отчётов о сбоях нет.

## Права доступа в Kafka (ACL)
Сессии чтения создают consumer group с именем `glafira-kafka-<пользователь>-<суффикс>` (по умолчанию; имя
пользователя — латиница, цифры, `_`, `-`). Служебные запросы метаданных используют группу `glafira-kafka-metadata`.
Чтобы выдать права заранее, разрешите префикс `glafira-kafka-` для ресурса Group (операции Read, Describe)
и нужные Read/Describe на топики. Для режима «только чтение» права на запись не нужны.
Просмотр сообщений использует временные группы `glafira-kafka-browse-<uuid>` без коммитов оффсетов.
`client.id` клиентов — `glafira-kafka` (consumer-сессии — `glafira-kafka-session`): по нему соединения видны в квотах и метриках брокера.

## Данные и логи
| | macOS | Windows | Linux |
|---|---|---|---|
| Данные | `~/Library/Application Support/io.github.vvnovg.glafira.kafka` | `%APPDATA%\io.github.vvnovg.glafira.kafka` | `~/.local/share/io.github.vvnovg.glafira.kafka` |
| Логи | `~/Library/Logs/io.github.vvnovg.glafira.kafka` | `%LOCALAPPDATA%\io.github.vvnovg.glafira.kafka\logs` | `~/.local/share/io.github.vvnovg.glafira.kafka/logs` |
| Пароли | Keychain, сервис `glafira-kafka` | Credential Manager | Secret Service |

Пути Windows и Linux — по правилам Tauri (`app_data_dir`, `app_log_dir`), проверить при смоуке.
«О программе» → «Собрать диагностику» сохраняет архив с логами и профилями без паролей.
