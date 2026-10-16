# Glafira for Apache Kafka: руководство администратора

## Установка
| ОС | Файл | Тихая установка |
|---|---|---|
| macOS | `Glafira for Apache Kafka_<версия>_aarch64.pkg` / `_x64.pkg` | `sudo installer -pkg <файл>.pkg -target /` (Jamf, Intune, Kandji) |
| Windows | `Glafira for Apache Kafka_<версия>_x64_en-US.msi` | `msiexec /i <файл>.msi /qn` (SCCM, Intune) — установка на машину |
| Linux | `.deb`, `.rpm`, `.AppImage` | `sudo apt install ./<файл>.deb`, `sudo dnf install ./<файл>.rpm` |

Установщики — в разделе релизов `https://github.com/vvnovg/glafira-kafka-releases/releases`.
Точные имена файлов смотрите в релизе (GitHub заменяет пробелы в именах на точки).

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
| `updateUrl` | строка | публичный `latest.json` | зеркало с одобренными версиями (формат — `latest.json` Tauri) |
| `licenseKey` | строка | — | ключ организации: Pro у всех пользователей |

Если политика есть, но не читается, обновления выключаются, а в «О программе» и «Лицензия» показывается ошибка политики.

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

## Сеть
Приложение подключается только к:
- брокерам Kafka из профилей пользователя;
- серверу обновлений (`github.com`, `objects.githubusercontent.com` или `updateUrl`), если обновления не выключены.

Телеметрии и отчётов о сбоях нет.

## Права доступа в Kafka (ACL)
Сессии чтения создают consumer group с именем `kafka-explorer-<пользователь>-<суффикс>` (по умолчанию; имя
пользователя — латиница, цифры, `_`, `-`). Служебные запросы метаданных используют группу `kafka-explorer-metadata`.
Чтобы выдать права заранее, разрешите префикс `kafka-explorer-` для ресурса Group (операции Read, Describe)
и нужные Read/Describe на топики. Для режима «только чтение» права на запись не нужны.

## Данные и логи
| | macOS | Windows | Linux |
|---|---|---|---|
| Данные | `~/Library/Application Support/io.github.vvnovg.glafira.kafka` | `%APPDATA%\io.github.vvnovg.glafira.kafka` | `~/.local/share/io.github.vvnovg.glafira.kafka` |
| Логи | `~/Library/Logs/io.github.vvnovg.glafira.kafka` | `%LOCALAPPDATA%\io.github.vvnovg.glafira.kafka\logs` | `~/.local/share/io.github.vvnovg.glafira.kafka/logs` |
| Пароли | Keychain, сервис `glafira-kafka` | Credential Manager | Secret Service |

Пути Windows и Linux — по правилам Tauri (`app_data_dir`, `app_log_dir`), проверить при смоуке.
«О программе» → «Собрать диагностику» сохраняет архив с логами и профилями без паролей.
