# Техническая документация (актуальная)

## 1. Назначение
Приложение автоматизирует цикл:
- актуализация базы клиентов из Rocketman;
- формирование и редактирование очереди SMS;
- отправка SMS через Android-каналы (Traccar);
- запись комментария в карточку клиента после успешной отправки;
- учет ошибок и уведомлений.

## 2. Архитектура
- Backend: `src/Collector` (.NET 8, Minimal API, EF Core SQLite).
- UI: встроенные статики из `src/WebUiMock` (`index.html`, `app.js`, `styles.css`, `uikit.js`).
- Конфиг селекторов Rocketman вшит в `.exe` как embedded resource (`Collector.Config.rocketman.selectors.json`).
- Загрузка селекторов в runtime идет из embedded resource (без поиска внешних fallback-файлов).
- Хранилище: SQLite (`smscontrol.db`), путь по умолчанию:
  - Windows: `%LOCALAPPDATA%/SmsControl/smscontrol.db`
  - macOS/Linux: локальная app-data директория `SmsControl/smscontrol.db`

## 3. Основные модули backend
- `ClientSyncService`: сбор таблицы клиентов из Rocketman (без захода в карточки на этапе актуализации).
- `QueueService`: preview/forecast/build очереди, ручные правки очереди, массовые операции.
- `RunService`: состояния сессии запуска (planned/running/stopped/completed), старт/стоп.
- `RunDispatchService`: фоновые тики отправки, перезапуски/retry, failover каналов, запись комментариев.
- `DialogService`: диалоги, история исходящих, ручная отправка, черновики, очистка старых диалогов.
- `ChannelService`: CRUD каналов, health-check, статусы `online/unknown/offline/error`.
- `AlertService`: карточки уведомлений и смена статуса (`active/resolved/irrelevant`).
- `DebtCacheService`: получение и кэш суммы долга из карточки клиента.
- `RocketmanCommentService`: запись комментария в карточку клиента через Playwright.

## 4. Ключевые правила логики
- Отправка выполняется только в рабочем окне клиента (локальное время клиента, смещение относительно Москвы).
- Интервал отправки считается отдельно для каждого канала.
- При ошибке канала задачи автоматически перераспределяются на доступные каналы.
- После финального `failed` по задаче создается alert `sms_send_error`.
- После успешной отправки в `LIVE` запускается запись комментария в карточку клиента.
- При сбое записи комментария создается отдельный alert `contract_comment_error`.

## 5. Комментарии в карточке клиента
- Пишутся только для `live`-сессий.
- Текст определяется по `templateKind` + правилам из настроек:
  - `sms1/sms1_regular/sms2` -> `sms2`
  - `sms3` -> `sms3`
  - `ka1` -> `ka1`
  - `ka2` -> `kaN` (с подстановкой номера повтора `{n}`)
  - `ka_final` -> `kaFinal`
- Выполняется до 3 попыток с коротким backoff.
- Если `templateKind` не распознан, комментарий не пишется автоматически и создается ошибка `COMMENT_TEXT_RESOLVE_FAILED` (без «тихой» подстановки другого текста).

## 6. Что отключено в текущей версии
- Автоматическая pull/push синхронизация входящих SMS с устройств.
- Endpoint-ы входящих сообщений не публикуются.

## 7. Актуальные API-группы
- `/api/settings`, `/api/settings/comment-rules`
- `/api/channels`, `/api/channels/check`, `/api/channels/{id}/check`
- `/api/templates`, `/api/manual-presets`
- `/api/clients/sync`, `/api/clients/sync-status`, `/api/clients`
- `/api/queue/preview`, `/api/queue/forecast`, `/api/queue/build`, `/api/queue`
- `/api/run/status`, `/api/run/start`, `/api/run/stop`
- `/api/app/shutdown`
- `/api/dialogs`, `/api/dialogs/by-phone/*`, `/api/dialogs/prune`
- `/api/alerts`, `/api/alerts/{id}/status`
- `/api/reports/weekly`, `/api/events`, `/api/events/run`

## 8. Сборка и проверка
- Базовая проверка:
  - `dotnet build src/Collector/Collector.csproj`
  - `node --check src/WebUiMock/app.js`
- Установка браузера Playwright для Rocketman:
  - `dotnet run --project src/Collector/Collector.csproj -- --install-playwright`
- Старт локально:
  - `dotnet run --project src/Collector/Collector.csproj -- --serve`
