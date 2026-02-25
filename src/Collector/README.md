# Rocketman Collector (Playwright)

## Prereqs
- .NET 8 SDK
- Playwright browsers

## Install Playwright browsers
После первой сборки:
```bash
dotnet run --project src/Collector/Collector.csproj -- --install-playwright
```

## Run
```bash
# (optional) apply DB migrations before first run
dotnet run --project src/Collector/Collector.csproj -- --db-migrate

# run local backend API for settings (first integrated backend endpoints)
dotnet run --project src/Collector/Collector.csproj -- \
  --serve \
  --lan \
  --port 5057

# via args
DOTNET_ENVIRONMENT=Production \
  dotnet run --project src/Collector/Collector.csproj -- \
  --phone "+7..." \
  --password "..." \
  --login-url "https://rocketman.ru/manager/auth/login?token=..." \
  --output "out/clients.json" \
  --parallel 3 \
  --timeout 0

# or via env vars
export ROCKETMAN_PHONE="+7..."
export ROCKETMAN_PASSWORD="..."
export ROCKETMAN_LOGIN_URL="https://rocketman.ru/manager/auth/login?token=..."

dotnet run --project src/Collector/Collector.csproj -- --output "out/clients.json" --parallel 3 --timeout 0
```

## Notes
- Tokenized login URL must be provided at runtime, not stored in config.
- Selectors are embedded into the executable (`Collector.Config.rocketman.selectors.json`); external file is not required on user PC.
- During table sync collector selects the maximum available page size automatically (not hardcoded to `1000`).
- If `ClientsList.Pagination.NextPage` is configured, collector walks all pages; without it only the current page is collected.
- `--timeout 0` means wait until data appears (no timeout).
- Local DB migration command:
  - `--db-migrate` apply pending migrations and exit.
  - `--db-path` optional DB path override.
  - default DB path without `--db-path`:
    - Windows: `%LOCALAPPDATA%\SmsControl\smscontrol.db`
    - macOS/Linux: OS local app-data directory (`SmsControl/smscontrol.db`)
  - compatibility: if old `out/smscontrol.db` exists and new path is still empty, app uses old DB file automatically.
- Local API mode:
  - `--serve` start HTTP API.
  - `--host` optional bind host (default: `127.0.0.1`).
  - `--lan` bind on `0.0.0.0` (for devices in local network).
  - `--port` optional port (default: `5057`).
  - local web UI is embedded into the same executable and available at:
    - `http://127.0.0.1:5057/`
  - CORS for local UI is enabled (methods/headers/origin allowed for localhost development).
  - Implemented endpoints:
    - `GET /health`
    - `GET /api/errors/catalog`
    - `GET /api/audit`
    - `GET /api/settings`
    - `PUT /api/settings`
    - `GET /api/settings/comment-rules`
    - `PUT /api/settings/comment-rules`
    - `GET /api/reports/weekly`
    - `POST /api/rocketman/comment/test`
    - `GET /api/channels`
    - `POST /api/channels`
    - `PUT /api/channels/{id}`
    - `DELETE /api/channels/{id}`
    - `PATCH /api/channels/{id}/status` (`offline`/`unknown`)
    - `POST /api/channels/check`
    - `POST /api/channels/{id}/check`
    - `GET /api/templates/meta`
    - `GET /api/templates`
    - `GET /api/templates/active`
    - `GET /api/templates/{id}`
    - `POST /api/templates`
    - `PUT /api/templates/{id}`
    - `PATCH /api/templates/{id}/status`
    - `GET /api/manual-presets`
    - `GET /api/manual-presets/{id}`
    - `POST /api/manual-presets`
    - `PUT /api/manual-presets/{id}`
    - `DELETE /api/manual-presets/{id}`
    - `GET /api/dialogs`
    - `GET /api/dialogs/by-client/{externalClientId}`
    - `GET /api/dialogs/by-phone/{phone}/messages`
    - `GET /api/dialogs/by-phone/{phone}/draft`
    - `PUT /api/dialogs/by-phone/{phone}/draft`
    - `DELETE /api/dialogs/by-phone/{phone}/draft`
    - `POST /api/dialogs/by-phone/{phone}/send`
    - `DELETE /api/dialogs/by-phone/{phone}`
    - `POST /api/dialogs/prune`
    - `GET /api/stop-list`
    - `GET /api/stop-list/by-phone/{phone}`
    - `GET /api/stop-list/{id}`
    - `POST /api/stop-list`
    - `PUT /api/stop-list/{id}`
    - `DELETE /api/stop-list/{id}`
    - `DELETE /api/stop-list/by-phone/{phone}`
    - `POST /api/stop-list/bulk/add`
    - `POST /api/stop-list/bulk/remove`
    - `POST /api/stop-list/bulk/deactivate`
    - `GET /api/alerts`
    - `PATCH /api/alerts/{id}/status`
    - `GET /api/events`
    - `GET /api/events/run` (SSE)
    - `POST /api/clients/sync`
    - `GET /api/clients/sync-status`
    - `GET /api/clients`
    - `GET /api/clients/{externalClientId}/debt`
    - `POST /api/clients/{externalClientId}/debt/fetch`
    - `POST /api/queue/preview`
    - `POST /api/queue/forecast`
    - `POST /api/queue/build`
    - `GET /api/queue`
    - `PUT /api/queue/{jobId}/message-override`
    - `DELETE /api/queue/{jobId}/message-override`
    - `POST /api/queue/jobs/remove`
    - `POST /api/queue/retry-errors`
    - `POST /api/queue/bulk/set-template`
    - `GET /api/run/status`
    - `POST /api/run/start`
    - `POST /api/run/stop`
    - `POST /api/app/shutdown`
  - Background dispatcher:
    - processes `running` run-session in background;
    - transitions jobs through `queued/running/retry/stopped/sent/failed`;
    - applies retry/backoff and marks run as `completed` when pending jobs are exhausted.
    - processes up to `N` due jobs per tick, where `N` is number of available channels (`online/unknown`), so sends are not serialized into one global lane.
    - when a channel accumulates errors, dispatcher marks device as `error`, raises channel alert, and retries jobs on other available channels (failover).
    - on final `failed` delivery of a client job, dispatcher creates an active alert card with failure code/details (`sms_send_error`).
    - after successful send or health-check of channel, active channel alerts are auto-resolved.
  - Dispatcher delivery mode:
    - dispatcher sends real SMS via Traccar endpoint/token for `sourceMode=live`;
    - `queue/build` pre-fills `run_jobs.template_id` по правилу просрочки активного шаблона (`range` или `exact`);
    - if `templateId` is empty, template is auto-picked by its overdue rule from active templates (`autoAssign=true`);
    - if active templates are missing/incompatible, задача получает ошибку `TEMPLATE_NOT_RESOLVED`/`TEMPLATE_RENDER_EMPTY` и не отправляется;
    - tokens `{полное_фио}` and `{сумма_долга}` are rendered before send (`сумма_долга` = `итого + 2000`, rounding to nearest 1000, midpoint up);
    - комментарий в договор после успешной отправки берется из `commentText` выбранного шаблона;
    - delivery pipeline включает рендер шаблона, назначение канала, retry/failover и алерты.
  - Debt cache:
    - `POST /api/clients/{externalClientId}/debt/fetch` reads exact debt from Rocketman card and stores it in `client_debt_cache`;
    - `GET /api/clients/{externalClientId}/debt` returns exact/approx debt, status, source, timestamps and last error;
    - after successful fetch backend updates latest `client_snapshot_rows.total_with_commission_raw` and pending `run_jobs.payload_json.totalWithCommissionRaw`.
  - Dialog backend API:
    - supports list/read/delete/prune dialogs from `messages` storage;
    - `POST /api/dialogs/by-phone/{phone}/send` performs real manual SMS send via Traccar and writes message history;
    - автоматическая синхронизация входящих SMS с устройств отключена.
  - Settings payload now includes work window fields:
    - `allowLiveDispatch` (bool; blocks/allows real `live` run start from UI/API)
    - `workWindowStart` (HH:mm)
    - `workWindowEnd` (HH:mm)
  - `run/start` blocks launch when:
    - no planned queue exists;
    - queue is empty;
    - queue snapshot is stale relative to latest client sync;
    - another run session is already `running`;
    - live queue is selected and `allowLiveDispatch=false` in settings.
  - UI режим запуска зафиксирован как `LIVE`:
    - актуализация из интерфейса выполняется через `POST /api/clients/sync` (Rocketman);
  - `queue/forecast` now returns:
    - `onlineChannelsCount` (сколько каналов в статусе `online`);
    - `channelsUsed` (сколько каналов использовано в планировании, минимум 1; учитываются каналы в статусах `online/unknown`);
    - интервалы учитываются per-channel (не глобально на весь запуск).
  - Manual channel control:
    - set `offline` to stop/exclude device from dispatch and forecast;
    - set `unknown` to return device to rotation (recommended to run channel check after enabling).
  - If channel availability changes during a running session (new channel added, channel enabled/restored, channel check changed state), dispatcher rebalances remaining `queued/retry` jobs automatically.
  - Rebalance priority: channels with earlier `nextAvailable` are selected first (channels without cooldown are prioritized automatically).

## Test SMS from project (Android + ADB)
```bash
# install adb on macOS (once)
brew install android-platform-tools

# check device is visible
adb devices

# test send (auto send keyevent)
dotnet run --project src/Collector/Collector.csproj -- \
  --send-test-sms \
  --to "79XXXXXXXXX" \
  --message "Тест из проекта"

# safer mode: only open compose screen, send manually on phone
dotnet run --project src/Collector/Collector.csproj -- \
  --send-test-sms \
  --to "79XXXXXXXXX" \
  --message "Тест из проекта" \
  --open-only
```

Optional flags:
- `--adb-device <serial>` pick specific phone from `adb devices`
- `--adb-timeout <ms>` command timeout, default `20000`
- `--send-delay-ms <ms>` delay before auto keyevent send, default `1200`
- `--sms-log <path>` log path, default `out/sms_history.jsonl`

## Test SMS via Traccar SMS Gateway (no USB/ADB)
```bash
dotnet run --project src/Collector/Collector.csproj -- \
  --send-test-sms \
  --to "79XXXXXXXXX" \
  --message "Тест из проекта через Traccar" \
  --gateway-url "http://192.168.0.18:8082/" \
  --gateway-token "<TRACCAR_API_KEY>" \
  --gateway-timeout 15000
```

Notes:
- Use API key token from Traccar SMS Gateway app (`--gateway-token`), not FCM token.
- For gateway mode pass both `--gateway-url` and `--gateway-token`.

## Publish one-file EXE for Windows
```bash
dotnet publish src/Collector/Collector.csproj \
  -c Release \
  -r win-x64 \
  --self-contained true \
  -p:PublishSingleFile=true \
  -p:IncludeNativeLibrariesForSelfExtract=true \
  -p:DebugType=None \
  -o out/publish/win-x64
```

Run on employee PC:
```powershell
.\Collector.exe --desktop --port 5057
```
`--desktop` starts backend in background (if not running) and opens `http://127.0.0.1:5057/` in browser.

## Build Setup.exe (Inno Setup, desktop shortcut)
On Windows build machine with Inno Setup 6 installed:
```powershell
.\deploy\windows\build-installer.ps1
```
`build-installer.ps1` also installs Playwright Chromium into `out\publish\win-x64\ms-playwright` before packaging.

Installer output:
- `out\installer\SmsControlSetup.exe`
- includes bundled Playwright Chromium (`ms-playwright`) for offline use on employee PC.

Installer creates Start Menu and desktop shortcuts to:
- `Collector.exe --desktop --port 5057`

## Build Setup.exe from macOS (GitHub Actions)
- Workflow file: `.github/workflows/windows-installer.yml`
- Run manually in GitHub Actions and download artifact `SmsControlSetup`.
