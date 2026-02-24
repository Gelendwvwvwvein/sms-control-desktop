# Windows: сборка и развёртывание `.exe`

## 1. Сборка релиза
Запустить в корне проекта:

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

Результат: `out/publish/win-x64/Collector.exe`.

## 1.1 Сборка установщика `Setup.exe` (с ярлыком)
1. Установить Inno Setup 6 на Windows-машине сборки.
2. Запустить PowerShell-скрипт:

```powershell
.\deploy\windows\build-installer.ps1
```

Результат: `out\installer\SmsControlSetup.exe`.
Во время сборки `build-installer.ps1` скачивает Playwright Chromium в `out\publish\win-x64\ms-playwright`, после чего браузер включается в `Setup.exe`.

Инсталлятор:
- устанавливает приложение в профиль пользователя;
- создает ярлык на рабочем столе;
- создает ярлык в меню Пуск;
- запускает приложение через `Collector.exe --desktop` (backend стартует в фоне, затем открывается UI).

## 1.2 Сборка с Mac через GitHub Actions
Если локально только macOS, используйте workflow:

`/.github/workflows/windows-installer.yml`

Шаги:
1. Отправить изменения в GitHub.
2. Открыть `Actions` -> `Build Windows Installer`.
3. Нажать `Run workflow`.
4. Скачать artifact `SmsControlSetup` (внутри `SmsControlSetup.exe`).

## 2. Что передать сотруднику
- Вариант A: `SmsControlSetup.exe` (рекомендуется).
- Вариант B: `Collector.exe` + папка `ms-playwright` (ручная установка).

## 3. Первый запуск у сотрудника
1. Запустить ярлык `SMS Control` на рабочем столе или в меню Пуск.
2. `Collector.exe --desktop` поднимает backend и открывает интерфейс `http://127.0.0.1:5057/`.

## 4. Браузеры Playwright (обязательно)
Для работы Rocketman-части нужны браузеры Playwright.

Для `SmsControlSetup.exe` Chromium уже упакован в установщик, отдельная установка не нужна.

Для ручной установки без папки `ms-playwright` установите Chromium командой:

```powershell
.\Collector.exe --install-playwright
```

После установки перезапустить `Collector.exe --desktop --port 5057`.

## 5. Где хранится локальная БД
По умолчанию на Windows:

`%LOCALAPPDATA%\SmsControl\smscontrol.db`

Это отдельный путь от папки с `.exe`, поэтому при замене файла приложения БД сохраняется.

## 6. Обновление версии без потери данных
1. Остановить текущий `Collector.exe`.
2. Обновить через новый `SmsControlSetup.exe` или заменить `Collector.exe` вручную.
3. Запустить снова: `.\Collector.exe --desktop --port 5057`.

Локальная БД, история, очередь, настройки и диалоги останутся, если путь БД не меняли вручную.

## 7. Явное указание пути БД (опционально)
Если нужен фиксированный путь:

```powershell
.\Collector.exe --desktop --port 5057 --db-path "C:\SmsControl\data\smscontrol.db"
```

При обновлениях использовать тот же `--db-path`.
