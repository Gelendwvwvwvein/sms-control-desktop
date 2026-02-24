namespace Collector.Api;

/// <summary>
/// Каталог кодов ошибок с рекомендуемыми действиями для оператора.
/// Используется для обогащения ApiErrorDto в ответах API.
/// </summary>
public static class ErrorCatalog
{
    public sealed record Entry(string Severity, bool Retryable, string OperatorAction);

    private static readonly IReadOnlyDictionary<string, Entry> Catalog = new Dictionary<string, Entry>(StringComparer.OrdinalIgnoreCase)
    {
        ["CFG_REQUIRED_MISSING"] = new("warning", false, "Заполните обязательные поля и повторите запрос."),
        ["COMMENT_CARD_URL_REQUIRED"] = new("warning", false, "Укажите URL карточки клиента."),
        ["COMMENT_TEXT_REQUIRED"] = new("warning", false, "Укажите текст комментария."),
        ["COMMENT_SETTINGS_MISSING"] = new("critical", false, "Проверьте настройки Rocketman (логин, пароль)."),
        ["ROCKETMAN_AUTH_FAILED"] = new("critical", false, "Проверьте логин и пароль Rocketman в настройках."),
        ["ROCKETMAN_UNAVAILABLE"] = new("critical", true, "Проверьте доступность Rocketman и интернет-соединение."),
        ["ROCKETMAN_DOM_CHANGED"] = new("critical", false, "Обновите селекторы в конфигурации. Обратитесь к разработчику."),
        ["SYNC_SELECTORS_NOT_FOUND"] = new("critical", false, "Селекторы не найдены. Проверьте конфигурацию Rocketman."),
        ["SYNC_INVALID_STATE"] = new("warning", false, "Сначала остановите текущую операцию синхронизации."),
        ["SYNC_PLAYWRIGHT_NOT_INSTALLED"] = new("critical", false, "Установите Chromium для Playwright командой `Collector.exe --install-playwright`."),
        ["SYNC_PLAYWRIGHT_ERROR"] = new("critical", true, "Проверьте запуск Chromium на этом ПК и доступ к Rocketman, затем повторите актуализацию."),
        ["DB_UNAVAILABLE"] = new("critical", true, "Проверьте доступность файла БД и права доступа."),
        ["CHANNEL_NOT_FOUND"] = new("warning", false, "Канал не найден. Обновите список каналов."),
        ["CHANNEL_CONFIG_INVALID"] = new("critical", false, "Проверьте endpoint и токен канала в настройках."),
        ["CHANNEL_OFFLINE_OR_SIM_BLOCKED"] = new("warning", true, "Проверьте устройство, SIM-карту и сеть. Нажмите «Проверить канал»."),
        ["CHANNEL_UNAVAILABLE"] = new("warning", true, "Нет доступных каналов. Включите или проверьте устройства."),
        ["CHANNEL_NAME_REQUIRED"] = new("warning", false, "Укажите название канала."),
        ["CHANNEL_ENDPOINT_REQUIRED"] = new("warning", false, "Укажите URL endpoint Traccar."),
        ["CHANNEL_ENDPOINT_INVALID"] = new("warning", false, "Endpoint должен быть валидным URL (http или https)."),
        ["CHANNEL_TOKEN_REQUIRED"] = new("warning", false, "Укажите токен API Traccar."),
        ["STOP_LIST_PHONE_INVALID"] = new("warning", false, "Укажите корректный номер телефона (10–15 цифр)."),
        ["STOP_LIST_PHONES_EMPTY"] = new("warning", false, "Выберите хотя бы один номер."),
        ["STOP_LIST_PHONES_TOO_MANY"] = new("warning", false, "Слишком много записей. Разбейте операцию на части."),
        ["STOP_LIST_IDS_EMPTY"] = new("warning", false, "Выберите записи для удаления."),
        ["STOP_LIST_IDS_TOO_MANY"] = new("warning", false, "Слишком много записей. Разбейте операцию на части."),
        ["STOP_LIST_NOT_FOUND"] = new("info", false, "Запись не найдена. Обновите список."),
        ["TEMPLATE_NOT_FOUND"] = new("warning", false, "Шаблон не найден. Обновите список шаблонов."),
        ["TEMPLATE_NAME_REQUIRED"] = new("warning", false, "Укажите название шаблона."),
        ["TEMPLATE_KIND_REQUIRED"] = new("warning", false, "Укажите тип шаблона."),
        ["TEMPLATE_KIND_INVALID"] = new("warning", false, "Выберите корректный тип шаблона из списка."),
        ["TEMPLATE_STATUS_REQUIRED"] = new("warning", false, "Укажите статус шаблона (draft или active)."),
        ["TEMPLATE_STATUS_INVALID"] = new("warning", false, "Статус должен быть «Черновик» или «Актуальный»."),
        ["TEMPLATE_TEXT_REQUIRED"] = new("warning", false, "Укажите текст шаблона."),
        ["TEMPLATE_NOT_RESOLVED"] = new("warning", false, "Нет подходящего шаблона по просрочке. Добавьте или активируйте шаблон."),
        ["TEMPLATE_RENDER_EMPTY"] = new("warning", false, "Шаблон дал пустой текст. Проверьте переменные {полное_фио} и {сумма_долга}."),
        ["MANUAL_PRESET_NOT_FOUND"] = new("warning", false, "Типовой ответ не найден. Обновите список."),
        ["MANUAL_PRESET_TITLE_REQUIRED"] = new("warning", false, "Укажите название типового ответа."),
        ["DIALOG_NOT_FOUND"] = new("info", false, "Диалог не найден."),
        ["DIALOG_TEXT_REQUIRED"] = new("warning", false, "Введите текст сообщения."),
        ["DIALOG_TEXT_TOO_LONG"] = new("warning", false, "Текст сообщения не должен превышать 2000 символов."),
        ["MANUAL_TZ_UNKNOWN"] = new("warning", false, "Часовой пояс клиента не определён. Проверьте данные клиента в базе."),
        ["MANUAL_OUT_OF_WORK_WINDOW"] = new("warning", false, "Локальное время клиента вне рабочего окна. Отправка разрешена в настраиваемые часы."),
        ["INVALID_CLIENT_PHONE"] = new("warning", false, "Проверьте номер телефона клиента в базе."),
        ["PHONE_EMPTY"] = new("warning", false, "Номер телефона клиента пуст."),
        ["GATEWAY_TIMEOUT"] = new("warning", true, "Таймаут при отправке. Проверьте устройство и сеть, нажмите «Переотправить»."),
        ["GATEWAY_SEND_FAILED"] = new("warning", true, "Ошибка отправки. Проверьте канал, нажмите «Проверить канал» или «Переотправить»."),
        ["GATEWAY_UNKNOWN_STATUS"] = new("warning", true, "Статус доставки не распознан. Проверьте устройство, нажмите «Переотправить»."),
        ["COMMENT_WRITE_FAILED"] = new("critical", true, "SMS отправлена, но комментарий в договор не записан. Запишите вручную в Rocketman."),
        ["RUN_INTERRUPTED"] = new("info", true, "Задача прервана рестартом. Обработка продолжится автоматически."),
        ["RUN_LIVE_DISPATCH_BLOCKED"] = new("warning", false, "Включите разрешение live-рассылки в настройках для запуска боевой очереди."),
        ["RUN_SESSION_NOT_FOUND"] = new("warning", false, "Сессия не найдена. Сформируйте очередь заново."),
        ["RUN_SESSION_ID_INVALID"] = new("warning", false, "Укажите корректный идентификатор сессии."),
        ["RUN_NOT_RUNNING"] = new("info", false, "Сессия уже остановлена."),
        ["RUN_ALREADY_RUNNING"] = new("warning", false, "Другая сессия уже выполняется. Дождитесь завершения или остановите её."),
        ["RUN_QUEUE_NOT_FOUND"] = new("warning", false, "Сначала сформируйте плановую очередь."),
        ["RUN_QUEUE_EMPTY"] = new("warning", false, "Очередь пуста. Измените фильтры и сформируйте очередь заново."),
        ["RUN_QUEUE_STALE"] = new("warning", false, "Актуализируйте базу клиентов и сформируйте очередь заново."),
        ["RUN_SNAPSHOT_MISSING"] = new("warning", false, "Сначала актуализируйте базу клиентов."),
        ["RUN_SESSION_SNAPSHOT_MISSING"] = new("warning", false, "Snapshot сессии не найден. Сформируйте очередь заново."),
        ["RUN_START_STATE_INVALID"] = new("warning", false, "Запуск недоступен в текущем состоянии сессии."),
        ["RUN_STOPPED_BY_OPERATOR"] = new("info", false, "Остановлено оператором."),
        ["QUEUE_FILTER_EMPTY"] = new("warning", false, "Выберите хотя бы один фильтр: часовой пояс или диапазон просрочки."),
        ["QUEUE_FILTER_INVALID"] = new("warning", false, "Проверьте параметры фильтра очереди."),
        ["QUEUE_FILTER_TZ_INVALID"] = new("warning", false, "Часовые пояса должны быть в диапазоне -12..+14."),
        ["QUEUE_FILTER_RANGE_INVALID"] = new("warning", false, "Проверьте формат диапазонов просрочки (например, 3-15)."),
        ["QUEUE_SNAPSHOT_NOT_FOUND"] = new("warning", false, "Сначала актуализируйте базу клиентов."),
        ["QUEUE_RUN_SESSION_NOT_FOUND"] = new("warning", false, "Сессия очереди не найдена. Сформируйте очередь заново."),
        ["QUEUE_JOB_NOT_FOUND"] = new("warning", false, "Задача не найдена. Обновите список очереди."),
        ["QUEUE_REMOVE_INVALID_STATE"] = new("warning", false, "Удаление доступно только для сессии в статусе «Запланировано»."),
        ["CLIENT_NOT_FOUND"] = new("info", false, "Клиент не найден. Обновите базу."),
        ["ALERT_NOT_FOUND"] = new("info", false, "Уведомление не найдено.")
    };

    public static ApiErrorDto Enrich(ApiErrorDto dto)
    {
        if (!Catalog.TryGetValue(dto.Code, out var entry))
        {
            return dto;
        }

        return new ApiErrorDto
        {
            Code = dto.Code,
            Message = dto.Message,
            Severity = entry.Severity,
            Retryable = entry.Retryable,
            OperatorAction = entry.OperatorAction
        };
    }

    public static IReadOnlyDictionary<string, Entry> GetAll() => Catalog;
}
