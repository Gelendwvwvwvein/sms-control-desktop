const STATUS_TEXT = {
  queued: "В очереди",
  running: "В работе",
  retry: "Повтор",
  stopped: "Остановлено",
  sent: "Отправлено",
  failed: "Ошибка"
};

const RUN_SESSION_STATUS_TEXT = {
  planned: "Готово",
  running: "Выполняется",
  stopped: "Остановлено",
  completed: "Завершено"
};

const QUEUE_STATUS_PRIORITY = {
  running: 0,
  retry: 1,
  queued: 2,
  stopped: 3,
  failed: 4,
  sent: 5
};

const QUEUE_RETRYABLE_STATUSES = new Set(["failed", "stopped"]);
const QUEUE_REMOVABLE_STATUSES = new Set(["queued", "retry", "stopped"]);
const QUEUE_TEMPLATE_ASSIGNABLE_STATUSES = new Set(["queued", "retry"]);

const PREVIEW_STATUS_TEXT = {
  empty: "Нет превью",
  ready: "Готово",
  needs_debt: "Нужна сумма долга",
  error: "Ошибка превью"
};

const ALERT_STATUS_TEXT = {
  active: "Актуально",
  resolved: "Решено",
  irrelevant: "Неактуально"
};

const CHANNEL_STATUS_TEXT = {
  online: "Онлайн",
  offline: "Оффлайн",
  error: "Ошибка устройства",
  unknown: "Не проверен"
};

const MSK_TZ_LABEL = {
  "-1": "МСК -1",
  "0": "МСК 0",
  "1": "МСК +1",
  "2": "МСК +2",
  "3": "МСК +3",
  "4": "МСК +4",
  "5": "МСК +5",
  "6": "МСК +6",
  "7": "МСК +7",
  "8": "МСК +8",
  "9": "МСК +9"
};

const MSK_UTC_OFFSET_MIN = 0;
const DEFAULT_WORK_WINDOW_START = "08:00";
const DEFAULT_WORK_WINDOW_END = "21:00";
const DEFAULT_DEBT_BUFFER_AMOUNT = 2000;
const ROCKETMAN_CARD_URL_PREFIX = "https://rocketman.ru/manager/collector-comment/view?id=";
const DEFAULT_TEMPLATE_KIND = "sms1";
const DEFAULT_TEMPLATE_OVERDUE_MODE = "range";
const TEMPLATE_TYPE_ORDER = ["sms1", "sms1_regular", "sms2", "sms3", "ka1", "ka2", "ka_final"];
const TEMPLATE_TYPE_CONFIG = {
  sms1: {
    label: "СМС 1",
    rangeText: "3-5 дней",
    minOverdue: 3,
    maxOverdue: 5,
    autoAssign: true,
    ruleHint: "Первичное сообщение: только клиенты с просрочкой 3-5 дней."
  },
  sms1_regular: {
    label: "СМС 1 (постоянный клиент)",
    rangeText: "3-5 дней",
    minOverdue: 3,
    maxOverdue: 5,
    autoAssign: false,
    ruleHint: "Вариант для постоянных клиентов. В автоподбор не включается, назначается вручную."
  },
  sms2: {
    label: "СМС 2",
    rangeText: "6-20 дней",
    minOverdue: 6,
    maxOverdue: 20,
    autoAssign: true,
    ruleHint: "Повторное сообщение: для клиентов с просрочкой 6-20 дней."
  },
  sms3: {
    label: "СМС 3",
    rangeText: "21-29 дней",
    minOverdue: 21,
    maxOverdue: 29,
    autoAssign: true,
    ruleHint: "Третье сообщение до этапа КА: для клиентов с просрочкой 21-29 дней."
  },
  ka1: {
    label: "СМС от КА1",
    rangeText: "30-45 дней",
    minOverdue: 30,
    maxOverdue: 45,
    autoAssign: true,
    ruleHint: "Первое сообщение от КА: клиенты с просрочкой 30-45 дней."
  },
  ka2: {
    label: "СМС от КА2",
    rangeText: "46-50 дней",
    minOverdue: 46,
    maxOverdue: 50,
    autoAssign: true,
    ruleHint: "Повторное сообщение от КА: клиенты с просрочкой 46-50 дней."
  },
  ka_final: {
    label: "СМС от КА (финал)",
    rangeText: "51-59 дней",
    minOverdue: 51,
    maxOverdue: 59,
    autoAssign: true,
    ruleHint: "Финальное сообщение КА: клиенты с просрочкой 51-59 дней."
  }
};
const DEFAULT_TEMPLATE_RULE_TYPES = TEMPLATE_TYPE_ORDER.map((kind, index) => {
  const cfg = TEMPLATE_TYPE_CONFIG[kind];
  return {
    id: kind,
    name: cfg.label,
    overdueMode: DEFAULT_TEMPLATE_OVERDUE_MODE,
    overdueFromDays: cfg.minOverdue,
    overdueToDays: cfg.maxOverdue,
    overdueExactDay: null,
    autoAssign: cfg.autoAssign !== false,
    sortOrder: (index + 1) * 10
  };
});
const DEFAULT_COMMENT_RULES = {
  sms2: "смс2",
  sms3: "смс3",
  ka1: "смс от ка",
  kaN: "смс ка{n}",
  kaFinal: "смс ка фин"
};
const DEFAULT_NEW_MANUAL_PRESET_TEXT = "{полное_фио}, добрый день. Уточните, пожалуйста, дату оплаты. Ориентировочная сумма {сумма_долга}.";

const state = {
  runTimer: null,
  runPollTimer: null,
  runPollBusy: false,
  eventStream: null,
  eventStreamSinceId: 0,
  eventStreamConnected: false,
  eventStreamRetryTimer: null,
  eventStreamRetryAttempt: 0,
  criticalErrorToastCooldownUntilMs: 0,
  criticalErrorToastSuppressed: 0,
  realtimeRefreshTimer: null,
  realtimeRefreshBusy: false,
  forecastPollTimer: null,
  forecastPollBusy: false,
  dialogPollTimer: null,
  dialogPollBusy: false,
  draftSaveTimersByPhone: {},
  dialogLastRenderedMessageByPhone: {},
  dialogForceScrollPhone: "",
  runRuntime: null,
  runForecast: null,
  runHistory: [],
  runHistoryTotal: 0,
  runHistorySelectedSessionId: null,
  dialogPreview: {
    enabled: false,
    phone: "",
    jobId: null,
    client: "",
    status: "empty",
    text: "",
    variablesJson: "",
    updatedAtUtc: "",
    errorCode: "",
    errorDetail: "",
    hasMessageOverride: false,
    messageOverrideText: "",
    editorSourceJobId: null,
    editorText: "",
    editorDirty: false,
    editorSaving: false
  },
  reportSentToday: null,
  reportFailedToday: null,
  reportWeeklyDays: [],
  runForecastRequestSeq: 0,
  runCanStart: true,
  runStartBlockMessage: "",
  planPrepared: false,
  planStale: false,
  excludedClientIds: new Set(),
  queueSessionId: null,
  queueSession: null,
  clientsDb: {
    actualized: false,
    syncing: false,
    syncedAt: null,
    rows: 0,
    snapshotId: null,
    sourceMode: ""
  },
  clients: [],
  channels: [],
  channelFormMode: "create",
  channelEditId: null,
  templates: [],
  queue: [],
  dialogs: [],
  stoplist: [],
  alerts: [],
  alertView: "active",
  commentRules: { ...DEFAULT_COMMENT_RULES },
  templateRuleTypes: DEFAULT_TEMPLATE_RULE_TYPES.map((x) => ({ ...x })),
  selectedTemplateRuleTypeId: null,
  templateRuleTypeCreateMode: false,
  templateRuleTypeCreateDraft: {
    id: "",
    name: "",
    overdueMode: DEFAULT_TEMPLATE_OVERDUE_MODE,
    overdueFromDays: 0,
    overdueToDays: 0,
    overdueExactDay: null,
    autoAssign: true,
    sortOrder: 0
  },
  runFilters: {
    tz: new Set([-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
    overdueRanges: new Set(),
    exactOverdue: ""
  },
  clientsViewFilters: {
    search: "",
    tz: "all",
    overdue: "all"
  },
  selectedClients: new Set(),
  selectedQueue: new Set(),
  selectedStopList: new Set(),
  manualReplyPresets: [],
  selectedManualPresetId: null,
  manualPresetCreateMode: false,
  manualPresetCreateDraft: {
    title: "",
    text: DEFAULT_NEW_MANUAL_PRESET_TEXT
  },
  selectedTemplateId: null,
  templateCreateMode: false,
  templateCreateDraft: {
    kind: DEFAULT_TEMPLATE_KIND,
    overdueMode: DEFAULT_TEMPLATE_OVERDUE_MODE,
    overdueFromDays: DEFAULT_TEMPLATE_RULE_TYPES[0]?.overdueFromDays ?? 0,
    overdueToDays: DEFAULT_TEMPLATE_RULE_TYPES[0]?.overdueToDays ?? 0,
    overdueExactDay: null,
    autoAssign: DEFAULT_TEMPLATE_RULE_TYPES[0]?.autoAssign !== false,
    name: "",
    text: ""
  },
  templateEditorBaseline: {
    kind: DEFAULT_TEMPLATE_KIND,
    overdueMode: DEFAULT_TEMPLATE_OVERDUE_MODE,
    overdueFromDays: DEFAULT_TEMPLATE_RULE_TYPES[0]?.overdueFromDays ?? 0,
    overdueToDays: DEFAULT_TEMPLATE_RULE_TYPES[0]?.overdueToDays ?? 0,
    overdueExactDay: null,
    autoAssign: DEFAULT_TEMPLATE_RULE_TYPES[0]?.autoAssign !== false,
    name: "",
    text: ""
  },
  settingsTemplateCommentTemplateId: null,
  selectedDialogId: null,
  settingsBaseline: null,
  bulkDebtInProgress: false
};

const titleMap = {
  run: ["Запуск", "Дневной цикл и оперативный контроль"],
  settings: ["Настройки", "Параметры Rocketman, каналов и правил отправки"],
  templates: ["Шаблоны", "Редактор текстов, переменных и статусов"],
  clients: ["База клиентов", "Полный набор клиентов после актуализации Rocketman"],
  queue: ["Очередь", "Управление задачами отправки и контроль статусов"],
  dialogs: ["Диалоги", "История сообщений по клиентам и ручные действия"],
  stoplist: ["Стоп-лист", "Ручная блокировка отправки по номеру телефона"],
  reports: ["Отчеты", "Дневная и недельная статистика работы"]
};

const UI = window.UiKit;
const $ = UI.$;
function resolveApiBase() {
  const explicit = typeof window !== "undefined" ? window.__SMS_CONTROL_API_BASE__ : "";
  if (typeof explicit === "string" && explicit.trim()) {
    return explicit.trim().replace(/\/+$/, "");
  }

  if (typeof window !== "undefined" && window.location && /^https?:$/i.test(window.location.protocol)) {
    return window.location.origin.replace(/\/+$/, "");
  }

  return "http://127.0.0.1:5057";
}

const SETTINGS_API_BASE = resolveApiBase();
const buttonFeedbackState = {
  lastClickedBtn: null,
  lastClickedAtMs: 0,
  loadingByButton: new WeakMap()
};

function startButtonLoading(btn) {
  if (!(btn instanceof HTMLButtonElement)) return;
  const current = Number(buttonFeedbackState.loadingByButton.get(btn) || 0);
  buttonFeedbackState.loadingByButton.set(btn, current + 1);
  if (current > 0) return;
  btn.classList.add("is-loading");
  btn.setAttribute("aria-busy", "true");
}

function stopButtonLoading(btn) {
  if (!(btn instanceof HTMLButtonElement)) return;
  const current = Number(buttonFeedbackState.loadingByButton.get(btn) || 0);
  if (current <= 1) {
    buttonFeedbackState.loadingByButton.delete(btn);
    btn.classList.remove("is-loading");
    btn.removeAttribute("aria-busy");
    return;
  }
  buttonFeedbackState.loadingByButton.set(btn, current - 1);
}
