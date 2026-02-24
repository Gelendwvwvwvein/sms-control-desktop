const STATUS_TEXT = {
  queued: "В очереди",
  running: "В работе",
  retry: "Повтор",
  stopped: "Остановлено",
  sent: "Отправлено",
  failed: "Ошибка"
};

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

// В проекте МСК используется как базовая зона (МСК+0).
const MSK_UTC_OFFSET_MIN = 0;
const DEFAULT_WORK_WINDOW_START = "08:00";
const DEFAULT_WORK_WINDOW_END = "21:00";
const ROCKETMAN_CARD_URL_PREFIX = "https://rocketman.ru/manager/collector-comment/view?id=";
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
const DEFAULT_TEMPLATE_KIND = "sms1";
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
  commentRules: {
    sms2: "смс2",
    sms3: "смс3",
    ka1: "смс от ка",
    kaN: "смс ка{n}",
    kaFinal: "смс ка фин"
  },
  runFilters: {
    tz: new Set([-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
    overdueRanges: new Set(["3-15", "16-29", "30-45", "46-59"]),
    exactDay: null
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
    name: "",
    text: ""
  },
  templateEditorBaseline: { kind: DEFAULT_TEMPLATE_KIND, name: "", text: "" },
  selectedDialogId: null,
  settingsBaseline: null
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
const SETTINGS_API_BASE = "http://127.0.0.1:5057";
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

function resolveRequestButton(options = {}) {
  const explicit = options.requestButton;
  if (explicit instanceof HTMLButtonElement) {
    return explicit;
  }
  if (options.disableButtonFeedback === true) {
    return null;
  }
  const candidate = buttonFeedbackState.lastClickedBtn;
  if (!(candidate instanceof HTMLButtonElement)) {
    return null;
  }
  const elapsed = currentTimeMs() - Number(buttonFeedbackState.lastClickedAtMs || 0);
  if (elapsed > 1800) {
    return null;
  }
  return candidate;
}

function initButtonFeedback() {
  document.addEventListener("click", (event) => {
    const btn = event.target instanceof Element
      ? event.target.closest("button")
      : null;
    if (!(btn instanceof HTMLButtonElement) || btn.disabled) return;
    buttonFeedbackState.lastClickedBtn = btn;
    buttonFeedbackState.lastClickedAtMs = currentTimeMs();
    btn.classList.remove("is-pressed");
    void btn.offsetWidth;
    btn.classList.add("is-pressed");
    window.setTimeout(() => btn.classList.remove("is-pressed"), 180);
  }, true);
}

function apiUrl(path) {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${SETTINGS_API_BASE}${normalizedPath}`;
}

async function fetchApiJson(path, options = {}) {
  const requestOptions = {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    },
    body: options.body
  };

  const requestButton = resolveRequestButton(options);
  if (requestButton) {
    startButtonLoading(requestButton);
  }

  try {
    const response = await fetch(apiUrl(path), requestOptions);
    const contentType = (response.headers.get("content-type") || "").toLowerCase();
    const payload = contentType.includes("application/json")
      ? await response.json()
      : await response.text();

    if (!response.ok) {
      const message = payload?.message || payload?.title || `HTTP ${response.status}`;
      const action = payload?.operatorAction;
      const fullMessage = action ? `${message} Рекомендация: ${action}` : message;
      const err = new Error(fullMessage);
      err.apiCode = payload?.code;
      err.operatorAction = action;
      err.severity = payload?.severity;
      throw err;
    }

    return payload;
  } finally {
    if (requestButton) {
      stopButtonLoading(requestButton);
    }
  }
}

function setNotice(id, text, tone = "warning") {
  UI.notice(id, text, tone);
}

function currentTimeMs() {
  return Date.now();
}

function nowHHMM() {
  return new Date(currentTimeMs()).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" });
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function mskNowUtcMs() {
  return currentTimeMs();
}

function formatMskHHMM(utcMs) {
  const shifted = new Date(utcMs + MSK_UTC_OFFSET_MIN * 60 * 1000);
  return `${pad2(shifted.getHours())}:${pad2(shifted.getMinutes())}`;
}

function formatMskDateTime(utcMs) {
  const shifted = new Date(utcMs + MSK_UTC_OFFSET_MIN * 60 * 1000);
  return `${pad2(shifted.getDate())}.${pad2(shifted.getMonth() + 1)}.${shifted.getFullYear()} ${pad2(shifted.getHours())}:${pad2(shifted.getMinutes())}`;
}

function formatLocalHHMM(utcMs) {
  const date = new Date(utcMs);
  return `${pad2(date.getHours())}:${pad2(date.getMinutes())}`;
}

function formatLocalDateTime(utcMs) {
  const date = new Date(utcMs);
  return `${pad2(date.getDate())}.${pad2(date.getMonth() + 1)}.${date.getFullYear()} ${pad2(date.getHours())}:${pad2(date.getMinutes())}`;
}

function isSameLocalDate(aMs, bMs) {
  const a = new Date(aMs);
  const b = new Date(bMs);
  return a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate();
}

function queuePlanCellText(job) {
  const hasRunStarted = Boolean(state.queueSession?.startedAtUtc);
  if (!hasRunStarted) {
    return "После запуска";
  }

  const plannedMs = safeDateMs(job?.plannedAtUtc);
  if (!Number.isFinite(plannedMs)) {
    return "—";
  }

  return isSameLocalDate(plannedMs, currentTimeMs())
    ? formatLocalHHMM(plannedMs)
    : formatLocalDateTime(plannedMs);
}

function previewStatusText(status) {
  return PREVIEW_STATUS_TEXT[String(status || "empty")] || PREVIEW_STATUS_TEXT.empty;
}

function parsePreviewVariablesJson(raw) {
  if (typeof raw !== "string" || !raw.trim()) return null;
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function resetDialogPreview() {
  state.dialogPreview = {
    enabled: Boolean(state.dialogPreview?.enabled),
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
  };
}

function setDialogPreviewFromQueueJob(job) {
  if (!job) {
    resetDialogPreview();
    return;
  }

  state.dialogPreview.phone = toApiPhone(job.phone || "");
  const nextJobId = Number(job.id || 0) || null;
  state.dialogPreview.jobId = nextJobId;
  state.dialogPreview.client = String(job.client || "").trim();
  state.dialogPreview.status = String(job.previewStatus || "empty").trim() || "empty";
  state.dialogPreview.text = String(job.previewText || "");
  state.dialogPreview.variablesJson = String(job.previewVariablesJson || "");
  state.dialogPreview.updatedAtUtc = String(job.previewUpdatedAtUtc || "");
  state.dialogPreview.errorCode = String(job.previewErrorCode || "");
  state.dialogPreview.errorDetail = String(job.previewErrorDetail || "");
  state.dialogPreview.hasMessageOverride = Boolean(job.hasMessageOverride);
  state.dialogPreview.messageOverrideText = String(job.messageOverrideText || "");
  if (state.dialogPreview.editorSourceJobId !== nextJobId || !state.dialogPreview.editorDirty) {
    state.dialogPreview.editorSourceJobId = nextJobId;
    state.dialogPreview.editorText = state.dialogPreview.text;
    state.dialogPreview.editorDirty = false;
  }
}

function findQueueJobForPhone(phone) {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) return null;

  const candidates = state.queue
    .filter((job) => normalizePhone(job.phone) === normalizedPhone);
  if (candidates.length === 0) return null;

  const statusPriority = {
    running: 0,
    retry: 1,
    queued: 2,
    stopped: 3,
    failed: 4,
    sent: 5
  };

  const sorted = [...candidates].sort((a, b) => {
    const byStatus = (statusPriority[a.status] ?? 99) - (statusPriority[b.status] ?? 99);
    if (byStatus !== 0) return byStatus;
    const aPlan = safeDateMs(a.plannedAtUtc) ?? Number.MAX_SAFE_INTEGER;
    const bPlan = safeDateMs(b.plannedAtUtc) ?? Number.MAX_SAFE_INTEGER;
    if (aPlan !== bPlan) return aPlan - bPlan;
    return Number(a.id || 0) - Number(b.id || 0);
  });

  return sorted[0] || null;
}

function resolveDialogTimezoneOffset(dialog) {
  if (!dialog) return null;
  const normalizedPhone = normalizePhone(dialog.phone || "");
  if (!normalizedPhone) return null;

  const queueJob = findQueueJobForPhone(normalizedPhone);
  if (queueJob && Number.isFinite(Number(queueJob.tzOffset))) {
    return Number(queueJob.tzOffset);
  }

  const client = state.clients.find((item) => normalizePhone(item.phone) === normalizedPhone) || null;
  if (client && Number.isFinite(Number(client.tzOffset))) {
    return Number(client.tzOffset);
  }

  return null;
}

function syncDialogPreviewWithQueue() {
  const selectedDialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId)) || null;
  if (!selectedDialog) {
    resetDialogPreview();
    return;
  }

  const queueJob = findQueueJobForPhone(selectedDialog.phone);
  if (!queueJob) {
    state.dialogPreview.phone = toApiPhone(selectedDialog.phone || "");
    state.dialogPreview.jobId = null;
    state.dialogPreview.client = String(selectedDialog.fio || "").trim();
    state.dialogPreview.status = "empty";
    state.dialogPreview.text = "";
    state.dialogPreview.variablesJson = "";
    state.dialogPreview.updatedAtUtc = "";
    state.dialogPreview.errorCode = "PREVIEW_NO_QUEUE_JOB";
    state.dialogPreview.errorDetail = "Клиент сейчас не находится в плановой/активной очереди.";
    state.dialogPreview.hasMessageOverride = false;
    state.dialogPreview.messageOverrideText = "";
    state.dialogPreview.editorSourceJobId = null;
    state.dialogPreview.editorText = "";
    state.dialogPreview.editorDirty = false;
    state.dialogPreview.editorSaving = false;
    return;
  }

  setDialogPreviewFromQueueJob(queueJob);
}

function safeDateMs(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string" || !value.trim()) return null;
  const raw = value.trim();

  // API иногда отдает UTC без суффикса "Z" (например, из SQLite DateTime),
  // поэтому такие строки интерпретируем как UTC, а не как локальное время.
  const hasExplicitTz = /(?:[zZ]|[+-]\d{2}:\d{2})$/.test(raw);
  const isoNoTz = /^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2})(?::(\d{2})(\.\d{1,7})?)?$/.exec(raw);
  if (!hasExplicitTz && isoNoTz) {
    const year = Number(isoNoTz[1]);
    const month = Number(isoNoTz[2]) - 1;
    const day = Number(isoNoTz[3]);
    const hour = Number(isoNoTz[4]);
    const minute = Number(isoNoTz[5]);
    const second = Number(isoNoTz[6] || 0);
    const fraction = (isoNoTz[7] || "").slice(1);
    const millis = Number((fraction + "000").slice(0, 3) || 0);
    return Date.UTC(year, month, day, hour, minute, second, millis);
  }

  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function dialogLastActivityUtcMs(dialog) {
  if (!dialog) return mskNowUtcMs();
  const fromUpdated = safeDateMs(dialog.updatedAt);
  if (Number.isFinite(fromUpdated)) return fromUpdated;
  return mskNowUtcMs();
}

function formatDurationMinutes(minutes) {
  if (!Number.isFinite(minutes) || minutes <= 0) return "0 мин";
  const total = Math.round(minutes);
  const hours = Math.floor(total / 60);
  const mins = total % 60;
  if (hours === 0) return `${mins} мин`;
  if (mins === 0) return `${hours} ч`;
  return `${hours} ч ${mins} мин`;
}

function parseHmToMinutes(value) {
  const text = String(value || "").trim();
  const match = /^([01]\d|2[0-3]):([0-5]\d)$/.exec(text);
  if (!match) return null;
  return Number(match[1]) * 60 + Number(match[2]);
}

function minutesToHm(value) {
  const min = Math.max(0, Math.min(23 * 60 + 59, Number(value) || 0));
  const h = Math.floor(min / 60);
  const m = min % 60;
  return `${pad2(h)}:${pad2(m)}`;
}

function getWorkWindowRange() {
  const startRaw = $("cfgWorkWindowStart")?.value ?? DEFAULT_WORK_WINDOW_START;
  const endRaw = $("cfgWorkWindowEnd")?.value ?? DEFAULT_WORK_WINDOW_END;
  let startMin = parseHmToMinutes(startRaw);
  let endMin = parseHmToMinutes(endRaw);

  if (!Number.isFinite(startMin) || !Number.isFinite(endMin) || endMin <= startMin) {
    startMin = parseHmToMinutes(DEFAULT_WORK_WINDOW_START);
    endMin = parseHmToMinutes(DEFAULT_WORK_WINDOW_END);
  }

  const startText = minutesToHm(startMin);
  const endText = minutesToHm(endMin);
  return {
    startMin,
    endMin,
    startText,
    endText,
    label: `${startText}-${endText}`
  };
}

function getGapMinutes() {
  const value = Number($("cfgGap").value);
  if (!Number.isFinite(value) || value <= 0) return 8;
  return Math.round(value);
}

function normalizePhone(phone) {
  let digits = (phone || "").replace(/\D/g, "");
  if (digits.length === 10) {
    digits = `7${digits}`;
  } else if (digits.length === 11 && digits.startsWith("8")) {
    digits = `7${digits.slice(1)}`;
  }
  return digits;
}

function toApiPhone(phone) {
  const digits = normalizePhone(phone);
  return digits ? `+${digits}` : "";
}

function activeTabId() {
  return document.querySelector(".tab-btn.active")?.dataset?.tab || "";
}

function parseAmount(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string") return null;
  const normalized = value
    .replace(/\u00A0/g, " ")
    .replace(/[^\d,.\-]/g, "")
    .replace(",", ".");
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatApproxDebtFromTotal(totalWithCommission) {
  const numeric = parseAmount(totalWithCommission);
  if (!Number.isFinite(numeric)) return "";
  const rounded = Math.round((numeric + 2000) / 1000) * 1000;
  return `${rounded.toLocaleString("ru-RU")} руб.`;
}

function formatExactDebtFromRaw(totalWithCommissionRaw) {
  const raw = String(totalWithCommissionRaw || "").trim();
  if (!raw) return "";
  if (/[₽рР][убУБ]?/u.test(raw)) {
    return raw;
  }
  const numeric = parseAmount(raw);
  if (!Number.isFinite(numeric)) {
    return raw;
  }
  const formatted = numeric.toLocaleString("ru-RU", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
  return `${formatted} ₽`;
}

function debtCellText(row) {
  if (!row) return "Не загружена";
  const status = String(row.debtStatus || "").trim().toLowerCase();
  if (status === "error") {
    const code = String(row.debtErrorCode || "").trim();
    return code ? `Ошибка (${code})` : "Ошибка";
  }

  const exact = formatExactDebtFromRaw(row.totalWithCommissionRaw);
  const approx = String(row.debtApproxText || "").trim() || formatApproxDebtFromTotal(row.totalWithCommissionRaw);
  if (approx && exact) {
    return `${approx} (точно: ${exact})`;
  }
  if (approx) return approx;
  if (exact) return `Точно: ${exact}`;
  return "Не загружена";
}

function getManualPresetById(presetId) {
  return state.manualReplyPresets.find((x) => x.id === presetId) || null;
}

function firstNameFromFio(fio) {
  const parts = (fio || "").trim().split(/\s+/).filter(Boolean);
  if (parts.length >= 2) return parts[1];
  if (parts.length === 1) return parts[0];
  return "Клиент";
}

function findClientByPhone(phone) {
  const normalized = normalizePhone(phone);
  return state.clients.find((c) => normalizePhone(c.phone) === normalized) || null;
}

function findQueueClientByPhone(phone) {
  const normalized = normalizePhone(phone);
  return state.queue.find((q) => normalizePhone(q.phone) === normalized) || null;
}

function resolveDialogFioByPhone(phone, fallback = "") {
  const fromClients = findClientByPhone(phone);
  if (fromClients?.client) return fromClients.client;

  const fromQueue = findQueueClientByPhone(phone);
  if (fromQueue?.client) return fromQueue.client;

  const fromDialogs = findDialogByPhone(phone);
  if (fromDialogs?.fio) return fromDialogs.fio;

  const trimmedFallback = String(fallback || "").trim();
  if (trimmedFallback) return trimmedFallback;

  const apiPhone = toApiPhone(phone);
  return apiPhone || "Клиент";
}

function buildClientMetaIndexByPhone() {
  const index = new Map();
  const put = (phone, fio, contractNumber, cardUrl = "") => {
    const normalized = normalizePhone(phone);
    if (!normalized || index.has(normalized)) return;
    index.set(normalized, {
      fio: String(fio || "").trim(),
      contractNumber: String(contractNumber || "").trim(),
      cardUrl: String(cardUrl || "").trim()
    });
  };

  state.clients.forEach((client) => {
    put(client.phone, client.client, client.contractNumber || client.id, client.cardUrl);
  });

  state.queue.forEach((job) => {
    put(job.phone, job.client, job.contractNumber || job.clientId, job.cardUrl);
  });

  return index;
}

function resolveDraftDebtText(phone) {
  const client = findClientByPhone(phone);
  const fromClient = client
    ? String(client.debtApproxText || "").trim() || formatApproxDebtFromTotal(client.totalWithCommissionRaw ?? client.totalWithCommission)
    : "";
  if (fromClient) return fromClient;

  const queueItem = state.queue.find((q) => normalizePhone(q.phone) === normalizePhone(phone)) || null;
  const fromQueue = queueItem
    ? String(queueItem.debtApproxText || "").trim() || formatApproxDebtFromTotal(queueItem.totalWithCommissionRaw ?? queueItem.totalWithCommission)
    : "";
  if (fromQueue) return fromQueue;
  return "сумма уточняется";
}

function buildManualPresetDraft(dialog, presetId) {
  const preset = getManualPresetById(presetId);
  if (!preset || !dialog) return "";
  return applyManualVariables(dialog, preset.text);
}

function applyManualVariables(dialog, rawText) {
  if (!dialog) return String(rawText || "");
  const fio = resolveDialogFioByPhone(dialog.phone, dialog.fio);
  const phone = toApiPhone(dialog.phone);
  const amount = resolveDraftDebtText(dialog.phone);
  return String(rawText || "")
    .replaceAll("{полное_фио}", fio)
    .replaceAll("{сумма_долга}", amount)
    .replaceAll("{фио}", fio)
    .replaceAll("{имя}", firstNameFromFio(fio))
    .replaceAll("{телефон}", phone);
}

function overdueRange(days) {
  if (days >= 3 && days <= 15) return "3-15";
  if (days >= 16 && days <= 29) return "16-29";
  if (days >= 30 && days <= 45) return "30-45";
  return "46-59";
}

function normalizeTemplateKind(kind) {
  if (kind && TEMPLATE_TYPE_CONFIG[kind]) return kind;
  return DEFAULT_TEMPLATE_KIND;
}

function getTemplateType(kind) {
  return TEMPLATE_TYPE_CONFIG[normalizeTemplateKind(kind)];
}

function templateTypeLabel(kind) {
  return getTemplateType(kind).label;
}

function templateTypeRange(kind) {
  return getTemplateType(kind).rangeText;
}

function templateTypeRule(kind) {
  return getTemplateType(kind).ruleHint;
}

function templateKindSortOrder(kind) {
  const index = TEMPLATE_TYPE_ORDER.indexOf(normalizeTemplateKind(kind));
  return index >= 0 ? index : Number.MAX_SAFE_INTEGER;
}

function isTemplateEligibleForOverdue(kind, daysOverdue, options = {}) {
  const { allowManualOnly = false } = options;
  const cfg = getTemplateType(kind);
  if (!allowManualOnly && cfg.autoAssign === false) return false;
  return daysOverdue >= cfg.minOverdue && daysOverdue <= cfg.maxOverdue;
}

function cardUrlByClientId(clientId) {
  if (!clientId) return "";
  return `${ROCKETMAN_CARD_URL_PREFIX}${clientId}`;
}

function parseClientId(rawId) {
  const asNumber = Number(rawId);
  if (Number.isFinite(asNumber) && asNumber > 0) return asNumber;
  return String(rawId || "").trim();
}

function toMskDateTimeOrEmpty(utcIso) {
  const ms = safeDateMs(utcIso);
  if (!Number.isFinite(ms)) return "";
  return formatMskDateTime(ms);
}

function toMskTimeOrDash(utcIso) {
  const ms = safeDateMs(utcIso);
  if (!Number.isFinite(ms)) return "--:--";
  return formatMskHHMM(ms);
}

function hasAnyRunFilterSelected() {
  return (
    state.runFilters.tz.size > 0 ||
    state.runFilters.overdueRanges.size > 0 ||
    state.runFilters.exactDay !== null
  );
}

function getExpectedSnapshotMode() {
  return "live";
}

function hasExpectedSnapshotModeLoaded() {
  if (!state.clientsDb.actualized) return false;
  const currentMode = String(state.clientsDb.sourceMode || "").trim().toLowerCase();
  if (!currentMode) return false;
  return currentMode === getExpectedSnapshotMode();
}

function buildQueueFilterPayload() {
  return {
    snapshotId: state.clientsDb.snapshotId || null,
    timezoneOffsets: Array.from(state.runFilters.tz).sort((a, b) => a - b),
    overdueRanges: Array.from(state.runFilters.overdueRanges),
    exactDay: state.runFilters.exactDay
  };
}

function mapClientDtoToUi(item) {
  const externalClientId = String(item.externalClientId || "").trim();
  const clientId = parseClientId(externalClientId);
  const contractNumber = String(externalClientId || clientId || "").trim();
  return {
    id: clientId,
    externalClientId,
    client: item.fio || "",
    phone: item.phone || "",
    contractNumber,
    tzOffset: Number(item.timezoneOffset ?? 0),
    daysOverdue: Number(item.daysOverdue ?? 0),
    messageIndex: 1,
    cardUrl: item.cardUrl || cardUrlByClientId(clientId),
    totalWithCommissionRaw: item.totalWithCommissionRaw || "",
    debtApproxText: item.debtApproxText || "",
    debtApproxValue: item.debtApproxValue == null ? null : Number(item.debtApproxValue),
    debtStatus: item.debtStatus || "empty",
    debtSource: item.debtSource || "",
    debtUpdatedAtUtc: item.debtUpdatedAtUtc || "",
    debtErrorCode: item.debtErrorCode || "",
    debtErrorDetail: item.debtErrorDetail || ""
  };
}

function mapQueueJobDtoToUi(job) {
  const externalClientId = String(job.externalClientId || "").trim();
  const clientId = parseClientId(externalClientId);
  const contractNumber = String(externalClientId || clientId || "").trim();
  const plannedMs = safeDateMs(job.plannedAtUtc);
  return {
    id: Number(job.id),
    jobId: Number(job.id),
    runSessionId: Number(job.runSessionId || 0),
    clientId,
    externalClientId,
    contractNumber,
    client: job.clientFio || "",
    phone: job.phone || "",
    templateId: job.templateId == null ? null : Number(job.templateId),
    templateName: String(job.templateName || "").trim(),
    templateKind: String(job.templateKind || "").trim(),
    deliveryType: job.deliveryType || "sms",
    senderId: job.channelId == null ? 0 : Number(job.channelId),
    status: job.status || "queued",
    tzOffset: Number(job.tzOffset ?? 0),
    daysOverdue: Number(job.daysOverdue ?? 0),
    cardUrl: job.cardUrl || cardUrlByClientId(clientId),
    plannedAtUtc: Number.isFinite(plannedMs) ? new Date(plannedMs).toISOString() : "",
    attempts: Number(job.attempts ?? 0),
    maxAttempts: Number(job.maxAttempts ?? 3),
    messageIndex: 1,
    lastErrorCode: job.lastErrorCode || "",
    lastErrorDetail: job.lastErrorDetail || "",
    totalWithCommissionRaw: job.totalWithCommissionRaw || "",
    debtApproxText: job.debtApproxText || "",
    debtApproxValue: job.debtApproxValue == null ? null : Number(job.debtApproxValue),
    debtStatus: job.debtStatus || "empty",
    debtSource: job.debtSource || "",
    debtUpdatedAtUtc: job.debtUpdatedAtUtc || "",
    debtErrorCode: job.debtErrorCode || "",
    debtErrorDetail: job.debtErrorDetail || "",
    previewStatus: String(job.previewStatus || "empty").trim() || "empty",
    previewText: String(job.previewText || ""),
    previewVariablesJson: String(job.previewVariablesJson || ""),
    previewUpdatedAtUtc: String(job.previewUpdatedAtUtc || ""),
    previewErrorCode: String(job.previewErrorCode || ""),
    previewErrorDetail: String(job.previewErrorDetail || ""),
    hasMessageOverride: Boolean(job.hasMessageOverride),
    messageOverrideText: String(job.messageOverrideText || "")
  };
}

function mapChannelDtoToUi(item) {
  return {
    id: Number(item.id),
    name: item.name || "",
    endpoint: item.endpoint || "",
    token: item.tokenMasked || "",
    simPhone: item.simPhone || "",
    status: item.status || "unknown",
    checkedAt: toMskTimeOrDash(item.lastCheckedAtUtc),
    failStreak: Number(item.failStreak ?? 0),
    alerted: Boolean(item.alerted)
  };
}

function mapAlertDtoToUi(item) {
  const createdMs = safeDateMs(item.createdAtUtc);
  const closedMs = safeDateMs(item.closedAtUtc);
  return {
    id: Number(item.id),
    level: String(item.level || "error"),
    text: String(item.text || "").trim(),
    status: String(item.status || "active"),
    channelId: item.channelId == null ? null : Number(item.channelId),
    channelName: String(item.channelName || "").trim(),
    at: Number.isFinite(createdMs) ? formatMskHHMM(createdMs) : nowHHMM(),
    createdAtUtc: Number.isFinite(createdMs) ? new Date(createdMs).toISOString() : "",
    closedAt: Number.isFinite(closedMs) ? formatMskHHMM(closedMs) : null,
    closedAtUtc: Number.isFinite(closedMs) ? new Date(closedMs).toISOString() : null
  };
}

function mapTemplateDtoToUi(item) {
  return {
    id: Number(item.id),
    kind: normalizeTemplateKind(item.kind),
    status: String(item.status || "draft"),
    name: String(item.name || "").trim(),
    text: String(item.text || "").trim()
  };
}

function mapManualPresetDtoToUi(item) {
  return {
    id: String(item.id),
    title: String(item.title || "").trim(),
    text: String(item.text || "").trim()
  };
}

function mapStopListDtoToUi(item) {
  return {
    id: Number(item.id),
    phone: normalizePhone(item.phone || ""),
    reason: item.reason || "",
    added: toMskDateTimeOrEmpty(item.addedAtUtc),
    source: item.source || "manual"
  };
}

function mapDialogSummaryDtoToUi(item) {
  const phone = toApiPhone(item.phone || "");
  const activityMs = safeDateMs(item.lastMessageAtUtc);
  return {
    id: item.dialogId || phone,
    phone,
    fio: item.fio || phone || "Клиент",
    updatedAt: Number.isFinite(activityMs) ? new Date(activityMs).toISOString() : new Date(mskNowUtcMs()).toISOString(),
    totalMessages: Number(item.totalMessages ?? 0),
    lastText: String(item.lastText || "").trim(),
    lastOutgoingChannelId: Number(item.lastOutgoingChannelId ?? 0),
    lastOutgoingChannelName: String(item.lastOutgoingChannelName || "").trim(),
    messages: [],
    messagesLoaded: false,
    messagesLoading: false,
    manualDraftText: "",
    manualDraftLoaded: false,
    manualDraftLoading: false,
    manualDraftDirty: false,
    localOnly: false
  };
}

function mapDialogMessageDtoToUi(item) {
  const createdMs = safeDateMs(item.createdAtUtc);
  const createdAt = Number.isFinite(createdMs) ? createdMs : mskNowUtcMs();
  let type = "sys";
  if (item.direction === "in") type = "in";
  if (item.direction === "out") type = "out";
  return {
    id: Number(item.id || 0),
    type,
    text: item.text || "",
    at: formatMskHHMM(createdAt),
    createdAtUtc: Number.isFinite(createdMs) ? new Date(createdMs).toISOString() : new Date(createdAt).toISOString(),
    gatewayStatus: item.gatewayStatus || ""
  };
}

function dialogChannelLabel(dialog) {
  if (!dialog) return "Не определен";

  const named = String(dialog.lastOutgoingChannelName || "").trim();
  if (named) return named;

  const channelId = Number(dialog.lastOutgoingChannelId || 0);
  if (channelId > 0) {
    const sender = senderById(channelId);
    if (sender?.name) return sender.name;
    return `Канал #${channelId}`;
  }

  const queueRow = state.queue.find((q) => normalizePhone(q.phone) === normalizePhone(dialog.phone) && Number(q.senderId || 0) > 0);
  if (queueRow) {
    return senderNameById(queueRow.senderId);
  }

  return "Не определен";
}

function senderNameById(id) {
  const s = state.channels.find((x) => x.id === id);
  return s ? s.name : "Неизвестный канал";
}

function senderById(id) {
  return state.channels.find((x) => x.id === id) || null;
}

function channelStatusText(status) {
  return CHANNEL_STATUS_TEXT[status] || CHANNEL_STATUS_TEXT.unknown;
}

function isPhoneInStopList(phone) {
  const p = normalizePhone(phone);
  return state.stoplist.some((s) => normalizePhone(s.phone) === p);
}

function statusCell(status) {
  return `<span class="pill ${status}">${STATUS_TEXT[status] || status}</span>`;
}

function shouldPassRunFilter(job) {
  const hasAnyFilter =
    state.runFilters.tz.size > 0 ||
    state.runFilters.overdueRanges.size > 0 ||
    state.runFilters.exactDay !== null;
  if (!hasAnyFilter) return false;

  const tzFilterActive = state.runFilters.tz.size > 0;
  if (tzFilterActive && !state.runFilters.tz.has(job.tzOffset)) return false;

  if (state.runFilters.exactDay !== null) {
    return job.daysOverdue === state.runFilters.exactDay;
  }

  const overdueFilterActive = state.runFilters.overdueRanges.size > 0;
  if (!overdueFilterActive) return true;
  return state.runFilters.overdueRanges.has(overdueRange(job.daysOverdue));
}

function findDialogByPhone(phone) {
  const normalized = normalizePhone(phone);
  return state.dialogs.find((d) => normalizePhone(d.phone) === normalized) || null;
}

function dialogStatusByPhone(phone) {
  const dialog = findDialogByPhone(phone);
  if (!dialog) {
    return { text: "Нет истории", hasDialog: false, hasReply: false };
  }
  const hasHistory = Number(dialog.totalMessages || 0) > 0 || (Array.isArray(dialog.messages) && dialog.messages.length > 0);
  if (!hasHistory) {
    return { text: "Нет истории", hasDialog: false, hasReply: false };
  }
  return { text: "Есть история", hasDialog: true, hasReply: false };
}

function hasClientInQueue(clientId) {
  return state.queue.some((q) => q.clientId === clientId);
}

function upsertLocalDialogByPhone(phone, fio = "") {
  const apiPhone = toApiPhone(phone);
  if (!apiPhone) return null;
  const existing = findDialogByPhone(apiPhone);
  if (existing) {
    if ((!existing.fio || existing.fio === existing.phone) && fio) existing.fio = fio;
    existing.localOnly = Number(existing.totalMessages || 0) <= 0;
    if (typeof existing.manualDraftText !== "string") existing.manualDraftText = "";
    if (typeof existing.manualDraftLoaded !== "boolean") existing.manualDraftLoaded = false;
    if (typeof existing.manualDraftLoading !== "boolean") existing.manualDraftLoading = false;
    if (typeof existing.manualDraftDirty !== "boolean") existing.manualDraftDirty = false;
    return existing;
  }

  const created = {
    id: apiPhone,
    phone: apiPhone,
    fio: resolveDialogFioByPhone(apiPhone, fio),
    updatedAt: new Date(mskNowUtcMs()).toISOString(),
    totalMessages: 0,
    lastOutgoingChannelId: 0,
    lastOutgoingChannelName: "",
    messages: [],
    messagesLoaded: false,
    messagesLoading: false,
    manualDraftText: "",
    manualDraftLoaded: false,
    manualDraftLoading: false,
    manualDraftDirty: false,
    localOnly: true
  };
  state.dialogs.unshift(created);
  return created;
}

async function refreshDialogMessagesFromBackend(phone, options = {}) {
  const { silent = true } = options;
  const apiPhone = toApiPhone(phone);
  if (!apiPhone) return false;

  const dialog = upsertLocalDialogByPhone(apiPhone);
  if (!dialog) return false;
  if (dialog.messagesLoading) return true;

  dialog.messagesLoading = true;
  renderChat();
  try {
    const data = await fetchApiJson(`/api/dialogs/by-phone/${encodeURIComponent(apiPhone)}/messages?limit=5000&offset=0`);
    const rows = Array.isArray(data?.items) ? data.items.map(mapDialogMessageDtoToUi) : [];
    dialog.messages = rows;
    dialog.totalMessages = Number(data?.totalMessages ?? rows.length);
    dialog.messagesLoaded = true;
    dialog.localOnly = dialog.totalMessages <= 0;
    if (rows.length > 0) {
      const last = rows[rows.length - 1];
      dialog.updatedAt = last.createdAtUtc || dialog.updatedAt;
    }

    renderDialogs();
    renderChat();
    renderQueue();
    renderClientsDb();
    syncDialogPreviewWithQueue();
    renderDialogPreviewPanel();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить сообщения диалога: ${error?.message || "ошибка backend"}`);
    }
    return false;
  } finally {
    dialog.messagesLoading = false;
    renderChat();
  }
}

function clearDraftSaveTimer(phone) {
  const normalized = normalizePhone(phone);
  const current = state.draftSaveTimersByPhone[normalized];
  if (!current) return;
  clearTimeout(current);
  delete state.draftSaveTimersByPhone[normalized];
}

function scheduleDialogDraftSave(dialog) {
  if (!dialog || !dialog.phone) return;
  const normalized = normalizePhone(dialog.phone);
  clearDraftSaveTimer(normalized);
  state.draftSaveTimersByPhone[normalized] = setTimeout(() => {
    delete state.draftSaveTimersByPhone[normalized];
    void persistDialogDraft(dialog, dialog.manualDraftText, { silent: true });
  }, 450);
}

async function persistDialogDraft(dialog, text, options = {}) {
  const { silent = true } = options;
  if (!dialog || !dialog.phone) return false;
  const apiPhone = toApiPhone(dialog.phone);
  if (!apiPhone) return false;
  const normalized = normalizePhone(apiPhone);
  const draftText = String(text || "");

  try {
    if (!draftText.trim()) {
      await fetchApiJson(`/api/dialogs/by-phone/${encodeURIComponent(apiPhone)}/draft`, { method: "DELETE" });
      const target = findDialogByPhone(normalized);
      if (target) {
        target.manualDraftText = "";
        target.manualDraftLoaded = true;
        target.manualDraftDirty = false;
      }
      return true;
    }

    const saved = await fetchApiJson(`/api/dialogs/by-phone/${encodeURIComponent(apiPhone)}/draft`, {
      method: "PUT",
      body: JSON.stringify({ text: draftText })
    });
    const target = findDialogByPhone(normalized);
    if (target) {
      target.manualDraftText = String(saved?.text || draftText);
      target.manualDraftLoaded = true;
      if (target.manualDraftText === draftText) {
        target.manualDraftDirty = false;
      }
    }
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось сохранить черновик: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshDialogDraftFromBackend(phone, options = {}) {
  const { silent = true, applyToInput = true } = options;
  const apiPhone = toApiPhone(phone);
  if (!apiPhone) return false;
  const dialog = upsertLocalDialogByPhone(apiPhone);
  if (!dialog) return false;
  if (dialog.manualDraftLoading) return true;

  dialog.manualDraftLoading = true;
  try {
    const data = await fetchApiJson(`/api/dialogs/by-phone/${encodeURIComponent(apiPhone)}/draft`);
    const serverText = String(data?.text || "");
    const serverExists = Boolean(data?.exists);
    if (!dialog.manualDraftDirty) {
      dialog.manualDraftText = serverText;
      dialog.manualDraftDirty = false;
    }
    dialog.manualDraftLoaded = serverExists;

    const selectedDialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
    if (applyToInput &&
        !dialog.manualDraftDirty &&
        selectedDialog &&
        normalizePhone(selectedDialog.phone) === normalizePhone(apiPhone)) {
      const input = $("manualInput");
      if (input) {
        input.value = dialog.manualDraftText;
      }
    }

    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить черновик: ${error?.message || "ошибка backend"}`);
    }
    return false;
  } finally {
    dialog.manualDraftLoading = false;
  }
}

async function refreshDialogsFromBackend(options = {}) {
  const { silent = true, ensurePhone = "" } = options;
  const previousSelectedId = state.selectedDialogId;
  const previousDialogsByPhone = new Map(
    state.dialogs.map((dialog) => [normalizePhone(dialog.phone), dialog])
  );

  try {
    const data = await fetchApiJson("/api/dialogs?limit=5000&offset=0");
    const apiDialogs = (Array.isArray(data?.items) ? data.items : []).map((item) => {
      const mapped = mapDialogSummaryDtoToUi(item);
      const previous = previousDialogsByPhone.get(normalizePhone(mapped.phone));

      if (!previous) {
        return mapped;
      }

      const previousMessages = Array.isArray(previous.messages) ? previous.messages : [];
      const previousCount = Math.max(Number(previous.totalMessages || 0), previousMessages.length);
      const mappedCount = Number(mapped.totalMessages || 0);
      const hasNewMessages = mappedCount > previousCount;

      const keepLoadedMessages = previous.messagesLoaded && !hasNewMessages;
      return {
        ...mapped,
        messages: [...previousMessages],
        messagesLoaded: keepLoadedMessages,
        messagesLoading: false,
        manualDraftText: String(previous.manualDraftText || ""),
        manualDraftLoaded: Boolean(previous.manualDraftLoaded),
        manualDraftLoading: false,
        manualDraftDirty: Boolean(previous.manualDraftDirty),
        totalMessages: Math.max(mappedCount, previousCount),
        localOnly: false
      };
    });
    const ensureNormalized = normalizePhone(ensurePhone);

    previousDialogsByPhone.forEach((previous, normalizedPhone) => {
      if (!previous || !previous.localOnly) return;
      if (apiDialogs.some((dialog) => normalizePhone(dialog.phone) === normalizedPhone)) return;
      apiDialogs.unshift({
        ...previous,
        messages: Array.isArray(previous.messages) ? [...previous.messages] : [],
        messagesLoaded: Boolean(previous.messagesLoaded),
        messagesLoading: false,
        manualDraftText: String(previous.manualDraftText || ""),
        manualDraftLoaded: Boolean(previous.manualDraftLoaded),
        manualDraftLoading: false,
        manualDraftDirty: Boolean(previous.manualDraftDirty),
        localOnly: true
      });
    });

    if (ensureNormalized && !apiDialogs.some((dialog) => normalizePhone(dialog.phone) === ensureNormalized)) {
      const fromPrevious = previousDialogsByPhone.get(ensureNormalized);
      if (fromPrevious) {
        apiDialogs.unshift(fromPrevious);
      } else {
        const ensuredPhone = toApiPhone(ensurePhone);
        apiDialogs.unshift({
          id: ensuredPhone,
          phone: ensuredPhone,
          fio: resolveDialogFioByPhone(ensurePhone),
          updatedAt: new Date(mskNowUtcMs()).toISOString(),
          totalMessages: 0,
          lastOutgoingChannelId: 0,
          lastOutgoingChannelName: "",
          messages: [],
          messagesLoaded: false,
          messagesLoading: false,
          manualDraftText: "",
          manualDraftLoaded: false,
          manualDraftLoading: false,
          manualDraftDirty: false,
          localOnly: true
        });
      }
    }

    state.dialogs = apiDialogs;

    if (previousSelectedId !== null && state.dialogs.some((dialog) => String(dialog.id) === String(previousSelectedId))) {
      state.selectedDialogId = String(previousSelectedId);
    } else if (ensureNormalized) {
      const ensured = state.dialogs.find((dialog) => normalizePhone(dialog.phone) === ensureNormalized);
      state.selectedDialogId = ensured ? String(ensured.id) : (state.dialogs[0]?.id ?? null);
    } else {
      state.selectedDialogId = state.dialogs[0]?.id ?? null;
    }

    renderDialogs();
    renderChat();
    renderQueue();
    renderClientsDb();
    syncDialogPreviewWithQueue();
    renderDialogPreviewPanel();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить диалоги: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function rebuildPlannedQueue(showToast = true) {
  if (state.runRuntime) {
    if (showToast) toast("Нельзя пересчитать план во время запущенного цикла");
    return false;
  }
  if (!state.clientsDb.actualized) {
    if (showToast) toast("Сначала выполните «Актуализировать базу клиентов»");
    return false;
  }
  if (!hasAnyRunFilterSelected()) {
    if (showToast) toast("Выберите хотя бы один фильтр перед формированием очереди");
    return false;
  }
  if (!hasExpectedSnapshotModeLoaded()) {
    if (showToast) toast("Для LIVE-режима сначала выполните актуализацию из Rocketman");
    return false;
  }

  try {
    const payload = buildQueueFilterPayload();
    const result = await fetchApiJson("/api/queue/build", {
      method: "POST",
      body: JSON.stringify(payload)
    });

    state.queueSessionId = Number(result?.runSessionId || 0);
    state.planPrepared = true;
    state.planStale = false;
    state.selectedQueue.clear();
    state.runForecast = result?.forecast || null;
    await refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId });
    await refreshRunStatusFromBackend({ silent: true, runSessionId: state.queueSessionId });
    renderClientsDb();
    renderRunForecast();
    renderDbSyncState();
    if (showToast) toast(`Плановая очередь сформирована: ${result?.createdJobs ?? state.queue.length} клиентов`);
    addRunLog(`Сформирована плановая очередь: ${result?.createdJobs ?? state.queue.length} клиентов.`);
    return true;
  } catch (error) {
    if (showToast) {
      toast(`Не удалось сформировать плановую очередь: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function rebuildQueueJobPreview(jobId, options = {}) {
  const { silent = true, skipRenderQueue = false } = options;
  const normalizedId = Number(jobId || 0);
  if (!normalizedId) {
    if (!silent) toast("Некорректный идентификатор задачи очереди");
    return null;
  }

  try {
    const data = await fetchApiJson(`/api/queue/${encodeURIComponent(normalizedId)}/preview/rebuild`, {
      method: "POST",
      body: JSON.stringify({ persist: true })
    });

    const queueRow = state.queue.find((x) => Number(x.id) === normalizedId);
    if (queueRow) {
      queueRow.previewStatus = String(data?.status || "empty");
      queueRow.previewText = String(data?.text || "");
      queueRow.previewVariablesJson = String(data?.variablesJson || "");
      queueRow.previewUpdatedAtUtc = String(data?.updatedAtUtc || "");
      queueRow.previewErrorCode = String(data?.errorCode || "");
      queueRow.previewErrorDetail = String(data?.errorDetail || "");
      if (data?.templateId != null) {
        queueRow.templateId = Number(data.templateId);
      }
      if (data?.templateName) {
        queueRow.templateName = String(data.templateName).trim();
      }
      if (data?.templateKind) {
        queueRow.templateKind = String(data.templateKind).trim();
      }
    }

    if (!skipRenderQueue) {
      renderQueue();
    }
    return data || null;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось обновить превью SMS: ${error?.message || "ошибка backend"}`);
    }
    return null;
  }
}

async function openDialogByPhone(phone) {
  const apiPhone = toApiPhone(phone);
  if (!apiPhone) {
    toast("Некорректный номер телефона для открытия диалога");
    return;
  }
  const switched = await switchTab("dialogs", { actionLabel: "перейти к диалогу" });
  if (!switched) return;

  const ensured = upsertLocalDialogByPhone(apiPhone, resolveDialogFioByPhone(apiPhone));
  state.selectedDialogId = ensured ? String(ensured.id) : apiPhone;
  state.dialogForceScrollPhone = normalizePhone(apiPhone);
  renderDialogs();
  renderChat();

  await refreshDialogMessagesFromBackend(apiPhone, { silent: true });
  await refreshDialogsFromBackend({ silent: true, ensurePhone: apiPhone });
  await refreshDialogDraftFromBackend(apiPhone, { silent: true, applyToInput: true });
  if (state.dialogPreview.enabled) {
    await rebuildDialogPreviewForSelected({ silent: true });
  } else {
    syncDialogPreviewWithQueue();
    renderDialogPreviewPanel();
  }
}

function shouldPassQueueFilter(job) {
  const search = (($("queueSearch")?.value) || "").trim().toLowerCase();
  const status = $("queueFilterStatus").value;
  const tz = $("queueFilterTz").value;
  const overdue = $("queueFilterOverdue").value;
  if (search) {
    const searchPhone = normalizePhone(search);
    const inName = String(job.client || "").toLowerCase().includes(search);
    const inPhone = searchPhone ? normalizePhone(job.phone).includes(searchPhone) : false;
    const inContract = String(job.contractNumber || job.clientId || "").toLowerCase().includes(search);
    if (!inName && !inPhone && !inContract) return false;
  }
  if (status !== "all" && job.status !== status) return false;
  if (tz !== "all" && job.tzOffset !== Number(tz)) return false;
  if (overdue !== "all" && overdueRange(job.daysOverdue) !== overdue) return false;
  return true;
}

function syncSelectionSets() {
  const validClientIds = new Set(state.clients.map((c) => c.id));
  state.selectedClients.forEach((id) => {
    if (!validClientIds.has(id)) state.selectedClients.delete(id);
  });

  const validQueueIds = new Set(state.queue.map((q) => q.id));
  state.selectedQueue.forEach((id) => {
    if (!validQueueIds.has(id)) state.selectedQueue.delete(id);
  });

  const validStopIds = new Set(state.stoplist.map((s) => s.id));
  state.selectedStopList.forEach((id) => {
    if (!validStopIds.has(id)) state.selectedStopList.delete(id);
  });
}

function getVisibleClientRows() {
  return state.clients.filter(shouldPassClientsViewFilter);
}

function getVisibleQueueRows() {
  return state.queue.filter((q) => shouldPassQueueFilter(q));
}

function getVisibleStopRows() {
  const search = (($("stopSearch")?.value) || "").trim().toLowerCase();
  const searchPhone = normalizePhone(search);
  const clientMetaByPhone = buildClientMetaIndexByPhone();

  return state.stoplist
    .map((entry) => {
      const normalizedPhone = normalizePhone(entry.phone);
      const meta = clientMetaByPhone.get(normalizedPhone) || null;
      return { ...entry, normalizedPhone, meta };
    })
    .filter((entry) => {
      if (!search) return true;
      const inPhone = searchPhone ? entry.normalizedPhone.includes(searchPhone) : false;
      const inFio = String(entry.meta?.fio || "").toLowerCase().includes(search);
      const inContract = String(entry.meta?.contractNumber || "").toLowerCase().includes(search);
      return inPhone || inFio || inContract;
    });
}

function getSelectedClientRows() {
  return state.clients.filter((c) => state.selectedClients.has(c.id));
}

function getSelectedQueueRows() {
  return state.queue.filter((q) => state.selectedQueue.has(q.id));
}

function getSelectedStopRows() {
  return state.stoplist.filter((s) => state.selectedStopList.has(s.id));
}

function renderClientsBulkUi(visibleRows = getVisibleClientRows()) {
  const visibleIds = visibleRows.map((c) => c.id);
  const hasAnyRows = state.clients.length > 0;
  const selectedRows = getSelectedClientRows();
  const selectedTotal = selectedRows.length;
  const selectedVisible = visibleIds.filter((id) => state.selectedClients.has(id)).length;
  const returnableCount = selectedRows.filter((c) => state.excludedClientIds.has(c.id)).length;
  const selectedInStopList = selectedRows.filter((c) => isPhoneInStopList(c.phone)).length;
  const selectedOutStopList = selectedTotal - selectedInStopList;
  const canBulkAddToStopList = selectedTotal > 0 && selectedOutStopList === selectedTotal;
  const canBulkRemoveFromStopList = selectedTotal > 0 && selectedInStopList === selectedTotal;

  $("clientsBulkMeta").textContent = `Выбрано: ${selectedTotal} (видимо: ${selectedVisible})`;
  $("clientsSelectVisible").disabled = !hasAnyRows;
  $("clientsClearSelection").disabled = selectedTotal === 0;
  $("clientsBulkAddStop").disabled = !canBulkAddToStopList;
  $("clientsBulkRemoveStop").disabled = !canBulkRemoveFromStopList;
  $("clientsBulkReturnPlan").disabled = selectedTotal === 0 || Boolean(state.runRuntime) || returnableCount === 0;

  const selectAll = $("clientsSelectAllRows");
  selectAll.disabled = visibleIds.length === 0;
  selectAll.checked = visibleIds.length > 0 && selectedVisible === visibleIds.length;
  selectAll.indeterminate = selectedVisible > 0 && selectedVisible < visibleIds.length;
}

function renderQueueBulkUi(visibleRows = getVisibleQueueRows()) {
  const visibleIds = visibleRows.map((q) => q.id);
  const hasAnyRows = state.queue.length > 0;
  const selectedRows = getSelectedQueueRows();
  const selectedTotal = selectedRows.length;
  const selectedVisible = visibleIds.filter((id) => state.selectedQueue.has(id)).length;
  const removableFromPlan = selectedRows.filter((q) => ["queued", "retry"].includes(q.status)).length;

  $("queueBulkMeta").textContent = `Выбрано: ${selectedTotal} (видимо: ${selectedVisible})`;
  $("queueSelectVisible").disabled = !hasAnyRows;
  $("queueClearSelection").disabled = selectedTotal === 0;
  $("queueBulkAddStop").disabled = selectedTotal === 0;
  $("queueBulkRemoveStop").disabled = selectedTotal === 0;
  $("queueBulkRemovePlan").disabled = selectedTotal === 0 || Boolean(state.runRuntime) || removableFromPlan === 0;
  $("queueBulkSetTemplate").disabled = selectedTotal === 0 || Boolean(state.runRuntime);

  const templateSelect = $("queueBulkTemplate");
  const activeTemplates = state.templates
    .filter((t) => t.status === "active")
    .sort((a, b) => {
      const byKind = templateKindSortOrder(a.kind) - templateKindSortOrder(b.kind);
      if (byKind !== 0) return byKind;
      return String(a.name || "").localeCompare(String(b.name || ""), "ru");
    });
  if (activeTemplates.length === 0) {
    templateSelect.innerHTML = '<option value="">Нет активных шаблонов</option>';
    templateSelect.disabled = true;
    $("queueBulkSetTemplate").disabled = true;
  } else {
    const current = templateSelect.value;
    templateSelect.innerHTML = activeTemplates.map((tpl) => `
      <option value="${tpl.id}">${templateTypeLabel(tpl.kind)}: ${tpl.name}</option>
    `).join("");
    if (current && activeTemplates.some((tpl) => String(tpl.id) === current)) {
      templateSelect.value = current;
    }
    templateSelect.disabled = false;
  }

  const selectAll = $("queueSelectAllRows");
  selectAll.disabled = visibleIds.length === 0;
  selectAll.checked = visibleIds.length > 0 && selectedVisible === visibleIds.length;
  selectAll.indeterminate = selectedVisible > 0 && selectedVisible < visibleIds.length;
}

function renderQueueRetryActionState() {
  const retryBtn = $("queueRetryErrors");
  const hint = $("queueRetryHint");
  if (!retryBtn) return;

  const failedCount = state.queue.filter((q) => q.status === "failed").length;
  const stoppedCount = state.queue.filter((q) => q.status === "stopped").length;
  const totalRetryable = failedCount + stoppedCount;

  retryBtn.disabled = totalRetryable === 0;
  if (hint) {
    hint.textContent = totalRetryable > 0
      ? `Переотправляются задачи со статусами «Ошибка» и «Остановлено». Сейчас: ошибка ${failedCount}, остановлено ${stoppedCount}.`
      : "Переотправляются задачи со статусами «Ошибка» и «Остановлено». Сейчас задач для переотправки нет.";
  }
}

function renderStopBulkUi(visibleRows = getVisibleStopRows()) {
  const visibleIds = visibleRows.map((s) => s.id);
  const hasAnyRows = state.stoplist.length > 0;
  const selectedRows = getSelectedStopRows();
  const selectedTotal = selectedRows.length;
  const selectedVisible = visibleIds.filter((id) => state.selectedStopList.has(id)).length;

  $("stopBulkMeta").textContent = `Выбрано: ${selectedTotal} (видимо: ${selectedVisible})`;
  $("stopSelectVisible").disabled = !hasAnyRows;
  $("stopClearSelection").disabled = selectedTotal === 0;
  $("stopBulkRemove").disabled = selectedTotal === 0;

  const selectAll = $("stopSelectAllRows");
  selectAll.disabled = visibleIds.length === 0;
  selectAll.checked = visibleIds.length > 0 && selectedVisible === visibleIds.length;
  selectAll.indeterminate = selectedVisible > 0 && selectedVisible < visibleIds.length;
}

function collectRunFiltersFromUI() {
  state.runFilters.tz = new Set(
    Array.from(document.querySelectorAll(".run-tz:checked")).map((n) => Number(n.value))
  );
  state.runFilters.overdueRanges = new Set(
    Array.from(document.querySelectorAll(".run-overdue:checked")).map((n) => n.value)
  );
  const rawDay = $("runExactDay").value.trim();
  const exactDayValue = rawDay ? Number(rawDay) : null;
  state.runFilters.exactDay = Number.isFinite(exactDayValue) ? exactDayValue : null;
}

function renderDbSyncState() {
  const syncBtn = $("runSyncDb");
  const startBtn = $("runStart");
  const stopBtn = $("runStop");
  const globalStartBtn = $("globalStart");
  const globalStopBtn = $("globalStop");
  const statusEl = $("dbSyncStatus");
  const expectedSourceMode = getExpectedSnapshotMode();
  const sourceModeMismatch =
    state.clientsDb.actualized &&
    Boolean(state.clientsDb.sourceMode) &&
    state.clientsDb.sourceMode !== expectedSourceMode;

  if (syncBtn) {
    syncBtn.textContent = "Актуализировать базу клиентов";
  }

  if (syncBtn) syncBtn.disabled = state.clientsDb.syncing || Boolean(state.runRuntime);
  if (stopBtn) stopBtn.disabled = !state.runRuntime;
  if (globalStopBtn) globalStopBtn.disabled = !state.runRuntime;
  const startBlocked =
    !state.clientsDb.actualized ||
    state.clientsDb.syncing ||
    !state.planPrepared ||
    state.planStale ||
    state.queue.length === 0 ||
    !state.runCanStart ||
    sourceModeMismatch;
  if (startBtn) startBtn.disabled = startBlocked;
  if (globalStartBtn) globalStartBtn.disabled = startBlocked;

  if (!statusEl) return;
  if (state.clientsDb.syncing) {
    setNotice("dbSyncStatus", "Идет актуализация базы клиентов...", "info");
    return;
  }
  if (!state.clientsDb.actualized) {
    setNotice("dbSyncStatus", "Перед запуском нажмите «Актуализировать базу клиентов».", "warning");
    return;
  }
  if (sourceModeMismatch) {
    setNotice(
      "dbSyncStatus",
      "Сейчас загружена TEST-база. Для боевого запуска выполните актуализацию из Rocketman.",
      "warning");
    return;
  }
  if (!state.planPrepared) {
    const modeLabel = state.clientsDb.sourceMode ? state.clientsDb.sourceMode.toUpperCase() : "N/A";
    setNotice("dbSyncStatus", `База актуальна (${modeLabel}): ${state.clientsDb.rows} клиентов, обновлено ${state.clientsDb.syncedAt}. Следующий шаг: «Сформировать плановую очередь».`, "warning");
    return;
  }
  if (state.planStale) {
    setNotice("dbSyncStatus", "Фильтры изменены после формирования плана. Обновите плановую очередь перед запуском.", "warning");
    return;
  }
  if (!state.runCanStart && state.runStartBlockMessage) {
    setNotice("dbSyncStatus", state.runStartBlockMessage, "warning");
    return;
  }
  const modeLabel = state.clientsDb.sourceMode ? state.clientsDb.sourceMode.toUpperCase() : "N/A";
  setNotice("dbSyncStatus", `Плановая очередь готова: ${state.queue.length} клиентов. База (${modeLabel}) обновлена ${state.clientsDb.syncedAt}.`, "success");
}

function renderRunFilterSummary() {
  const hasAnyFilter =
    state.runFilters.tz.size > 0 ||
    state.runFilters.overdueRanges.size > 0 ||
    state.runFilters.exactDay !== null;
  if (!hasAnyFilter) {
    setNotice("runFilterSummary", "Фильтр не выбран. Выберите хотя бы один часовой пояс или просрочку.", "warning");
    return;
  }

  const tz = Array.from(state.runFilters.tz)
    .sort((a, b) => a - b)
    .map((x) => MSK_TZ_LABEL[String(x)])
    .join(", ");
  const ranges = Array.from(state.runFilters.overdueRanges).join(", ");
  const tzText = state.runFilters.tz.size === 0 ? "все" : tz;
  const rangesText = state.runFilters.overdueRanges.size === 0 ? "все" : ranges;
  const exact = state.runFilters.exactDay;
  const text = exact !== null
    ? `Фильтр: пояса [${tzText}], точная просрочка ${exact} дней`
    : `Фильтр: пояса [${tzText}], диапазоны просрочки [${rangesText}]`;
  setNotice("runFilterSummary", text, "info");
}

function refreshRunFiltersUI() {
  collectRunFiltersFromUI();
  if (state.clientsDb.actualized && state.planPrepared && !state.runRuntime) {
    state.planStale = true;
  }
  renderRunFilterSummary();
  renderClientsDb();
  void refreshRunForecastFromBackend({ silent: true });
  renderDbSyncState();
}

function setFilterCheckboxes(selector, checked) {
  document.querySelectorAll(selector).forEach((n) => {
    n.checked = checked;
  });
  refreshRunFiltersUI();
}

function syncChannelAlertFlags() {
  state.channels.forEach((channel) => {
    channel.alerted = state.alerts.some((a) => a.status === "active" && a.channelId === channel.id);
  });
}

async function updateAlertStatus(alertId, status) {
  const id = Number(alertId || 0);
  if (!id) return false;
  try {
    await fetchApiJson(`/api/alerts/${encodeURIComponent(id)}/status`, {
      method: "PATCH",
      body: JSON.stringify({ status })
    });
    await refreshAlertsFromBackend({ silent: true });
    return true;
  } catch (error) {
    toast(`Не удалось обновить статус уведомления: ${error?.message || "ошибка backend"}`);
    return false;
  }
}

function renderRunForecast() {
  if (!state.clientsDb.actualized || state.clientsDb.syncing) {
    state.runForecast = null;
    $("planClients").textContent = "0";
    $("planGap").textContent = String(getGapMinutes());
    $("planTzWait").textContent = "--";
    $("planGapWait").textContent = "--";
    $("planTotal").textContent = "--";
    $("planFinish").textContent = "--:--";
    setNotice(
      "planHint",
      state.clientsDb.syncing
        ? "Идет актуализация базы клиентов. После завершения будет рассчитан прогноз."
        : "Сначала выполните «Актуализировать базу клиентов», затем прогноз станет доступен.",
      state.clientsDb.syncing ? "info" : "warning"
    );
    return;
  }

  if (!hasExpectedSnapshotModeLoaded()) {
    state.runForecast = null;
    $("planClients").textContent = "0";
    $("planGap").textContent = String(getGapMinutes());
    $("planTzWait").textContent = "--";
    $("planGapWait").textContent = "--";
    $("planTotal").textContent = "--";
    $("planFinish").textContent = "--:--";
    setNotice(
      "planHint",
      "Включен LIVE-режим. Сначала выполните актуализацию из Rocketman (sourceMode=live).",
      "warning"
    );
    return;
  }

  if (!state.runRuntime && !hasAnyRunFilterSelected()) {
    state.runForecast = null;
    $("planClients").textContent = "0";
    $("planGap").textContent = String(getGapMinutes());
    $("planTzWait").textContent = "--";
    $("planGapWait").textContent = "--";
    $("planTotal").textContent = "--";
    $("planFinish").textContent = "--:--";
    setNotice("planHint", "Прогноз сброшен: фильтры не выбраны. Выберите хотя бы один фильтр.", "warning");
    return;
  }

  if (!state.runForecast) {
    $("planClients").textContent = "0";
    $("planGap").textContent = String(getGapMinutes());
    $("planTzWait").textContent = "--";
    $("planGapWait").textContent = "--";
    $("planTotal").textContent = "--";
    $("planFinish").textContent = "--:--";
    setNotice("planHint", "Прогноз не рассчитан. Проверьте фильтры и нажмите «Сформировать плановую очередь».", "warning");
    return;
  }

  const forecast = state.runForecast;
  const preview = forecast.preview || {};
  const jobsCount = Number(preview.readyRows || 0);
  const gapMinutes = Number(forecast.gapMinutes || getGapMinutes());
  const tzWaitMinutes = Number(forecast.timezoneWaitMinutes || 0);
  const gapWaitMinutes = Number(forecast.gapWaitMinutes || 0);
  const totalWaitMinutes = Number(forecast.totalWaitMinutes || 0);
  const onlineChannelsCount = Math.max(0, Number(forecast.onlineChannelsCount || 0));
  const channelsUsed = Math.max(1, Number(forecast.channelsUsed || 1));
  const finishAtUtc = forecast.estimatedFinishAtUtc || "";

  $("planClients").textContent = String(jobsCount);
  $("planGap").textContent = String(gapMinutes);
  $("planTzWait").textContent = formatDurationMinutes(tzWaitMinutes);
  $("planGapWait").textContent = formatDurationMinutes(gapWaitMinutes);
  $("planTotal").textContent = formatDurationMinutes(totalWaitMinutes);
  $("planFinish").textContent = jobsCount > 0 ? toMskDateTimeOrEmpty(finishAtUtc) : "--:--";

  if (jobsCount === 0) {
    setNotice("planHint", "По текущему фильтру нет клиентов для плановой очереди. Измените фильтр.", "warning");
    return;
  }

  if (!state.planPrepared && !state.runRuntime) {
    setNotice("planHint", "Предварительный прогноз рассчитан по текущим фильтрам. Нажмите «Сформировать плановую очередь» для фиксации запуска.", "info");
    return;
  }

  if (state.planStale && !state.runRuntime) {
    setNotice("planHint", "Прогноз уже пересчитан по новым фильтрам. Нажмите «Сформировать плановую очередь», чтобы обновить состав запуска.", "warning");
    return;
  }

  const workWindowLabel = getWorkWindowRange().label;
  setNotice(
    "planHint",
    `Онлайн-каналов: ${onlineChannelsCount}, в расчете использовано: ${channelsUsed}. Рабочее окно ${workWindowLabel} учитывается для всех исходящих сообщений по локальному времени клиента.`,
    "info"
  );
}

function refreshPlanningViews() {
  renderQueue();
  renderClientsDb();
  updateMetrics();
  void refreshRunForecastFromBackend({ silent: true });
  renderDbSyncState();
}

function updateMetrics() {
  const sent = state.queue.filter((q) => q.status === "sent").length;
  const failed = state.queue.filter((q) => q.status === "failed").length;
  const reportSent = Number.isFinite(Number(state.reportSentToday)) ? Number(state.reportSentToday) : sent;
  const reportFailed = Number.isFinite(Number(state.reportFailedToday)) ? Number(state.reportFailedToday) : failed;
  $("mClients").textContent = String(state.clients.length);
  $("mQueue").textContent = String(state.queue.length);
  $("mSent").textContent = String(sent);
  $("mErrors").textContent = String(failed);
  $("rSentToday").textContent = String(reportSent);
  $("rErrorsToday").textContent = String(reportFailed);
  $("rStopCount").textContent = String(state.stoplist.length);
}

function addRunLog(line, atUtcMs = null) {
  const log = $("runLog");
  const stamp = atUtcMs === null ? nowHHMM() : formatMskHHMM(atUtcMs);
  log.textContent += `[${stamp}] ${line}\n`;
  log.scrollTop = log.scrollHeight;
}

function renderAlerts() {
  const activeCount = state.alerts.filter((a) => a.status === "active").length;
  $("alertSummary").textContent = `Актуальных: ${activeCount} / Всего: ${state.alerts.length}`;
  if ($("alertViewFilter")) {
    $("alertViewFilter").value = state.alertView;
  }
  const list = $("alertList");
  if (state.alerts.length === 0) {
    list.innerHTML = '<p class="muted-note">Уведомлений нет</p>';
    return;
  }
  const filtered = state.alertView === "all"
    ? state.alerts
    : state.alerts.filter((a) => a.status === state.alertView);

  if (filtered.length === 0) {
    list.innerHTML = '<p class="muted-note">По выбранному статусу уведомлений нет</p>';
    return;
  }

  list.innerHTML = filtered.map((a) => `
    <div class="alert-item ${a.status}">
      <div class="alert-head">
        <strong>${a.level === "error" ? "Ошибка" : "Инфо"} · ${a.at}</strong>
        <span class="alert-status ${a.status}">${ALERT_STATUS_TEXT[a.status]}</span>
      </div>
      <p>${a.text}</p>
      ${a.closedAt ? `<small class="muted-note">Статус обновлен: ${a.closedAt}</small>` : ""}
      <div class="actions alert-actions">
        ${a.status === "active" ? `
          <button class="ghost-btn" data-action="alert-resolve" data-alert-id="${a.id}">Проблема решена</button>
          <button class="ghost-btn" data-action="alert-irrelevant" data-alert-id="${a.id}">Неактуально</button>
        ` : `
          <button class="ghost-btn" data-action="alert-activate" data-alert-id="${a.id}">Вернуть в актуальные</button>
        `}
      </div>
    </div>
  `).join("");
}

function renderChannels() {
  $("channelsBody").innerHTML = state.channels.map((c) => `
    <tr class="${c.status === "error" ? "channel-row-error" : ""}">
      <td>${c.name}</td>
      <td><code>${c.endpoint}</code></td>
      <td><code>${c.token || "****"}</code></td>
      <td>${c.simPhone || "-"}</td>
      <td><span class="channel-pill ${c.status}">${channelStatusText(c.status)}</span></td>
      <td>${c.checkedAt}</td>
      <td>
        <div class="actions slim">
          <button class="ghost-btn" data-action="channel-edit" data-ch-id="${c.id}">Редактировать</button>
          <button class="ghost-btn" data-action="channel-toggle" data-ch-id="${c.id}" data-next-status="${c.status === "offline" ? "unknown" : "offline"}">${c.status === "offline" ? "Включить" : "Отключить"}</button>
          <button class="ghost-btn" data-action="channel-check" data-ch-id="${c.id}">Проверить канал</button>
          <button class="ghost-btn" data-action="channel-delete" data-ch-id="${c.id}">Удалить</button>
        </div>
      </td>
    </tr>
  `).join("");
}

function renderTemplates() {
  const selectedId = state.templateCreateMode ? null : state.selectedTemplateId;
  $("templatesBody").innerHTML = state.templates.map((t) => `
    <tr class="${selectedId === t.id ? "active-row" : ""}">
      <td>${t.name}</td>
      <td>${templateTypeLabel(t.kind)}</td>
      <td>${t.status === "active" ? "Актуальный" : "Черновик"}</td>
      <td>${templateTypeRange(t.kind)}</td>
      <td><button class="ghost-btn" data-action="template-open" data-tpl-id="${t.id}">Открыть</button></td>
    </tr>
  `).join("");
}

function getTemplateById(templateId) {
  return state.templates.find((x) => x.id === templateId) || null;
}

function templateDisplayName(template) {
  if (!template) return "шаблон";
  return `${templateTypeLabel(template.kind)}: ${template.name}`;
}

function canApplyTemplateToOverdue(template, daysOverdue) {
  if (!template) return false;
  return isTemplateEligibleForOverdue(template.kind, daysOverdue, { allowManualOnly: true });
}

function resetTemplateCreateDraft() {
  state.templateCreateDraft = {
    kind: DEFAULT_TEMPLATE_KIND,
    name: "",
    text: ""
  };
}

function exitTemplateCreateMode() {
  state.templateCreateMode = false;
  resetTemplateCreateDraft();
}

function renderTemplateTypeSelect() {
  const select = $("tplType");
  if (!select) return;
  const current = normalizeTemplateKind(select.value || state.templateEditorBaseline.kind || DEFAULT_TEMPLATE_KIND);
  select.innerHTML = TEMPLATE_TYPE_ORDER.map((kind) => `
    <option value="${kind}">${templateTypeLabel(kind)} (${templateTypeRange(kind)})</option>
  `).join("");
  select.value = current;
}

function syncTemplateTypeRule(kind) {
  setNotice("tplTypeRule", templateTypeRule(kind), "info");
}

function setTemplateEditorStatus(text, tone = "warning") {
  setNotice("tplEditorStatus", text, tone);
}

function renderTemplateEditorState() {
  renderTemplateTypeSelect();
  const typeSelect = $("tplType");
  const nameInput = $("tplName");
  const textInput = $("tplText");
  if (!typeSelect || !nameInput || !textInput) return;

  if (state.templateCreateMode) {
    const kind = normalizeTemplateKind(typeSelect.value || state.templateCreateDraft.kind);
    const name = nameInput.value.trim();
    const text = textInput.value.trim();
    state.templateCreateDraft = { kind, name, text };
    syncTemplateTypeRule(kind);

    const hasAnyInput = Boolean(name || text);
    const canSave = Boolean(name && text);

    $("tplDraft").disabled = !canSave;
    $("tplPublish").disabled = !canSave;
    $("tplCancel").disabled = false;

    if (!hasAnyInput) {
      setTemplateEditorStatus("Заполните поля и сохраните новый шаблон.", "warning");
      return;
    }
    if (!canSave) {
      setTemplateEditorStatus("Название и текст нового шаблона обязательны.", "warning");
      return;
    }
    setTemplateEditorStatus("Новый шаблон готов к сохранению. Выберите статус: черновик или активный.", "info");
    return;
  }

  const template = getTemplateById(state.selectedTemplateId);
  if (!template) {
    typeSelect.value = DEFAULT_TEMPLATE_KIND;
    syncTemplateTypeRule(DEFAULT_TEMPLATE_KIND);
    nameInput.value = "";
    textInput.value = "";
    $("tplDraft").disabled = true;
    $("tplPublish").disabled = true;
    $("tplCancel").disabled = true;
    setTemplateEditorStatus("Выберите шаблон из списка или нажмите «Создать новый шаблон».", "warning");
    return;
  }

  if (!typeSelect.value) {
    typeSelect.value = normalizeTemplateKind(template.kind);
  }
  const selectedKind = normalizeTemplateKind(typeSelect.value);
  syncTemplateTypeRule(selectedKind);

  const dirty =
    nameInput.value.trim() !== state.templateEditorBaseline.name ||
    textInput.value.trim() !== state.templateEditorBaseline.text ||
    selectedKind !== state.templateEditorBaseline.kind;

  $("tplDraft").disabled = false;
  $("tplPublish").disabled = false;
  $("tplCancel").disabled = !dirty;

  if (dirty) {
    setTemplateEditorStatus(`Есть несохраненные изменения в шаблоне «${template.name}».`, "warning");
  } else {
    setTemplateEditorStatus(`Шаблон «${template.name}» открыт. Изменений нет.`, "info");
  }
}

function loadTemplateToEditor(templateId) {
  const template = getTemplateById(templateId);
  if (!template) return false;
  exitTemplateCreateMode();
  state.selectedTemplateId = template.id;
  template.kind = normalizeTemplateKind(template.kind);
  $("tplType").value = template.kind;
  $("tplName").value = template.name || "";
  $("tplText").value = template.text || "";
  state.templateEditorBaseline = {
    kind: template.kind,
    name: $("tplName").value.trim(),
    text: $("tplText").value.trim()
  };
  renderTemplates();
  renderTemplateEditorState();
  return true;
}

async function applyTemplateEditorChanges(nextStatus, successText) {
  const kind = normalizeTemplateKind($("tplType").value);
  const name = $("tplName").value.trim();
  const text = $("tplText").value.trim();
  if (!name || !text) {
    toast("Название и текст шаблона обязательны");
    return false;
  }

  const currentTemplate = state.templateCreateMode ? null : getTemplateById(state.selectedTemplateId);
  const status = nextStatus || (currentTemplate?.status || "draft");
  const payload = { kind, name, text, status };

  try {
    if (state.templateCreateMode) {
      const created = await fetchApiJson("/api/templates", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      await refreshTemplatesFromBackend({ silent: true });
      exitTemplateCreateMode();
      loadTemplateToEditor(Number(created.id));
      const message = status === "active"
        ? `Новый шаблон «${name}» создан и активирован`
        : `Новый шаблон «${name}» создан как черновик`;
      toast(message);
      return true;
    }

    const template = getTemplateById(state.selectedTemplateId);
    if (!template) {
      toast("Сначала выберите шаблон в списке");
      return false;
    }

    await fetchApiJson(`/api/templates/${encodeURIComponent(template.id)}`, {
      method: "PUT",
      body: JSON.stringify(payload)
    });
    if (nextStatus) {
      await fetchApiJson(`/api/templates/${encodeURIComponent(template.id)}/status`, {
        method: "PATCH",
        body: JSON.stringify({ status: nextStatus })
      });
    }
    await refreshTemplatesFromBackend({ silent: true });
    loadTemplateToEditor(template.id);
    toast(successText);
    return true;
  } catch (error) {
    toast(`Не удалось сохранить шаблон: ${error?.message || "ошибка backend"}`);
    return false;
  }
}

function cancelTemplateEditorChanges(silent = false) {
  if (state.templateCreateMode) {
    exitTemplateCreateMode();
    if (state.selectedTemplateId) {
      loadTemplateToEditor(state.selectedTemplateId);
    } else {
      $("tplType").value = DEFAULT_TEMPLATE_KIND;
      $("tplName").value = "";
      $("tplText").value = "";
      renderTemplates();
      renderTemplateEditorState();
    }
    if (!silent) {
      toast("Создание нового шаблона отменено");
    }
    return;
  }

  const template = getTemplateById(state.selectedTemplateId);
  if (!template) return;
  $("tplType").value = state.templateEditorBaseline.kind;
  $("tplName").value = state.templateEditorBaseline.name;
  $("tplText").value = state.templateEditorBaseline.text;
  renderTemplateEditorState();
  if (!silent) {
    toast(`Изменения шаблона «${template.name}» отменены`);
  }
}

function createTemplate() {
  state.templateCreateMode = true;
  resetTemplateCreateDraft();
  $("tplType").value = DEFAULT_TEMPLATE_KIND;
  $("tplName").value = "";
  $("tplText").value = "";
  renderTemplates();
  renderTemplateEditorState();
  $("tplName").focus();
}

function ensureTemplateCreateModeFromEditorInput() {
  if (state.templateCreateMode || state.selectedTemplateId !== null) return false;
  state.templateCreateMode = true;
  state.templateCreateDraft = {
    kind: normalizeTemplateKind($("tplType").value || DEFAULT_TEMPLATE_KIND),
    name: $("tplName").value,
    text: $("tplText").value
  };
  renderTemplates();
  return true;
}

function resetManualPresetCreateDraft() {
  state.manualPresetCreateDraft = {
    title: "",
    text: DEFAULT_NEW_MANUAL_PRESET_TEXT
  };
}

function exitManualPresetCreateMode() {
  state.manualPresetCreateMode = false;
  resetManualPresetCreateDraft();
}

function ensureManualPresetSelection() {
  if (state.manualPresetCreateMode) return;
  if (state.manualReplyPresets.length === 0) {
    state.selectedManualPresetId = null;
    return;
  }
  if (!getManualPresetById(state.selectedManualPresetId)) {
    state.selectedManualPresetId = state.manualReplyPresets[0].id;
  }
}

function ensureManualPresetEditorCreateDefault() {
  if (state.manualPresetCreateMode) return;
  const selected = getManualPresetById(state.selectedManualPresetId);
  if (selected) return;
  state.selectedManualPresetId = null;
  state.manualPresetCreateMode = true;
  resetManualPresetCreateDraft();
}

function setManualPresetEditorStatus(text, tone = "warning") {
  setNotice("manualPresetEditorStatus", text, tone);
}

function renderManualPresetSelect() {
  const select = $("manualPreset");
  const openEditorBtn = $("openManualPresetManager");
  if (!select) return;
  ensureManualPresetSelection();
  if (state.manualReplyPresets.length === 0) {
    select.innerHTML = '<option value="">Типовые ответы не созданы</option>';
    select.disabled = true;
    if (openEditorBtn) openEditorBtn.disabled = true;
    return;
  }
  select.disabled = false;
  select.innerHTML = state.manualReplyPresets.map((preset) => `
    <option value="${preset.id}" ${preset.id === state.selectedManualPresetId ? "selected" : ""}>${preset.title}</option>
  `).join("");
  if (openEditorBtn) {
    openEditorBtn.disabled = !select.value;
  }
}

function renderManualPresetManager() {
  const body = $("manualPresetBody");
  if (!body) return;
  ensureManualPresetEditorCreateDefault();
  ensureManualPresetSelection();
  renderManualPresetSelect();
  if (state.manualReplyPresets.length === 0) {
    body.innerHTML = UI.emptyRow(3, "Типовые ответы еще не созданы");
    if (state.manualPresetCreateMode) {
      $("manualPresetTitle").value = state.manualPresetCreateDraft.title;
      $("manualPresetText").value = state.manualPresetCreateDraft.text;
      renderManualPresetEditorState();
      return;
    }
    $("manualPresetTitle").value = "";
    $("manualPresetText").value = "";
    $("manualPresetSave").disabled = true;
    $("manualPresetCancel").disabled = true;
    $("manualPresetDelete").disabled = true;
    setManualPresetEditorStatus("Типовых ответов нет. Нажмите «Создать новый типовой».", "warning");
    return;
  }
  body.innerHTML = state.manualReplyPresets.map((preset) => `
    <tr class="${!state.manualPresetCreateMode && preset.id === state.selectedManualPresetId ? "active-row" : ""}">
      <td>${preset.title}</td>
      <td class="cell-ellipsis"><span>${preset.text}</span></td>
      <td><button class="ghost-btn" data-action="manual-preset-open" data-preset-id="${preset.id}">Открыть</button></td>
    </tr>
  `).join("");
  if (state.manualPresetCreateMode) {
    $("manualPresetTitle").value = state.manualPresetCreateDraft.title;
    $("manualPresetText").value = state.manualPresetCreateDraft.text;
  } else {
    const selected = getManualPresetById(state.selectedManualPresetId);
    $("manualPresetTitle").value = selected ? selected.title : "";
    $("manualPresetText").value = selected ? selected.text : "";
  }
  renderManualPresetEditorState();
}

function renderManualPresetEditorState() {
  $("manualPresetNew").disabled = state.manualPresetCreateMode;

  if (state.manualPresetCreateMode) {
    const title = $("manualPresetTitle").value.trim();
    const text = $("manualPresetText").value.trim();
    state.manualPresetCreateDraft.title = title;
    state.manualPresetCreateDraft.text = text;

    const hasAnyInput = Boolean(title || text);
    const canSave = Boolean(title && text);

    $("manualPresetSave").disabled = !canSave;
    $("manualPresetCancel").disabled = false;
    $("manualPresetDelete").disabled = true;

    if (!hasAnyInput) {
      setManualPresetEditorStatus("Заполните поля и сохраните новый типовой ответ.", "warning");
      return;
    }
    if (!canSave) {
      setManualPresetEditorStatus("Название и текст нового типового ответа обязательны.", "warning");
      return;
    }
    setManualPresetEditorStatus("Нажмите «Сохранить типовой», чтобы создать новый типовой ответ.", "info");
    return;
  }

  const selected = getManualPresetById(state.selectedManualPresetId);
  if (!selected) {
    $("manualPresetSave").disabled = true;
    $("manualPresetCancel").disabled = true;
    $("manualPresetDelete").disabled = true;
    setManualPresetEditorStatus("Выберите типовой ответ или создайте новый.", "warning");
    return;
  }
  const title = $("manualPresetTitle").value.trim();
  const text = $("manualPresetText").value.trim();
  const dirty = title !== selected.title || text !== selected.text;
  const canSave = Boolean(title && text);

  $("manualPresetSave").disabled = !canSave || !dirty;
  $("manualPresetCancel").disabled = !dirty;
  $("manualPresetDelete").disabled = false;

  if (!canSave) {
    setManualPresetEditorStatus("Название и текст типового ответа обязательны.", "warning");
    return;
  }
  if (dirty) {
    setManualPresetEditorStatus(`Есть несохраненные изменения в типовом ответе «${selected.title}».`, "warning");
    return;
  }
  setManualPresetEditorStatus(`Типовой ответ «${selected.title}» открыт. Изменений нет.`, "info");
}

function createManualPreset() {
  state.manualPresetCreateMode = true;
  resetManualPresetCreateDraft();
  renderManualPresetManager();
  $("manualPresetTitle").focus();
  $("manualPresetTitle").select();
}

async function saveManualPresetFromEditor() {
  const title = $("manualPresetTitle").value.trim();
  const text = $("manualPresetText").value.trim();
  if (!title || !text) {
    toast("Заполните название и текст типового ответа");
    return false;
  }
  try {
    let savedId = null;
    if (state.manualPresetCreateMode || !state.selectedManualPresetId) {
      const created = await fetchApiJson("/api/manual-presets", {
        method: "POST",
        body: JSON.stringify({ title, text })
      });
      savedId = String(created.id);
    } else {
      const currentId = String(state.selectedManualPresetId);
      const updated = await fetchApiJson(`/api/manual-presets/${encodeURIComponent(currentId)}`, {
        method: "PUT",
        body: JSON.stringify({ title, text })
      });
      savedId = String(updated.id);
    }
    await refreshManualPresetsFromBackend({ silent: true });
    exitManualPresetCreateMode();
    state.selectedManualPresetId = savedId;
    renderManualPresetManager();
    return true;
  } catch (error) {
    toast(`Не удалось сохранить типовой ответ: ${error?.message || "ошибка backend"}`);
    return false;
  }
}

function cancelManualPresetChanges() {
  if (state.manualPresetCreateMode) {
    exitManualPresetCreateMode();
    renderManualPresetManager();
    return true;
  }
  const selected = getManualPresetById(state.selectedManualPresetId);
  if (!selected) return false;
  $("manualPresetTitle").value = selected.title;
  $("manualPresetText").value = selected.text;
  renderManualPresetEditorState();
  return true;
}

async function deleteSelectedManualPreset() {
  if (state.manualPresetCreateMode) {
    return false;
  }
  if (!state.selectedManualPresetId) return false;
  try {
    await fetchApiJson(`/api/manual-presets/${encodeURIComponent(state.selectedManualPresetId)}`, {
      method: "DELETE"
    });
    await refreshManualPresetsFromBackend({ silent: true });
    return true;
  } catch (error) {
    toast(`Не удалось удалить типовой ответ: ${error?.message || "ошибка backend"}`);
    return false;
  }
}

function readSettingsDraftFromUI() {
  return {
    loginUrl: $("cfgLoginUrl").value.trim(),
    login: $("cfgLogin").value.trim(),
    password: $("cfgPassword").value,
    gap: $("cfgGap").value.trim(),
    allowLiveDispatch: $("cfgAllowLiveDispatch")?.checked !== false,
    workWindowStart: $("cfgWorkWindowStart").value.trim() || DEFAULT_WORK_WINDOW_START,
    workWindowEnd: $("cfgWorkWindowEnd").value.trim() || DEFAULT_WORK_WINDOW_END,
    commentRules: {
      sms2: $("cfgCommentSms2").value.trim() || "смс2",
      sms3: $("cfgCommentSms3").value.trim() || "смс3",
      ka1: $("cfgCommentKa1").value.trim() || "смс от ка",
      kaN: $("cfgCommentKaN").value.trim() || "смс ка{n}",
      kaFinal: $("cfgCommentKaFinal").value.trim() || "смс ка фин"
    }
  };
}

function applySettingsDraftToUI(settings) {
  if (!settings) return;
  if (Object.prototype.hasOwnProperty.call(settings, "loginUrl")) {
    $("cfgLoginUrl").value = settings.loginUrl ?? "";
  }
  if (Object.prototype.hasOwnProperty.call(settings, "login")) {
    $("cfgLogin").value = settings.login ?? "";
  }
  if (Object.prototype.hasOwnProperty.call(settings, "password")) {
    $("cfgPassword").value = settings.password ?? "";
  }
  if (Object.prototype.hasOwnProperty.call(settings, "gap")) {
    $("cfgGap").value = settings.gap ?? "";
  }
  if (Object.prototype.hasOwnProperty.call(settings, "allowLiveDispatch")) {
    $("cfgAllowLiveDispatch").checked = settings.allowLiveDispatch !== false;
  }
  if (Object.prototype.hasOwnProperty.call(settings, "workWindowStart")) {
    $("cfgWorkWindowStart").value = settings.workWindowStart || DEFAULT_WORK_WINDOW_START;
  }
  if (Object.prototype.hasOwnProperty.call(settings, "workWindowEnd")) {
    $("cfgWorkWindowEnd").value = settings.workWindowEnd || DEFAULT_WORK_WINDOW_END;
  }
  $("cfgCommentSms2").value = settings.commentRules?.sms2 || "смс2";
  $("cfgCommentSms3").value = settings.commentRules?.sms3 || "смс3";
  $("cfgCommentKa1").value = settings.commentRules?.ka1 || "смс от ка";
  $("cfgCommentKaN").value = settings.commentRules?.kaN || "смс ка{n}";
  $("cfgCommentKaFinal").value = settings.commentRules?.kaFinal || "смс ка фин";
}

function setSettingsBaselineFromUI() {
  state.settingsBaseline = readSettingsDraftFromUI();
}

function hasSettingsUnsavedChanges() {
  if (!state.settingsBaseline) return false;
  const current = readSettingsDraftFromUI();
  return JSON.stringify(current) !== JSON.stringify(state.settingsBaseline);
}

function hasTemplateUnsavedChanges() {
  const typeSelect = $("tplType");
  const nameInput = $("tplName");
  const textInput = $("tplText");
  if (!nameInput || !textInput || !typeSelect) return false;
  const kind = normalizeTemplateKind(typeSelect.value);
  if (state.templateCreateMode) {
    return Boolean(nameInput.value.trim() || textInput.value.trim() || kind !== DEFAULT_TEMPLATE_KIND);
  }
  const template = getTemplateById(state.selectedTemplateId);
  if (!template) return false;
  return (
    kind !== state.templateEditorBaseline.kind ||
    nameInput.value.trim() !== state.templateEditorBaseline.name ||
    textInput.value.trim() !== state.templateEditorBaseline.text
  );
}

function hasManualPresetUnsavedChanges() {
  const titleInput = $("manualPresetTitle");
  const textInput = $("manualPresetText");
  if (!titleInput || !textInput) return false;
  const title = titleInput.value.trim();
  const text = textInput.value.trim();
  if (state.manualPresetCreateMode) {
    return title !== "" || text !== DEFAULT_NEW_MANUAL_PRESET_TEXT;
  }
  const selected = getManualPresetById(state.selectedManualPresetId);
  if (!selected) return false;
  return title !== selected.title || text !== selected.text;
}

async function resolveUnsavedChangesBeforeNavigation(options = {}) {
  const {
    actionLabel = "продолжить",
    includeTemplate = true,
    includeManualPreset = true,
    includeSettings = false
  } = options;
  const templateDirty = includeTemplate && hasTemplateUnsavedChanges();
  const manualDirty = includeManualPreset && hasManualPresetUnsavedChanges();
  const settingsDirty = includeSettings && hasSettingsUnsavedChanges();
  if (!templateDirty && !manualDirty && !settingsDirty) return true;

  const dirtyAreas = [];
  if (templateDirty) dirtyAreas.push("шаблонов рассылки");
  if (manualDirty) dirtyAreas.push("типовых ответов");
  if (settingsDirty) dirtyAreas.push("настроек");
  const areasText = dirtyAreas.join(" и ");

  const saveNow = window.confirm(
    `Есть несохраненные изменения в разделе ${areasText}. Нажмите «ОК», чтобы сохранить их перед тем, как ${actionLabel}. Нажмите «Отмена», чтобы перейти без сохранения.`
  );
  if (saveNow) {
    if (templateDirty) {
      const savedTemplate = await applyTemplateEditorChanges(null, "Шаблон сохранен");
      if (!savedTemplate) return false;
    }
    if (manualDirty) {
      const savedPreset = await saveManualPresetFromEditor();
      if (!savedPreset) return false;
      toast("Типовой ответ сохранен");
    }
    if (settingsDirty) {
      const settingsSaved = await saveSettings({ silent: true });
      if (!settingsSaved) return false;
    }
    return true;
  }

  if (templateDirty) {
    cancelTemplateEditorChanges(true);
  }
  if (manualDirty) {
    cancelManualPresetChanges();
  }
  if (settingsDirty && state.settingsBaseline) {
    applySettingsDraftToUI(state.settingsBaseline);
    void refreshRunForecastFromBackend({ silent: true });
  }
  return true;
}

function queueTemplateOptions(currentTemplateId) {
  const options = state.templates
    .filter((t) => t.status === "active" || t.id === currentTemplateId)
    .sort((a, b) => {
      const byKind = templateKindSortOrder(a.kind) - templateKindSortOrder(b.kind);
      if (byKind !== 0) return byKind;
      return String(a.name || "").localeCompare(String(b.name || ""), "ru");
    });
  const items = [];
  const hasCurrentTemplate = Number.isFinite(Number(currentTemplateId)) && Number(currentTemplateId) > 0;
  if (!hasCurrentTemplate) {
    items.push('<option value="" selected>Не назначен</option>');
  }
  if (options.length === 0) {
    items.push('<option value="" disabled>Нет активных шаблонов</option>');
    return items.join("");
  }
  items.push(...options.map((tpl) => `
    <option value="${tpl.id}" ${tpl.id === currentTemplateId ? "selected" : ""}>${templateTypeLabel(tpl.kind)}: ${tpl.name}</option>
  `));
  return items.join("");
}

function shouldPassClientsViewFilter(client) {
  const search = state.clientsViewFilters.search.trim().toLowerCase();
  if (search) {
    const searchPhone = normalizePhone(search);
    const inClient = String(client.client || "").toLowerCase().includes(search);
    const inPhone = searchPhone ? normalizePhone(client.phone).includes(searchPhone) : false;
    const inContract = String(client.contractNumber || client.id || "").toLowerCase().includes(search);
    if (!inClient && !inPhone && !inContract) return false;
  }
  if (state.clientsViewFilters.tz !== "all" && client.tzOffset !== Number(state.clientsViewFilters.tz)) {
    return false;
  }
  if (state.clientsViewFilters.overdue !== "all" && overdueRange(client.daysOverdue) !== state.clientsViewFilters.overdue) {
    return false;
  }
  return true;
}

function renderClientsDb() {
  syncSelectionSets();
  const body = $("clientsDbBody");
  if (!body) return;
  const rows = getVisibleClientRows();
  if (rows.length === 0) {
    body.innerHTML = UI.emptyRow(10, "Клиенты не найдены");
    renderClientsBulkUi(rows);
    return;
  }

  body.innerHTML = rows.map((client) => {
    const dialogMeta = dialogStatusByPhone(client.phone);
    const inPlan = hasClientInQueue(client.id);
    const inStop = isPhoneInStopList(client.phone);
    const excluded = state.excludedClientIds.has(client.id);
    let planText = "Нет";
    if (!state.planPrepared) {
      planText = "План не сформирован";
    } else if (inPlan) {
      planText = "Да";
    } else if (inStop) {
      planText = "Нет (стоп-лист)";
    } else if (excluded) {
      planText = "Нет (исключен вручную)";
    } else if (!shouldPassRunFilter(client)) {
      planText = "Нет (вне фильтра)";
    }

    return `
      <tr class="${state.selectedClients.has(client.id) ? "row-selected" : ""}">
        <td class="select-col"><input type="checkbox" data-action="client-select-row" data-client-id="${client.id}" ${state.selectedClients.has(client.id) ? "checked" : ""}></td>
        <td>${client.client}</td>
        <td>${client.cardUrl ? `<a href="${client.cardUrl}" target="_blank" rel="noopener noreferrer">Открыть</a>` : "-"}</td>
        <td>${client.phone}</td>
        <td>${MSK_TZ_LABEL[String(client.tzOffset)]}</td>
        <td>${client.daysOverdue}</td>
        <td class="cell-ellipsis"><span>${debtCellText(client)}</span></td>
        <td>${dialogMeta.text}</td>
        <td>${planText}</td>
        <td>
          <div class="actions slim">
            <button class="ghost-btn" data-action="client-open-dialog" data-client-phone="${client.phone}">В диалог</button>
            <button class="ghost-btn" data-action="client-fetch-debt" data-client-external-id="${client.externalClientId || ""}" ${client.externalClientId ? "" : "disabled"}>Узнать сумму</button>
            <button class="ghost-btn" data-action="client-toggle-stop" data-client-phone="${client.phone}">${inStop ? "Убрать из стоп-листа" : "В стоп-лист"}</button>
            ${excluded ? `<button class="ghost-btn" data-action="client-return-plan" data-client-id="${client.id}" ${state.runRuntime ? "disabled" : ""}>Вернуть в план</button>` : ""}
          </div>
        </td>
      </tr>
    `;
  }).join("");
  renderClientsBulkUi(rows);
}

function renderQueue() {
  syncSelectionSets();
  const rows = getVisibleQueueRows();
  if (rows.length === 0) {
    $("queueBody").innerHTML = UI.emptyRow(14, "Очередь пуста. Сформируйте плановую очередь после актуализации базы.");
    renderQueueBulkUi(rows);
    renderQueueRetryActionState();
    return;
  }

  $("queueBody").innerHTML = rows.map((q) => `
    <tr class="${state.selectedQueue.has(q.id) ? "row-selected" : ""}">
      <td class="select-col"><input type="checkbox" data-action="queue-select-row" data-q-id="${q.id}" ${state.selectedQueue.has(q.id) ? "checked" : ""}></td>
      <td class="cell-ellipsis"><span>${q.client}</span></td>
      <td>${q.cardUrl ? `<a href="${q.cardUrl}" target="_blank" rel="noopener noreferrer">Открыть</a>` : "-"}</td>
      <td>${q.phone}</td>
      <td>${MSK_TZ_LABEL[String(q.tzOffset)]}</td>
      <td>${q.daysOverdue}</td>
      <td>
        <select class="queue-template-select" data-action="queue-set-template" data-q-id="${q.id}" ${state.runRuntime ? "disabled" : ""}>
          ${queueTemplateOptions(q.templateId)}
        </select>
      </td>
      <td class="cell-ellipsis"><span>${senderNameById(q.senderId)}</span></td>
      <td class="cell-ellipsis"><span>${debtCellText(q)}</span></td>
      <td>${queuePlanCellText(q)}</td>
      <td>${statusCell(q.status)}</td>
      <td>${q.attempts}/${q.maxAttempts}</td>
      <td class="cell-ellipsis"><span>${dialogStatusByPhone(q.phone).text}</span></td>
      <td>
        <div class="actions slim">
          <button class="ghost-btn" data-action="queue-open-dialog" data-q-phone="${q.phone}">В диалог</button>
          <button class="ghost-btn" data-action="queue-fetch-debt" data-q-external-id="${q.externalClientId || ""}" ${q.externalClientId ? "" : "disabled"}>Узнать сумму</button>
          <button class="ghost-btn" data-action="queue-toggle-stop" data-q-id="${q.id}" data-q-phone="${q.phone}">${isPhoneInStopList(q.phone) ? "Убрать из стоп-листа" : "В стоп-лист"}</button>
          <button class="ghost-btn" data-action="queue-remove-plan" data-q-id="${q.id}" ${state.runRuntime || !["queued", "retry"].includes(q.status) ? "disabled" : ""}>Убрать из плана</button>
        </div>
      </td>
    </tr>
  `).join("");
  renderQueueBulkUi(rows);
  renderQueueRetryActionState();
}

function renderDialogs() {
  const list = $("dialogList");
  const sortedDialogs = [...state.dialogs].sort((a, b) => dialogLastActivityUtcMs(b) - dialogLastActivityUtcMs(a));
  if (sortedDialogs.length === 0) {
    state.selectedDialogId = null;
    list.innerHTML = '<p class="muted-note dialog-empty">Диалогов нет</p>';
    renderDialogMaintenanceState();
    return;
  }
  if (state.selectedDialogId !== null && !sortedDialogs.some((d) => String(d.id) === String(state.selectedDialogId))) {
    state.selectedDialogId = null;
  }
  if (state.selectedDialogId === null && sortedDialogs.length > 0) {
    state.selectedDialogId = sortedDialogs[0].id;
    state.dialogForceScrollPhone = normalizePhone(sortedDialogs[0].phone);
  }
  list.innerHTML = sortedDialogs.map((d) => `
    <div class="contact-item ${String(state.selectedDialogId) === String(d.id) ? "active" : ""}" data-action="dialog-select" data-dialog-id="${d.id}">
      <div class="contact-head">
        <strong>${d.fio}</strong>
      </div>
      <p>${d.phone}</p>
      <small class="contact-meta">Девайс: ${dialogChannelLabel(d)}</small>
      <small class="contact-meta">Активность: ${formatMskDateTime(dialogLastActivityUtcMs(d))}</small>
      ${isPhoneInStopList(d.phone) ? "<small class=\"contact-flag\">в стоп-листе</small>" : ""}
    </div>
  `).join("");
  renderDialogMaintenanceState();
}

function renderChat() {
  renderManualPresetSelect();
  const dialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
  if (!dialog) {
    $("dialogTitle").textContent = "Выберите клиента";
    $("chatThread").innerHTML = '<p class="muted-note">Диалог не выбран или уже удален.</p>';
    $("manualInput").value = "";
    resetDialogPreview();
    renderDialogMaintenanceState();
    return;
  }

  const phoneKey = normalizePhone(dialog.phone);
  const chatNode = $("chatThread");
  const wasNearBottom = chatNode
    ? (chatNode.scrollHeight - chatNode.scrollTop - chatNode.clientHeight) <= 56
    : true;
  const previousTail = String(state.dialogLastRenderedMessageByPhone[phoneKey] || "");

  if (!dialog.messagesLoaded && !dialog.messagesLoading) {
    void refreshDialogMessagesFromBackend(dialog.phone, { silent: true });
  }

  $("dialogTitle").textContent = `${dialog.fio} · ${dialog.phone} · Девайс: ${dialogChannelLabel(dialog)}`;
  if (dialog.messagesLoading && !dialog.messagesLoaded) {
    $("chatThread").innerHTML = '<p class="muted-note">Загрузка истории сообщений...</p>';
  } else if (!Array.isArray(dialog.messages) || dialog.messages.length === 0) {
    $("chatThread").innerHTML = '<p class="muted-note">Сообщений пока нет.</p>';
  } else {
    $("chatThread").innerHTML = dialog.messages.map((m) => `
    <div class="msg ${m.type}">
      <div>${m.text}</div>
      <small>${m.at}</small>
    </div>
  `).join("");
  }

  const messages = Array.isArray(dialog.messages) ? dialog.messages : [];
  const lastMessage = messages.length > 0 ? messages[messages.length - 1] : null;
  const nextTail = lastMessage ? String(lastMessage.id || lastMessage.createdAtUtc || messages.length) : "";
  const forcedOpen = state.dialogForceScrollPhone === phoneKey;
  if (forcedOpen) {
    state.dialogForceScrollPhone = "";
  }
  const hasNewTail = Boolean(nextTail && nextTail !== previousTail);
  const shouldScrollToLast = forcedOpen || (!previousTail && Boolean(nextTail)) || (hasNewTail && wasNearBottom);
  state.dialogLastRenderedMessageByPhone[phoneKey] = nextTail;

  if (shouldScrollToLast) {
    requestAnimationFrame(() => {
      const node = $("chatThread");
      if (!node) return;
      node.scrollTop = node.scrollHeight;
    });
  }

  if (!dialog.manualDraftLoaded && !dialog.manualDraftLoading) {
    void refreshDialogDraftFromBackend(dialog.phone, { silent: true, applyToInput: true });
  }
  if (!dialog.manualDraftDirty) {
    $("manualInput").value = String(dialog.manualDraftText || "");
  }
  if (!state.dialogPreview.enabled) {
    syncDialogPreviewWithQueue();
  }
  renderDialogMaintenanceState();
}

function renderDialogMaintenanceState() {
  const selectedDialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId)) || null;
  const hasSelected = Boolean(selectedDialog);
  if ($("dialogDeleteCurrent")) {
    $("dialogDeleteCurrent").disabled = !hasSelected;
  }
  if ($("dialogPruneOld")) {
    $("dialogPruneOld").disabled = state.dialogs.length === 0;
  }
  if ($("dialogPreviewToggle")) {
    $("dialogPreviewToggle").disabled = !hasSelected;
    $("dialogPreviewToggle").checked = hasSelected && Boolean(state.dialogPreview.enabled);
  }
  renderDialogPreviewPanel();
}

function renderDialogPreviewPanel() {
  const panel = $("dialogPreviewPanel");
  const statusNode = $("dialogPreviewStatus");
  const editorNode = $("dialogPreviewEditor");
  const saveBtn = $("dialogPreviewSave");
  const resetBtn = $("dialogPreviewReset");
  const hintNode = $("dialogPreviewEditorHint");
  const metaNode = $("dialogPreviewMeta");
  if (!panel || !statusNode || !editorNode || !metaNode || !saveBtn || !resetBtn || !hintNode) return;

  const selectedDialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId)) || null;
  if (!selectedDialog || !state.dialogPreview.enabled) {
    panel.classList.add("hidden");
    return;
  }

  panel.classList.remove("hidden");
  const preview = state.dialogPreview || {};
  const jobId = Number(preview.jobId || 0);
  const status = String(preview.status || "empty").trim() || "empty";
  const tone = status === "ready" ? "success" : (status === "error" || status === "needs_debt") ? "warning" : "info";

  const codePart = preview.errorCode ? ` · ${preview.errorCode}` : "";
  if (!jobId) {
    setNotice("dialogPreviewStatus", `Превью SMS: ${previewStatusText(status)}${codePart}`, tone);
    metaNode.textContent = String(preview.errorDetail || "Клиент не находится в активной/плановой очереди.");
    editorNode.value = "";
    editorNode.disabled = true;
    saveBtn.disabled = true;
    resetBtn.disabled = true;
    hintNode.textContent = "Редактирование доступно только для клиента, который присутствует в активной/плановой очереди.";
    return;
  }

  const currentSourceText = String(preview.text || "").trim();
  if (state.dialogPreview.editorSourceJobId !== jobId) {
    state.dialogPreview.editorSourceJobId = jobId;
    state.dialogPreview.editorText = currentSourceText;
    state.dialogPreview.editorDirty = false;
  } else if (!state.dialogPreview.editorDirty && state.dialogPreview.editorText !== currentSourceText) {
    state.dialogPreview.editorText = currentSourceText;
  }
  if (editorNode.value !== state.dialogPreview.editorText) {
    editorNode.value = state.dialogPreview.editorText;
  }

  setNotice("dialogPreviewStatus", `Задача #${jobId}: ${previewStatusText(status)}${codePart}`, tone);
  const parsedVars = parsePreviewVariablesJson(preview.variablesJson);
  const totalRaw = String(parsedVars?.totalWithCommissionRaw || "").trim();
  const approxDebt = String(parsedVars?.approxDebtText || "").trim();
  const templateName = String(parsedVars?.templateName || "").trim();
  const templateKind = String(parsedVars?.templateKind || "").trim();
  const overrideEnabled = Boolean(preview.hasMessageOverride);
  const queueRow = state.queue.find((x) => Number(x.id) === jobId) || null;
  const canEdit = Boolean(queueRow) && ["queued", "retry"].includes(String(queueRow.status || ""));
  const updatedAt = toMskDateTimeOrEmpty(preview.updatedAtUtc);
  const errorDetail = String(preview.errorDetail || "").trim();
  const meta = [
    `Клиент: ${preview.client || selectedDialog.fio || "—"}`,
    `Телефон: ${preview.phone || selectedDialog.phone || "—"}`,
    `Шаблон: ${templateName || "—"}${templateKind ? ` (${templateKind})` : ""}`,
    `Ручной текст: ${overrideEnabled ? "включен" : "нет"}`,
    `Точная сумма: ${totalRaw || "—"}`,
    `Расчетная сумма: ${approxDebt || "—"}`,
    `Обновлено: ${updatedAt || "—"}`
  ];
  if (errorDetail) {
    meta.push(`Детали: ${errorDetail}`);
  }
  metaNode.textContent = meta.join(" | ");
  editorNode.disabled = Boolean(state.dialogPreview.editorSaving) || !canEdit;
  const hasEditorText = Boolean(String(state.dialogPreview.editorText || "").trim());
  saveBtn.disabled = Boolean(state.dialogPreview.editorSaving) || !canEdit || !state.dialogPreview.editorDirty || !hasEditorText;
  resetBtn.disabled = Boolean(state.dialogPreview.editorSaving) || !canEdit || !overrideEnabled;
  if (!canEdit) {
    hintNode.textContent = "Редактирование доступно только для задач со статусом «В очереди» или «Повтор» и до фактической отправки.";
  } else if (overrideEnabled) {
    hintNode.textContent = "Сохранен ручной текст для этого клиента. Можно изменить и сохранить снова или сбросить к шаблону.";
  } else {
    hintNode.textContent = "После сохранения этот текст будет отправлен именно этому клиенту вместо текста шаблона.";
  }
}

async function rebuildDialogPreviewForSelected(options = {}) {
  const { silent = false } = options;
  const selectedDialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId)) || null;
  if (!selectedDialog) {
    resetDialogPreview();
    renderDialogPreviewPanel();
    return null;
  }

  const queueJob = findQueueJobForPhone(selectedDialog.phone);
  if (!queueJob) {
    state.dialogPreview.phone = toApiPhone(selectedDialog.phone || "");
    state.dialogPreview.jobId = null;
    state.dialogPreview.client = String(selectedDialog.fio || "").trim();
    state.dialogPreview.status = "empty";
    state.dialogPreview.text = "";
    state.dialogPreview.variablesJson = "";
    state.dialogPreview.updatedAtUtc = "";
    state.dialogPreview.errorCode = "PREVIEW_NO_QUEUE_JOB";
    state.dialogPreview.errorDetail = "Клиент не находится в плановой/активной очереди.";
    state.dialogPreview.hasMessageOverride = false;
    state.dialogPreview.messageOverrideText = "";
    state.dialogPreview.editorSourceJobId = null;
    state.dialogPreview.editorText = "";
    state.dialogPreview.editorDirty = false;
    state.dialogPreview.editorSaving = false;
    renderDialogPreviewPanel();
    return null;
  }

  const result = await rebuildQueueJobPreview(queueJob.id, { silent: true, skipRenderQueue: true });
  if (!result) {
    if (!silent) {
      toast("Не удалось обновить превью SMS для выбранного клиента");
    }
    renderDialogPreviewPanel();
    return null;
  }

  const refreshedJob = state.queue.find((x) => Number(x.id) === Number(queueJob.id));
  if (refreshedJob) {
    setDialogPreviewFromQueueJob(refreshedJob);
  } else {
    setDialogPreviewFromQueueJob({
      id: queueJob.id,
      phone: selectedDialog.phone,
      client: selectedDialog.fio,
      previewStatus: result.status,
      previewText: result.text,
      previewVariablesJson: result.variablesJson,
      previewUpdatedAtUtc: result.updatedAtUtc,
      previewErrorCode: result.errorCode,
      previewErrorDetail: result.errorDetail,
      hasMessageOverride: Boolean(queueJob.hasMessageOverride),
      messageOverrideText: String(queueJob.messageOverrideText || "")
    });
  }

  renderQueue();
  renderDialogPreviewPanel();
  return result;
}

function applyPreviewDtoToQueueJob(queueJob, previewDto) {
  if (!queueJob || !previewDto) return;
  queueJob.previewStatus = String(previewDto.status || "empty");
  queueJob.previewText = String(previewDto.text || "");
  queueJob.previewVariablesJson = String(previewDto.variablesJson || "");
  queueJob.previewUpdatedAtUtc = String(previewDto.updatedAtUtc || "");
  queueJob.previewErrorCode = String(previewDto.errorCode || "");
  queueJob.previewErrorDetail = String(previewDto.errorDetail || "");
  if (previewDto.templateId != null) {
    queueJob.templateId = Number(previewDto.templateId);
  }
  if (previewDto.templateName != null) {
    queueJob.templateName = String(previewDto.templateName || "").trim();
  }
  if (previewDto.templateKind != null) {
    queueJob.templateKind = String(previewDto.templateKind || "").trim();
  }
}

async function saveDialogPreviewOverride() {
  const selectedDialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId)) || null;
  const jobId = Number(state.dialogPreview.jobId || 0);
  const queueJob = state.queue.find((x) => Number(x.id) === jobId) || null;
  if (!selectedDialog || !jobId) {
    toast("Клиент не находится в активной/плановой очереди");
    return false;
  }
  if (!queueJob || !["queued", "retry"].includes(String(queueJob.status || ""))) {
    toast("Редактирование доступно только для задач со статусом «В очереди» или «Повтор»");
    return false;
  }

  const text = String(state.dialogPreview.editorText || "").trim();
  if (!text) {
    toast("Текст SMS не должен быть пустым");
    return false;
  }

  state.dialogPreview.editorSaving = true;
  renderDialogPreviewPanel();
  try {
    const data = await fetchApiJson(`/api/queue/${encodeURIComponent(jobId)}/message-override`, {
      method: "PUT",
      body: JSON.stringify({ text })
    });

    if (queueJob) {
      queueJob.hasMessageOverride = Boolean(data?.hasMessageOverride);
      queueJob.messageOverrideText = String(data?.messageOverrideText || "");
      applyPreviewDtoToQueueJob(queueJob, data?.preview || null);
      setDialogPreviewFromQueueJob(queueJob);
    } else {
      await refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId || null });
      syncDialogPreviewWithQueue();
    }

    state.dialogPreview.editorDirty = false;
    state.dialogPreview.editorText = String(state.dialogPreview.text || "");
    renderQueue();
    renderDialogPreviewPanel();
    toast("Текст SMS для клиента сохранен");
    return true;
  } catch (error) {
    toast(`Не удалось сохранить текст SMS: ${error?.message || "ошибка backend"}`);
    renderDialogPreviewPanel();
    return false;
  } finally {
    state.dialogPreview.editorSaving = false;
    renderDialogPreviewPanel();
  }
}

async function ensureDialogPreviewEditsBeforeRun() {
  if (!state.dialogPreview?.enabled || !state.dialogPreview?.editorDirty) {
    return true;
  }

  const jobId = Number(state.dialogPreview.jobId || 0);
  if (!jobId) {
    return true;
  }

  const queueJob = state.queue.find((x) => Number(x.id) === jobId) || null;
  if (!queueJob || !["queued", "retry"].includes(String(queueJob.status || ""))) {
    return true;
  }

  const shouldSave = window.confirm(
    "У выбранного клиента есть несохраненное изменение превью SMS. Сохранить перед запуском?"
  );
  if (!shouldSave) {
    toast("Запуск отменен: сначала сохраните или сбросьте изменение превью SMS.");
    return false;
  }

  const saved = await saveDialogPreviewOverride();
  if (!saved) {
    toast("Запуск отменен: не удалось сохранить изменение превью SMS.");
    return false;
  }

  return true;
}

async function clearDialogPreviewOverride() {
  const selectedDialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId)) || null;
  const jobId = Number(state.dialogPreview.jobId || 0);
  const queueJob = state.queue.find((x) => Number(x.id) === jobId) || null;
  if (!selectedDialog || !jobId) {
    toast("Клиент не находится в активной/плановой очереди");
    return;
  }
  if (!queueJob || !["queued", "retry"].includes(String(queueJob.status || ""))) {
    toast("Сброс доступен только для задач со статусом «В очереди» или «Повтор»");
    return;
  }

  state.dialogPreview.editorSaving = true;
  renderDialogPreviewPanel();
  try {
    const data = await fetchApiJson(`/api/queue/${encodeURIComponent(jobId)}/message-override`, {
      method: "DELETE"
    });

    if (queueJob) {
      queueJob.hasMessageOverride = Boolean(data?.hasMessageOverride);
      queueJob.messageOverrideText = String(data?.messageOverrideText || "");
      applyPreviewDtoToQueueJob(queueJob, data?.preview || null);
      setDialogPreviewFromQueueJob(queueJob);
    } else {
      await refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId || null });
      syncDialogPreviewWithQueue();
    }

    state.dialogPreview.editorDirty = false;
    state.dialogPreview.editorText = String(state.dialogPreview.text || "");
    renderQueue();
    renderDialogPreviewPanel();
    toast("Возвращен текст SMS по шаблону");
  } catch (error) {
    toast(`Не удалось сбросить ручной текст: ${error?.message || "ошибка backend"}`);
    renderDialogPreviewPanel();
  } finally {
    state.dialogPreview.editorSaving = false;
    renderDialogPreviewPanel();
  }
}

function renderStopList() {
  syncSelectionSets();
  const body = $("stopListBody");
  if (!body) return;
  const rows = getVisibleStopRows();

  if (rows.length === 0) {
    body.innerHTML = UI.emptyRow(8, "Записи стоп-листа не найдены");
    renderStopBulkUi(rows);
    return;
  }

  body.innerHTML = rows.map((s) => {
    const meta = s.meta || null;
    const fio = meta?.fio || "-";
    const contractNumber = meta?.contractNumber || "-";
    const contractCell = meta?.cardUrl
      ? `<a href="${meta.cardUrl}" target="_blank" rel="noopener noreferrer">${contractNumber}</a>`
      : contractNumber;
    return `
    <tr>
      <td class="select-col"><input type="checkbox" data-action="stop-select-row" data-stop-id="${s.id}" ${state.selectedStopList.has(s.id) ? "checked" : ""}></td>
      <td>${s.phone}</td>
      <td>${fio}</td>
      <td>${contractCell}</td>
      <td>${s.reason || "-"}</td>
      <td>${s.added}</td>
      <td>${s.source}</td>
      <td><button class="ghost-btn" data-action="stop-remove" data-stop-id="${s.id}">Убрать</button></td>
    </tr>
  `;
  }).join("");
  renderStopBulkUi(rows);
}

function renderBars() {
  const barsNode = $("weeklyBars");
  const weekly = Array.isArray(state.reportWeeklyDays) ? state.reportWeeklyDays : [];
  if (weekly.length === 0) {
    barsNode.innerHTML = '<p class="muted-note">Данные по неделе пока недоступны.</p>';
    return;
  }
  const totals = weekly.map((day) => Number(day?.total || (Number(day?.sent || 0) + Number(day?.failed || 0))));
  const max = Math.max(...totals, 1);
  barsNode.innerHTML = weekly.map((day, i) => {
    const total = totals[i];
    const label = String(day?.label || `Д${i + 1}`);
    return `
    <div class="bar-wrap">
      <div class="bar" style="height:${Math.round(total / max * 140)}px"></div>
      <div>${label}</div>
      <strong>${total}</strong>
    </div>
  `;
  }).join("");
}

function stopRunPolling() {
  if (state.runPollTimer) {
    clearInterval(state.runPollTimer);
    state.runPollTimer = null;
  }
}

function stopEventStream() {
  if (state.eventStreamRetryTimer) {
    clearTimeout(state.eventStreamRetryTimer);
    state.eventStreamRetryTimer = null;
  }
  if (state.eventStream) {
    state.eventStream.close();
    state.eventStream = null;
  }
  state.eventStreamConnected = false;
  state.eventStreamRetryAttempt = 0;
}

function eventStreamRetryDelayMs() {
  const attempt = Math.max(0, Number(state.eventStreamRetryAttempt || 0));
  const cappedAttempt = Math.min(attempt, 6);
  const baseMs = 1500;
  const maxMs = 30000;
  const backoffMs = Math.min(maxMs, baseMs * Math.pow(2, cappedAttempt));
  const jitterMs = Math.floor(Math.random() * 800);
  return backoffMs + jitterMs;
}

function pushCriticalEventToast(message) {
  const now = Date.now();
  const safeMessage = String(message || "").trim() || "подробности в логе выполнения";
  if (now < state.criticalErrorToastCooldownUntilMs) {
    state.criticalErrorToastSuppressed += 1;
    return;
  }

  const suppressed = Number(state.criticalErrorToastSuppressed || 0);
  state.criticalErrorToastSuppressed = 0;
  state.criticalErrorToastCooldownUntilMs = now + 4000;
  const suffix = suppressed > 0 ? ` (+${suppressed} событий)` : "";
  toast(`Критичная ошибка: ${safeMessage}${suffix}`);
}

function scheduleRealtimeRefresh() {
  if (state.realtimeRefreshTimer || state.realtimeRefreshBusy) return;
  state.realtimeRefreshTimer = setTimeout(async () => {
    state.realtimeRefreshTimer = null;
    if (state.realtimeRefreshBusy) return;
    state.realtimeRefreshBusy = true;
    try {
      await refreshRunStatusFromBackend({ silent: true });
      await Promise.all([
        refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId || null }),
        refreshChannelsFromBackend({ silent: true }),
        refreshAlertsFromBackend({ silent: true }),
        refreshReportsFromBackend({ silent: true })
      ]);
      if (activeTabId() === "dialogs") {
        await refreshDialogsFromBackend({ silent: true });
      }
    } finally {
      state.realtimeRefreshBusy = false;
    }
  }, 700);
}

function handleRunEventPayload(rawEvent, isHistory) {
  if (!rawEvent || typeof rawEvent !== "object") return;
  const id = Number(rawEvent.id || 0);
  if (id > 0) {
    state.eventStreamSinceId = Math.max(state.eventStreamSinceId, id);
  }

  const message = String(rawEvent.message || "").trim();
  if (message) {
    const category = String(rawEvent.category || "event").trim();
    const eventType = String(rawEvent.eventType || "event").trim();
    const prefix = `[${category}/${eventType}]`;
    addRunLog(`${prefix} ${message}`, safeDateMs(rawEvent.createdAtUtc));
  }

  const severity = String(rawEvent.severity || "").trim().toLowerCase();
  if (!isHistory && severity === "error") {
    pushCriticalEventToast(message);
  }

  scheduleRealtimeRefresh();
}

function attachRunEventStreamHandlers(stream) {
  stream.addEventListener("run_history", (event) => {
    try {
      const payload = JSON.parse(String(event.data || "{}"));
      handleRunEventPayload(payload, true);
    } catch {
      // ignore malformed history chunk
    }
  });

  stream.addEventListener("run_event", (event) => {
    try {
      const payload = JSON.parse(String(event.data || "{}"));
      handleRunEventPayload(payload, false);
    } catch {
      // ignore malformed realtime chunk
    }
  });
}

function startEventStream(options = {}) {
  const { resetSince = false } = options;
  stopEventStream();
  if (resetSince) {
    state.eventStreamSinceId = 0;
  }

  const qs = new URLSearchParams();
  if (state.eventStreamSinceId > 0) {
    qs.set("sinceId", String(state.eventStreamSinceId));
  } else {
    qs.set("historyLimit", "120");
  }
  qs.set("pollMs", "1000");
  qs.set("heartbeatSec", "15");

  const stream = new EventSource(apiUrl(`/api/events/run?${qs.toString()}`));
  state.eventStream = stream;
  attachRunEventStreamHandlers(stream);

  stream.onopen = () => {
    state.eventStreamConnected = true;
    state.eventStreamRetryAttempt = 0;
  };

  stream.onerror = () => {
    if (state.eventStream) {
      state.eventStream.close();
      state.eventStream = null;
    }

    const wasConnected = state.eventStreamConnected;
    state.eventStreamConnected = false;
    scheduleRealtimeRefresh();

    if (!state.eventStreamRetryTimer) {
      const delayMs = eventStreamRetryDelayMs();
      state.eventStreamRetryAttempt = Math.min(10, Number(state.eventStreamRetryAttempt || 0) + 1);
      if (wasConnected || state.eventStreamRetryAttempt <= 2 || state.eventStreamRetryAttempt % 3 === 0) {
        addRunLog(
          `Realtime-поток событий временно недоступен. Переподключение через ${Math.round(delayMs / 1000)}с (попытка ${state.eventStreamRetryAttempt}).`
        );
      }
      state.eventStreamRetryTimer = setTimeout(() => {
        state.eventStreamRetryTimer = null;
        startEventStream();
      }, delayMs);
    }
  };
}

function startRunPolling() {
  stopRunPolling();
  state.runPollTimer = setInterval(async () => {
    if (state.runPollBusy) return;
    state.runPollBusy = true;
    try {
      await refreshRunStatusFromBackend({ silent: true });
      await Promise.all([
        refreshQueueFromBackend({ silent: true }),
        refreshChannelsFromBackend({ silent: true }),
        refreshAlertsFromBackend({ silent: true }),
        refreshReportsFromBackend({ silent: true })
      ]);
    } finally {
      state.runPollBusy = false;
    }
  }, 2000);
}

function stopForecastPolling() {
  if (state.forecastPollTimer) {
    clearInterval(state.forecastPollTimer);
    state.forecastPollTimer = null;
  }
}

function startForecastPolling() {
  stopForecastPolling();
  state.forecastPollTimer = setInterval(async () => {
    if (state.forecastPollBusy) return;
    if (activeTabId() !== "run") return;
    if (state.clientsDb.syncing || !state.clientsDb.actualized) return;
    if (!hasExpectedSnapshotModeLoaded()) return;
    if (!hasAnyRunFilterSelected()) return;

    state.forecastPollBusy = true;
    try {
      await refreshRunForecastFromBackend({ silent: true });
    } finally {
      state.forecastPollBusy = false;
    }
  }, 10000);
}

function stopDialogPolling() {
  if (state.dialogPollTimer) {
    clearInterval(state.dialogPollTimer);
    state.dialogPollTimer = null;
  }
}

function startDialogPolling() {
  stopDialogPolling();
  state.dialogPollTimer = setInterval(async () => {
    if (state.dialogPollBusy) return;
    const activeTab = document.querySelector(".tab-btn.active")?.dataset?.tab || "";
    if (activeTab !== "dialogs") return;

    state.dialogPollBusy = true;
    try {
      await refreshDialogsFromBackend({ silent: true });
      const selected = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
      if (selected) {
        await refreshDialogMessagesFromBackend(selected.phone, { silent: true });
      }
    } finally {
      state.dialogPollBusy = false;
    }
  }, 2500);
}

function applyRunStatusToUi(data) {
  const statusMap = {
    planned: "Готово",
    running: "Выполняется",
    stopped: "Остановлено",
    completed: "Завершено"
  };

  if (data?.hasSession && data.session?.id) {
    state.queueSessionId = Number(data.session.id);
    state.queueSession = data.session;
    state.planPrepared = Number(data.session.totalJobs || 0) > 0;
    if (data.session.status === "running") {
      state.planStale = false;
    }
  } else {
    state.queueSessionId = null;
    state.queueSession = null;
    state.planPrepared = false;
    resetDialogPreview();
  }

  state.runRuntime = data?.hasRunningSession
    ? { sessionId: Number(data.runningSessionId || 0) }
    : null;
  state.runCanStart = data?.canStart !== false;
  state.runStartBlockMessage = data?.startBlockMessage || "";

  if (!state.runRuntime) {
    stopRunPolling();
  }

  const label = data?.hasSession
    ? (statusMap[data.session.status] || data.session.status || "Готово")
    : "Готово";
  $("sessionStatus").textContent = label;
}

async function refreshRunStatusFromBackend(options = {}) {
  const { silent = false, runSessionId = null } = options;
  try {
    const path = runSessionId
      ? `/api/run/status?runSessionId=${encodeURIComponent(runSessionId)}`
      : "/api/run/status";
    const data = await fetchApiJson(path);
    applyRunStatusToUi(data);
    renderDbSyncState();
    return data;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось получить статус запуска: ${error?.message || "ошибка backend"}`);
    }
    return null;
  }
}

async function refreshTemplatesFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const data = await fetchApiJson("/api/templates");
    state.templates = Array.isArray(data) ? data.map(mapTemplateDtoToUi) : [];
    renderTemplates();
    renderTemplateEditorState();
    renderQueue();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить шаблоны: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshManualPresetsFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const data = await fetchApiJson("/api/manual-presets");
    state.manualReplyPresets = Array.isArray(data) ? data.map(mapManualPresetDtoToUi) : [];
    ensureManualPresetSelection();
    renderManualPresetManager();
    renderChat();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить типовые ответы: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshChannelsFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const data = await fetchApiJson("/api/channels");
    state.channels = Array.isArray(data) ? data.map(mapChannelDtoToUi) : [];
    syncChannelAlertFlags();
    renderChannels();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить Android-каналы: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshAlertsFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const status = state.alertView === "all" ? "all" : state.alertView;
    const data = await fetchApiJson(`/api/alerts?status=${encodeURIComponent(status)}&limit=500&offset=0`);
    const rows = Array.isArray(data?.items) ? data.items : [];
    state.alerts = rows.map(mapAlertDtoToUi);
    syncChannelAlertFlags();
    renderAlerts();
    renderChannels();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить уведомления: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshReportsFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const data = await fetchApiJson("/api/reports/weekly");
    state.reportSentToday = Number(data?.sentToday ?? 0);
    state.reportFailedToday = Number(data?.failedToday ?? 0);
    state.reportWeeklyDays = Array.isArray(data?.days)
      ? data.days.map((day) => ({
          label: String(day?.label || ""),
          sent: Number(day?.sent ?? 0),
          failed: Number(day?.failed ?? 0),
          total: Number(day?.total ?? (Number(day?.sent ?? 0) + Number(day?.failed ?? 0)))
        }))
      : [];

    if (Number.isFinite(Number(data?.stopListCount))) {
      $("rStopCount").textContent = String(Number(data.stopListCount));
    }

    renderBars();
    updateMetrics();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить отчеты: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshStopListFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const data = await fetchApiJson("/api/stop-list?activeOnly=true&limit=5000&offset=0");
    const rows = Array.isArray(data) ? data : [];
    state.stoplist = rows.map(mapStopListDtoToUi);
    renderStopList();
    renderDialogs();
    renderQueue();
    renderClientsDb();
    updateMetrics();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить стоп-лист: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshClientsSnapshotFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const status = await fetchApiJson("/api/clients/sync-status");
    state.clientsDb.actualized = Boolean(status?.hasSnapshot);
    state.clientsDb.snapshotId = status?.hasSnapshot ? Number(status.snapshotId) : null;
    state.clientsDb.rows = Number(status?.totalRows || 0);
    state.clientsDb.syncedAt = status?.hasSnapshot ? toMskDateTimeOrEmpty(status.createdAtUtc) : null;
    state.clientsDb.sourceMode = status?.hasSnapshot ? String(status.sourceMode || "").trim().toLowerCase() : "";

    if (status?.hasSnapshot) {
      const list = await fetchApiJson(`/api/clients?snapshotId=${encodeURIComponent(status.snapshotId)}&limit=5000&offset=0`);
      const items = Array.isArray(list?.items) ? list.items : [];
      state.clients = items.map(mapClientDtoToUi);
    } else {
      state.clients = [];
      state.queue = [];
      state.selectedClients.clear();
      state.selectedQueue.clear();
      state.planPrepared = false;
      state.planStale = false;
      state.queueSessionId = null;
      state.queueSession = null;
      resetDialogPreview();
      state.clientsDb.sourceMode = "";
    }

    renderClientsDb();
    updateMetrics();
    renderDbSyncState();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить актуальную базу клиентов: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshQueueFromBackend(options = {}) {
  const { silent = false, runSessionId = null } = options;
  try {
    const path = runSessionId
      ? `/api/queue?runSessionId=${encodeURIComponent(runSessionId)}&limit=5000&offset=0`
      : "/api/queue?limit=5000&offset=0";
    const data = await fetchApiJson(path);
    if (!data?.hasSession) {
      state.queue = [];
      state.selectedQueue.clear();
      state.planPrepared = false;
      state.queueSessionId = null;
      state.queueSession = null;
      resetDialogPreview();
      renderQueue();
      renderDialogPreviewPanel();
      updateMetrics();
      renderDbSyncState();
      return true;
    }

    state.queueSessionId = Number(data.session?.id || 0);
    state.queueSession = data.session || null;
    state.queue = (Array.isArray(data.items) ? data.items : []).map(mapQueueJobDtoToUi);
    state.planPrepared = Number(data.totalJobsInSession || 0) > 0;
    syncDialogPreviewWithQueue();
    renderQueue();
    renderDialogPreviewPanel();
    updateMetrics();
    renderDbSyncState();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить очередь: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function refreshRunForecastFromBackend(options = {}) {
  const { silent = true } = options;
  state.runForecastRequestSeq += 1;
  const requestSeq = state.runForecastRequestSeq;

  if (!state.clientsDb.actualized || state.clientsDb.syncing) {
    state.runForecast = null;
    renderRunForecast();
    return false;
  }

  if (!hasExpectedSnapshotModeLoaded()) {
    state.runForecast = null;
    renderRunForecast();
    return false;
  }

  if (!state.runRuntime && !hasAnyRunFilterSelected()) {
    state.runForecast = null;
    renderRunForecast();
    return false;
  }

  try {
    const payload = buildQueueFilterPayload();
    const data = await fetchApiJson("/api/queue/forecast", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    if (requestSeq !== state.runForecastRequestSeq) return false;
    state.runForecast = data || null;
    renderRunForecast();
    return true;
  } catch (error) {
    if (requestSeq !== state.runForecastRequestSeq) return false;
    state.runForecast = null;
    renderRunForecast();
    if (!silent) {
      toast(`Не удалось рассчитать прогноз: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function syncClientsDatabase() {
  if (state.runRuntime) {
    toast("Остановите цикл перед актуализацией базы клиентов");
    return;
  }
  if (state.clientsDb.syncing) return;

  state.clientsDb.syncing = true;
  renderDbSyncState();
  renderRunForecast();
  addRunLog("Запущена актуализация базы клиентов: чтение таблицы Rocketman (без захода в карточки).");

  try {
    const result = await fetchApiJson("/api/clients/sync", { method: "POST" });
    state.queue = [];
    state.excludedClientIds = new Set();
    state.selectedClients.clear();
    state.selectedQueue.clear();
    state.planPrepared = false;
    state.planStale = false;
    state.queueSessionId = null;
    state.queueSession = null;
    resetDialogPreview();
    state.runForecast = null;

    await refreshClientsSnapshotFromBackend({ silent: true });
    await refreshDialogsFromBackend({ silent: true });
    renderQueue();
    await refreshRunForecastFromBackend({ silent: true });
    addRunLog(`Актуализация завершена: получено ${result?.totalRows ?? state.clientsDb.rows} клиентов из таблицы. Сформируйте плановую очередь по фильтрам.`);
    toast("База клиентов актуализирована");
  } catch (error) {
    toast(`Не удалось выполнить актуализацию базы: ${error?.message || "ошибка backend"}`);
    addRunLog(`Ошибка актуализации базы клиентов: ${error?.message || "backend error"}.`);
  } finally {
    state.clientsDb.syncing = false;
    renderDbSyncState();
  }
}

async function startRun() {
  if (state.runRuntime) return;
  if (!state.clientsDb.actualized) {
    toast("Сначала выполните «Актуализировать базу клиентов»");
    return;
  }
  if (!hasExpectedSnapshotModeLoaded()) {
    toast("Для LIVE-режима сначала выполните актуализацию из Rocketman");
    return;
  }
  if (state.clientsDb.syncing) {
    toast("Дождитесь завершения актуализации базы клиентов");
    return;
  }
  if (!state.planPrepared) {
    toast("Сначала сформируйте плановую очередь по фильтрам");
    return;
  }
  if (state.planStale) {
    toast("Фильтры изменены. Обновите плановую очередь перед запуском");
    return;
  }
  if (state.queue.length === 0) {
    toast("Плановая очередь пуста");
    return;
  }
  if (!state.runCanStart) {
    toast(state.runStartBlockMessage || "Запуск сейчас недоступен");
    return;
  }
  const previewEditsOk = await ensureDialogPreviewEditsBeforeRun();
  if (!previewEditsOk) {
    return;
  }

  try {
    const payload = state.queueSessionId ? { runSessionId: state.queueSessionId } : {};
    const result = await fetchApiJson("/api/run/start", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    if (result?.status) {
      applyRunStatusToUi(result.status);
    } else {
      await refreshRunStatusFromBackend({ silent: true });
    }
    await refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId || null });
    await refreshRunForecastFromBackend({ silent: true });
    startRunPolling();
    renderRunFilterSummary();
    renderDbSyncState();
    addRunLog(result?.message || `Запуск дневного цикла. Сессия #${state.queueSessionId}.`);
  } catch (error) {
    toast(`Не удалось запустить цикл: ${error?.message || "ошибка backend"}`);
  }
}

async function stopRun(reason = "Остановка по запросу оператора.") {
  if (!state.runRuntime) return;
  try {
    const payload = {
      runSessionId: state.queueSessionId || null,
      reason
    };
    const result = await fetchApiJson("/api/run/stop", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    stopRunPolling();
    if (result?.status) {
      applyRunStatusToUi(result.status);
    } else {
      await refreshRunStatusFromBackend({ silent: true });
    }
    await refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId || null });
    await refreshRunForecastFromBackend({ silent: true });
    addRunLog(result?.message || reason);
  } catch (error) {
    toast(`Не удалось остановить цикл: ${error?.message || "ошибка backend"}`);
  } finally {
    renderDbSyncState();
  }
}

async function switchTab(tabId, options = {}) {
  const { skipUnsavedGuard = false, actionLabel = "перейти в другую вкладку" } = options;
  const activeTabBtn = document.querySelector(".tab-btn.active");
  const currentTabId = activeTabBtn ? activeTabBtn.dataset.tab : null;
  if (!skipUnsavedGuard && currentTabId && currentTabId !== tabId) {
    const confirmed = await resolveUnsavedChangesBeforeNavigation({ actionLabel, includeSettings: true });
    if (!confirmed) return false;
  }
  document.querySelectorAll(".tab-btn").forEach((b) => b.classList.toggle("active", b.dataset.tab === tabId));
  document.querySelectorAll(".tab-screen").forEach((s) => s.classList.toggle("active", s.id === `tab-${tabId}`));
  const [title, subtitle] = titleMap[tabId];
  $("screenTitle").textContent = title;
  $("screenSubtitle").textContent = subtitle;
  if (tabId === "dialogs") {
    void refreshDialogsFromBackend({ silent: true }).then(() => {
      const selected = state.dialogs.find((dialog) => String(dialog.id) === String(state.selectedDialogId));
      if (selected) {
        state.dialogForceScrollPhone = normalizePhone(selected.phone);
        renderDialogs();
      }
    });
  }
  if (tabId === "run") {
    void refreshRunForecastFromBackend({ silent: true });
  }
  return true;
}

async function saveSettings(options = {}) {
  const { silent = false } = options;
  const draft = readSettingsDraftFromUI();
  const payload = {
    loginUrl: draft.loginUrl,
    login: draft.login,
    password: draft.password,
    gap: Math.max(1, Math.round(Number(draft.gap) || 8)),
    allowLiveDispatch: draft.allowLiveDispatch !== false,
    workWindowStart: draft.workWindowStart,
    workWindowEnd: draft.workWindowEnd,
    commentRules: {
      sms2: draft.commentRules.sms2,
      sms3: draft.commentRules.sms3,
      ka1: draft.commentRules.ka1,
      kaN: draft.commentRules.kaN,
      kaFinal: draft.commentRules.kaFinal
    }
  };

  try {
    const saved = await fetchApiJson("/api/settings", {
      method: "PUT",
      body: JSON.stringify(payload)
    });

    applySettingsDraftToUI({
      loginUrl: saved.loginUrl,
      login: saved.login,
      password: saved.password,
      gap: saved.gap,
      allowLiveDispatch: saved.allowLiveDispatch,
      workWindowStart: saved.workWindowStart,
      workWindowEnd: saved.workWindowEnd,
      commentRules: saved.commentRules
    });
    if (saved.commentRules) {
      state.commentRules = { ...state.commentRules, ...saved.commentRules };
    }
    setSettingsBaselineFromUI();
    await refreshRunStatusFromBackend({ silent: true });
    void refreshRunForecastFromBackend({ silent: true });
    if (!silent) {
      toast("Настройки сохранены");
    }
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось сохранить настройки: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function loadSettings() {
  try {
    const data = await fetchApiJson("/api/settings");
    applySettingsDraftToUI({
      loginUrl: data.loginUrl,
      login: data.login,
      password: data.password,
      gap: data.gap,
      allowLiveDispatch: data.allowLiveDispatch,
      workWindowStart: data.workWindowStart,
      workWindowEnd: data.workWindowEnd,
      commentRules: data.commentRules
    });
    if (data.commentRules) {
      state.commentRules = { ...state.commentRules, ...data.commentRules };
    }
  } catch (error) {
    addRunLog(`Не удалось загрузить настройки из backend: ${error?.message || "ошибка"}. Используются текущие значения формы.`);
    toast("Не удалось загрузить настройки из backend");
  }

  setSettingsBaselineFromUI();
  renderManualPresetManager();
}

function toast(text) {
  UI.toast("toast", text);
}

function resetRunFilters() {
  document.querySelectorAll(".run-tz").forEach((n) => { n.checked = true; });
  document.querySelectorAll(".run-overdue").forEach((n) => { n.checked = true; });
  $("runExactDay").value = "";
  refreshRunFiltersUI();
  toast("Фильтр сброшен");
}

function clearExactDay() {
  $("runExactDay").value = "";
}

function openChannelForm() {
  state.channelFormMode = "create";
  state.channelEditId = null;
  $("channelFormTitle").textContent = "Новый Android-канал";
  $("saveChannel").textContent = "Сохранить канал";
  $("channelFormWrap").classList.remove("hidden");
  $("newChannelName").focus();
}

function openChannelEditForm(channelId) {
  const channel = senderById(channelId);
  if (!channel) return;
  state.channelFormMode = "edit";
  state.channelEditId = channel.id;
  $("channelFormTitle").textContent = `Редактирование канала: ${channel.name}`;
  $("saveChannel").textContent = "Сохранить изменения";
  $("channelFormWrap").classList.remove("hidden");
  $("newChannelName").value = channel.name || "";
  $("newChannelSim").value = channel.simPhone || "";
  $("newChannelEndpoint").value = channel.endpoint || "";
  $("newChannelToken").value = "";
  $("newChannelName").focus();
}

function closeChannelForm() {
  state.channelFormMode = "create";
  state.channelEditId = null;
  $("channelFormTitle").textContent = "Новый Android-канал";
  $("saveChannel").textContent = "Сохранить канал";
  $("channelFormWrap").classList.add("hidden");
  $("newChannelName").value = "";
  $("newChannelSim").value = "";
  $("newChannelEndpoint").value = "";
  $("newChannelToken").value = "";
}

async function saveChannelFromForm() {
  const mode = state.channelFormMode;
  const editId = state.channelEditId;
  const name = $("newChannelName").value.trim();
  const simPhone = $("newChannelSim").value.trim();
  const endpoint = $("newChannelEndpoint").value.trim();
  const token = $("newChannelToken").value.trim();
  if (!name || !endpoint) {
    toast("Заполните название и endpoint");
    return;
  }
  if (mode === "create" && !token) {
    toast("Для нового канала обязательно укажите токен");
    return;
  }
  try {
    if (mode === "edit" && editId) {
      await fetchApiJson(`/api/channels/${encodeURIComponent(editId)}`, {
        method: "PUT",
        body: JSON.stringify({ name, endpoint, token, simPhone })
      });
    } else {
      await fetchApiJson("/api/channels", {
        method: "POST",
        body: JSON.stringify({ name, endpoint, token, simPhone })
      });
    }
    closeChannelForm();
    await refreshChannelsFromBackend({ silent: true });
    if (mode === "edit") {
      toast(token ? "Канал обновлен, токен сохранен" : "Канал обновлен");
    } else {
      toast("Канал добавлен");
    }
  } catch (error) {
    const action = mode === "edit" ? "обновить" : "добавить";
    toast(`Не удалось ${action} канал: ${error?.message || "ошибка backend"}`);
  }
}

async function checkChannelById(channelId, options = {}) {
  const { silent = false } = options;
  const channel = senderById(channelId);
  if (!channel) return false;
  try {
    const result = await fetchApiJson(`/api/channels/${encodeURIComponent(channelId)}/check?timeoutMs=5000`, {
      method: "POST"
    });
    await refreshChannelsFromBackend({ silent: true });
    await refreshAlertsFromBackend({ silent: true });
    if (result?.status === "online") {
      if (!silent) toast(`${channel.name}: канал работает`);
      return true;
    }
    if (result?.status === "offline") {
      if (!silent) toast(`${channel.name}: канал отключен вручную`);
      return false;
    }
    if (!silent) toast(`${channel.name}: канал не отвечает, проверьте устройство`);
    return false;
  } catch (error) {
    if (!silent) toast(`${channel.name}: ошибка проверки канала (${error?.message || "backend"})`);
    return false;
  } finally {
    renderQueue();
    void refreshRunForecastFromBackend({ silent: true });
  }
}

async function setChannelManualStatus(channelId, targetStatus) {
  const channel = senderById(channelId);
  if (!channel) return false;

  try {
    await fetchApiJson(`/api/channels/${encodeURIComponent(channelId)}/status`, {
      method: "PATCH",
      body: JSON.stringify({ status: targetStatus })
    });
    await refreshChannelsFromBackend({ silent: true });
    await refreshAlertsFromBackend({ silent: true });
    await refreshRunForecastFromBackend({ silent: true });
    renderQueue();

    if (targetStatus === "offline") {
      if (state.runRuntime) {
        addRunLog(`${channel.name}: канал отключен оператором.`);
      }
      toast(`${channel.name}: канал отключен`);
    } else {
      if (state.runRuntime) {
        addRunLog(`${channel.name}: канал включен оператором.`);
      }
      toast(`${channel.name}: канал включен`);
    }
    return true;
  } catch (error) {
    toast(`Не удалось изменить статус канала: ${error?.message || "ошибка backend"}`);
    return false;
  }
}

async function checkChannels() {
  try {
    const result = await fetchApiJson("/api/channels/check?timeoutMs=5000", { method: "POST" });
    await refreshChannelsFromBackend({ silent: true });
    await refreshAlertsFromBackend({ silent: true });
    updateMetrics();
    const online = Number(result?.online || 0);
    const total = Number(result?.total || state.channels.length);
    toast(`Проверка каналов завершена: ${online}/${total} каналов онлайн`);
  } catch (error) {
    toast(`Не удалось проверить каналы: ${error?.message || "ошибка backend"}`);
  }
}

async function deleteSelectedDialog() {
  const dialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
  if (!dialog) {
    toast("Сначала выберите диалог");
    return;
  }
  const confirmed = window.confirm(`Удалить диалог клиента ${dialog.fio} (${dialog.phone})? Это действие необратимо.`);
  if (!confirmed) return;

  try {
    const apiPhone = toApiPhone(dialog.phone);
    await fetchApiJson(`/api/dialogs/by-phone/${encodeURIComponent(apiPhone)}`, { method: "DELETE" });
    clearDraftSaveTimer(dialog.phone);
    delete state.dialogLastRenderedMessageByPhone[normalizePhone(dialog.phone)];
    if (String(state.selectedDialogId) === String(dialog.id)) {
      state.selectedDialogId = null;
    }
    await refreshDialogsFromBackend({ silent: true });
    toast("Диалог удален");
    addRunLog(`Диалог удален: ${dialog.phone}`);
  } catch (error) {
    toast(`Не удалось удалить диалог: ${error?.message || "ошибка backend"}`);
  }
}

async function pruneDialogsOlderThanDays() {
  const daysRaw = Number($("dialogPruneDays").value);
  if (!Number.isFinite(daysRaw) || daysRaw < 1) {
    toast("Укажите корректный срок в днях (от 1)");
    return;
  }
  const days = Math.floor(daysRaw);
  const confirmed = window.confirm(`Удалить сообщения в диалогах старше ${days} дн.? Это действие необратимо.`);
  if (!confirmed) return;

  try {
    const result = await fetchApiJson("/api/dialogs/prune", {
      method: "POST",
      body: JSON.stringify({ olderThanDays: days })
    });
    await refreshDialogsFromBackend({ silent: true });
    const deleted = Number(result?.deletedMessages || 0);
    toast(deleted > 0 ? `Удалено сообщений: ${deleted}` : `Сообщений старше ${days} дн. не найдено`);
    addRunLog(`Очистка диалогов старше ${days} дн.: удалено сообщений ${deleted}.`);
  } catch (error) {
    toast(`Не удалось очистить старые диалоги: ${error?.message || "ошибка backend"}`);
  }
}

function refreshAfterStopListChange() {
  renderStopList();
  renderDialogs();
  refreshPlanningViews();
}

async function addPhoneToStopList(phone, reason, source, options = {}) {
  const { silent = false, deferRender = false } = options;
  const normalized = normalizePhone(phone);
  if (!normalized) {
    if (!silent) toast("Введите номер телефона");
    return false;
  }
  if (state.stoplist.some((s) => normalizePhone(s.phone) === normalized)) {
    if (!silent) toast("Номер уже в стоп-листе");
    return false;
  }

  try {
    const created = await fetchApiJson("/api/stop-list", {
      method: "POST",
      body: JSON.stringify({
        phone: normalized,
        reason: reason || "",
        source: source || "manual",
        isActive: true
      })
    });
    if (deferRender) {
      state.stoplist.unshift(mapStopListDtoToUi(created));
    } else {
      await refreshStopListFromBackend({ silent: true });
    }
    if (!state.runRuntime) {
      const removedIds = state.queue
        .filter((q) => normalizePhone(q.phone) === normalized)
        .map((q) => q.clientId);
      removedIds.forEach((id) => state.excludedClientIds.add(id));
      state.queue = state.queue.filter((q) => normalizePhone(q.phone) !== normalized);
    }
    if (!deferRender) {
      refreshAfterStopListChange();
    }
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось добавить в стоп-лист: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function removePhoneFromStopList(phone, options = {}) {
  const { deferRender = false, silent = false } = options;
  const normalized = normalizePhone(phone);
  if (!normalized) return false;
  try {
    await fetchApiJson(`/api/stop-list/by-phone/${encodeURIComponent(normalized)}`, {
      method: "DELETE"
    });
    if (deferRender) {
      state.stoplist = state.stoplist.filter((x) => normalizePhone(x.phone) !== normalized);
    } else {
      await refreshStopListFromBackend({ silent: true });
    }
    if (state.planPrepared && !state.runRuntime) {
      state.planStale = true;
    }
    if (!deferRender) {
      refreshAfterStopListChange();
    }
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось убрать из стоп-листа: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

function setVisibleClientSelection(checked) {
  getVisibleClientRows().forEach((client) => {
    if (checked) {
      state.selectedClients.add(client.id);
    } else {
      state.selectedClients.delete(client.id);
    }
  });
  renderClientsDb();
}

function setAllClientSelection() {
  state.clients.forEach((client) => {
    state.selectedClients.add(client.id);
  });
  renderClientsDb();
}

function setVisibleQueueSelection(checked) {
  getVisibleQueueRows().forEach((job) => {
    if (checked) {
      state.selectedQueue.add(job.id);
    } else {
      state.selectedQueue.delete(job.id);
    }
  });
  renderQueue();
}

function setAllQueueSelection() {
  state.queue.forEach((job) => {
    state.selectedQueue.add(job.id);
  });
  renderQueue();
}

function setVisibleStopSelection(checked) {
  getVisibleStopRows().forEach((entry) => {
    if (checked) {
      state.selectedStopList.add(entry.id);
    } else {
      state.selectedStopList.delete(entry.id);
    }
  });
  renderStopList();
}

function setAllStopSelection() {
  state.stoplist.forEach((entry) => {
    state.selectedStopList.add(entry.id);
  });
  renderStopList();
}

function clearClientsSelection() {
  state.selectedClients.clear();
  renderClientsDb();
}

function clearQueueSelection() {
  state.selectedQueue.clear();
  renderQueue();
}

function clearStopSelection() {
  state.selectedStopList.clear();
  renderStopList();
}

async function bulkAddSelectedClientsToStopList() {
  const selected = getSelectedClientRows();
  if (selected.length === 0) {
    toast("Сначала выберите клиентов в таблице");
    return;
  }
  const phones = [...new Set(selected.map((c) => normalizePhone(c.phone)).filter(Boolean))];
  if (phones.length === 0) {
    toast("У выбранных клиентов нет валидных телефонов");
    return;
  }
  try {
    const result = await fetchApiJson("/api/stop-list/bulk/add", {
      method: "POST",
      body: JSON.stringify({
        phones,
        reason: "Добавлено массово из базы клиентов",
        source: "База клиентов (массово)"
      })
    });
    const added = Number(result?.added ?? 0);
    if (!state.runRuntime && added > 0) {
      phones.forEach((normPhone) => {
        const removedIds = state.queue
          .filter((q) => normalizePhone(q.phone) === normPhone)
          .map((q) => q.clientId);
        removedIds.forEach((id) => state.excludedClientIds.add(id));
      });
      state.queue = state.queue.filter((q) => !phones.includes(normalizePhone(q.phone)));
    }
    await refreshStopListFromBackend({ silent: true });
    refreshAfterStopListChange();
    state.selectedClients.clear();
    renderClientsDb();
    toast(added > 0 ? `В стоп-лист добавлено: ${added}` : "Выбранные номера уже в стоп-листе");
  } catch (error) {
    toast(`Не удалось добавить в стоп-лист: ${error?.message || "ошибка backend"}`);
  }
}

async function bulkRemoveSelectedClientsFromStopList() {
  const selected = getSelectedClientRows();
  if (selected.length === 0) {
    toast("Сначала выберите клиентов в таблице");
    return;
  }
  const phones = [...new Set(selected.map((c) => normalizePhone(c.phone)).filter(Boolean))];
  if (phones.length === 0) {
    toast("У выбранных клиентов нет валидных телефонов");
    return;
  }
  try {
    const result = await fetchApiJson("/api/stop-list/bulk/remove", {
      method: "POST",
      body: JSON.stringify({ phones })
    });
    const removed = Number(result?.removed ?? 0);
    await refreshStopListFromBackend({ silent: true });
    if (state.planPrepared && !state.runRuntime && removed > 0) {
      state.planStale = true;
    }
    refreshAfterStopListChange();
    state.selectedClients.clear();
    renderClientsDb();
    toast(removed > 0 ? `Из стоп-листа удалено: ${removed}` : "Выбранные номера не найдены в стоп-листе");
  } catch (error) {
    toast(`Не удалось убрать из стоп-листа: ${error?.message || "ошибка backend"}`);
  }
}

function bulkReturnSelectedClientsToPlan() {
  if (state.runRuntime) {
    toast("Во время запуска нельзя менять план вручную");
    return;
  }
  const selected = getSelectedClientRows();
  if (selected.length === 0) {
    toast("Сначала выберите клиентов в таблице");
    return;
  }
  let changed = 0;
  selected.forEach((client) => {
    if (!state.excludedClientIds.has(client.id)) return;
    state.excludedClientIds.delete(client.id);
    changed += 1;
  });
  if (changed === 0) {
    toast("Среди выбранных клиентов нет исключенных из плана");
    return;
  }
  if (state.planPrepared) {
    void rebuildPlannedQueue(false);
  } else {
    refreshPlanningViews();
  }
  state.selectedClients.clear();
  renderClientsDb();
  toast(`В план возвращено: ${changed}`);
}

async function bulkAddSelectedQueueToStopList() {
  const selected = getSelectedQueueRows();
  if (selected.length === 0) {
    toast("Сначала выберите клиентов в очереди");
    return;
  }
  const phones = [...new Set(selected.map((j) => normalizePhone(j.phone)).filter(Boolean))];
  if (phones.length === 0) {
    toast("У выбранных клиентов нет валидных телефонов");
    return;
  }
  try {
    const result = await fetchApiJson("/api/stop-list/bulk/add", {
      method: "POST",
      body: JSON.stringify({
        phones,
        reason: "Добавлено массово из очереди",
        source: "Очередь (массово)"
      })
    });
    const added = Number(result?.added ?? 0);
    if (!state.runRuntime && added > 0) {
      phones.forEach((normPhone) => {
        const removedIds = state.queue
          .filter((q) => normalizePhone(q.phone) === normPhone)
          .map((q) => q.clientId);
        removedIds.forEach((id) => state.excludedClientIds.add(id));
      });
      state.queue = state.queue.filter((q) => !phones.includes(normalizePhone(q.phone)));
    }
    await refreshStopListFromBackend({ silent: true });
    refreshAfterStopListChange();
    state.selectedQueue.clear();
    renderQueue();
    toast(added > 0 ? `В стоп-лист добавлено: ${added}` : "Выбранные номера уже в стоп-листе");
  } catch (error) {
    toast(`Не удалось добавить в стоп-лист: ${error?.message || "ошибка backend"}`);
  }
}

async function bulkRemoveSelectedQueueFromStopList() {
  const selected = getSelectedQueueRows();
  if (selected.length === 0) {
    toast("Сначала выберите клиентов в очереди");
    return;
  }
  const phones = [...new Set(selected.map((j) => normalizePhone(j.phone)).filter(Boolean))];
  if (phones.length === 0) {
    toast("У выбранных клиентов нет валидных телефонов");
    return;
  }
  try {
    const result = await fetchApiJson("/api/stop-list/bulk/remove", {
      method: "POST",
      body: JSON.stringify({ phones })
    });
    const removed = Number(result?.removed ?? 0);
    await refreshStopListFromBackend({ silent: true });
    if (state.planPrepared && !state.runRuntime && removed > 0) {
      state.planStale = true;
    }
    refreshAfterStopListChange();
    state.selectedQueue.clear();
    renderQueue();
    toast(removed > 0 ? `Из стоп-листа удалено: ${removed}` : "Выбранные номера не найдены в стоп-листе");
  } catch (error) {
    toast(`Не удалось убрать из стоп-листа: ${error?.message || "ошибка backend"}`);
  }
}

async function bulkRemoveSelectedStopListEntries() {
  const selected = getSelectedStopRows();
  if (selected.length === 0) {
    toast("Сначала выберите записи в стоп-листе");
    return;
  }

  const confirmed = window.confirm(`Убрать из стоп-листа выбранные записи (${selected.length})?`);
  if (!confirmed) return;

  const ids = selected.map((e) => Number(e.id)).filter((id) => Number.isFinite(id) && id > 0);
  if (ids.length === 0) {
    toast("Не удалось определить id выбранных записей");
    return;
  }

  try {
    const result = await fetchApiJson("/api/stop-list/bulk/deactivate", {
      method: "POST",
      body: JSON.stringify({ ids })
    });
    const deactivated = Number(result?.deactivated ?? 0);
    state.selectedStopList.clear();
    await refreshStopListFromBackend({ silent: true });
    if (state.planPrepared && !state.runRuntime && deactivated > 0) {
      state.planStale = true;
    }
    refreshAfterStopListChange();
    toast(deactivated > 0 ? `Из стоп-листа удалено: ${deactivated}` : "Выбранные записи не найдены в стоп-листе");
  } catch (error) {
    toast(`Не удалось убрать из стоп-листа: ${error?.message || "ошибка backend"}`);
  }
}

async function removeQueueJobsFromPlan(jobIds, options = {}) {
  const { silent = false } = options;
  const uniqueIds = Array.from(new Set((jobIds || [])
    .map((id) => Number(id))
    .filter((id) => Number.isFinite(id) && id > 0)));
  if (uniqueIds.length === 0) {
    return { removed: 0, skipped: 0, remainingJobs: state.queue.length, runSessionId: state.queueSessionId || 0 };
  }

  const jobClientIds = new Map(
    state.queue
      .filter((q) => uniqueIds.includes(q.id))
      .map((q) => [q.id, q.clientId])
  );

  try {
    const payload = { jobIds: uniqueIds };
    if (state.queueSessionId) {
      payload.runSessionId = state.queueSessionId;
    }
    const result = await fetchApiJson("/api/queue/jobs/remove", {
      method: "POST",
      body: JSON.stringify(payload)
    });

    uniqueIds.forEach((jobId) => {
      const clientId = jobClientIds.get(jobId);
      if (clientId != null) {
        state.excludedClientIds.add(clientId);
      }
    });

    const resolvedRunSessionId = Number(result?.runSessionId || state.queueSessionId || 0) || null;
    await refreshQueueFromBackend({ silent: true, runSessionId: resolvedRunSessionId });
    await refreshRunStatusFromBackend({ silent: true, runSessionId: resolvedRunSessionId });
    renderClientsDb();
    refreshPlanningViews();

    return {
      removed: Number(result?.removed ?? 0),
      skipped: Number(result?.skipped ?? 0),
      remainingJobs: Number(result?.remainingJobs ?? state.queue.length),
      runSessionId: Number(result?.runSessionId ?? state.queueSessionId ?? 0)
    };
  } catch (error) {
    if (!silent) {
      toast(`Не удалось удалить клиентов из плановой очереди: ${error?.message || "ошибка backend"}`);
    }
    return null;
  }
}

function buildDebtToastText(debt) {
  if (!debt) return "Сумма долга обновлена.";
  const exact = String(debt.exactTotalRaw || "").trim();
  const approx = String(debt.approxTotalText || "").trim();
  const updated = debt.updatedAtUtc ? formatMskDateTime(safeDateMs(debt.updatedAtUtc)) : "";
  const parts = [];
  if (exact) parts.push(`Точно: ${exact}`);
  if (approx) parts.push(`По формуле: ${approx}`);
  if (updated) parts.push(`Обновлено: ${updated}`);
  return parts.length > 0 ? parts.join(" | ") : "Сумма долга обновлена.";
}

async function fetchDebtByExternalClientId(externalClientId, options = {}) {
  const { silent = false } = options;
  const normalized = String(externalClientId || "").trim();
  if (!normalized) {
    if (!silent) {
      toast("Невозможно определить externalClientId клиента");
    }
    return null;
  }

  try {
    const result = await fetchApiJson(`/api/clients/${encodeURIComponent(normalized)}/debt/fetch`, {
      method: "POST",
      body: JSON.stringify({
        timeoutMs: 30000,
        headed: false
      })
    });

    await refreshClientsSnapshotFromBackend({ silent: true });
    await refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId || null });
    renderDialogs();
    renderChat();
    if (!silent) {
      toast(buildDebtToastText(result?.debt));
    }
    return result;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось получить сумму долга: ${error?.message || "ошибка backend"}`);
    }
    return null;
  }
}

async function bulkRemoveSelectedFromPlan() {
  if (state.runRuntime) {
    toast("Во время запуска нельзя менять план вручную");
    return;
  }
  const selected = getSelectedQueueRows();
  if (selected.length === 0) {
    toast("Сначала выберите клиентов в очереди");
    return;
  }
  const removableIds = selected
    .filter((q) => ["queued", "retry"].includes(q.status))
    .map((q) => q.id);
  const removableJobIds = new Set(
    selected
      .filter((q) => ["queued", "retry"].includes(q.status))
      .map((q) => q.id)
  );
  if (removableIds.length === 0) {
    toast("Среди выбранных нет задач, доступных для удаления из плана");
    return;
  }
  const result = await removeQueueJobsFromPlan(Array.from(removableJobIds), { silent: true });
  if (!result) return;
  state.selectedQueue.clear();
  if (result.removed > 0) {
    toast(`Из плана удалено: ${result.removed}`);
    return;
  }
  toast("Не удалось удалить выбранные задачи из плана");
}

async function bulkAssignTemplateToQueue() {
  if (state.runRuntime) {
    toast("Во время запуска нельзя менять шаблон в очереди");
    return;
  }
  const selected = getSelectedQueueRows();
  if (selected.length === 0) {
    toast("Сначала выберите клиентов в очереди");
    return;
  }
  const templateId = Number($("queueBulkTemplate").value);
  if (!templateId) {
    toast("Выберите шаблон для массового назначения");
    return;
  }
  const template = getTemplateById(templateId);
  if (!template) {
    toast("Выбранный шаблон не найден");
    return;
  }
  const removableJobIds = selected
    .filter((q) => ["queued", "retry"].includes(q.status))
    .filter((q) => canApplyTemplateToOverdue(template, q.daysOverdue))
    .map((q) => q.id);
  if (removableJobIds.length === 0) {
    toast(`Шаблон «${templateDisplayName(template)}» не подходит выбранным клиентам по просрочке или статусу`);
    return;
  }
  try {
    const payload = { jobIds: removableJobIds, templateId };
    if (state.queueSessionId) {
      payload.runSessionId = state.queueSessionId;
    }
    const result = await fetchApiJson("/api/queue/bulk/set-template", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    const applied = Number(result?.applied ?? 0);
    const skipped = Number(result?.skipped ?? 0);
    await refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId || null });
    state.selectedQueue.clear();
    renderQueue();
    const templateTitle = templateDisplayName(template);
    if (skipped > 0) {
      toast(`Шаблон «${templateTitle}» назначен для ${applied} клиентов, пропущено: ${skipped}`);
    } else {
      toast(`Шаблон «${templateTitle}» назначен для ${applied} клиентов`);
    }
  } catch (error) {
    toast(`Не удалось назначить шаблон: ${error?.message || "ошибка backend"}`);
  }
}

function bindEvents() {
  $("tabs").querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await switchTab(btn.dataset.tab, { actionLabel: `перейти во вкладку «${btn.textContent.trim()}»` });
    });
  });

  UI.bindDelegated("alertList", {
    "alert-resolve": async (btn) => {
      const ok = await updateAlertStatus(Number(btn.dataset.alertId), "resolved");
      if (ok) toast("Уведомление переведено в статус «Решено»");
    },
    "alert-irrelevant": async (btn) => {
      const ok = await updateAlertStatus(Number(btn.dataset.alertId), "irrelevant");
      if (ok) toast("Уведомление переведено в статус «Неактуально»");
    },
    "alert-activate": async (btn) => {
      const ok = await updateAlertStatus(Number(btn.dataset.alertId), "active");
      if (ok) toast("Уведомление снова активно");
    }
  });

  UI.bindDelegated("channelsBody", {
    "channel-edit": (btn) => {
      const id = Number(btn.dataset.chId);
      if (!id) return;
      openChannelEditForm(id);
    },
    "channel-toggle": async (btn) => {
      const id = Number(btn.dataset.chId);
      const nextStatus = String(btn.dataset.nextStatus || "").trim();
      if (!id || !nextStatus) return;
      await setChannelManualStatus(id, nextStatus);
    },
    "channel-check": async (btn) => {
      const id = Number(btn.dataset.chId);
      await checkChannelById(id);
    },
    "channel-delete": async (btn) => {
      const id = Number(btn.dataset.chId);
      try {
        await fetchApiJson(`/api/channels/${encodeURIComponent(id)}`, { method: "DELETE" });
        await refreshChannelsFromBackend({ silent: true });
        renderQueue();
        toast("Канал удален");
      } catch (error) {
        toast(`Не удалось удалить канал: ${error?.message || "ошибка backend"}`);
      }
    }
  });

  UI.bindDelegated("templatesBody", {
    "template-open": async (btn) => {
      const id = Number(btn.dataset.tplId);
      const tpl = getTemplateById(id);
      if (!tpl) return;
      if (state.templateCreateMode || (state.selectedTemplateId !== null && id !== state.selectedTemplateId)) {
        const confirmed = await resolveUnsavedChangesBeforeNavigation({
          actionLabel: "открыть другой шаблон",
          includeTemplate: true,
          includeManualPreset: false
        });
        if (!confirmed) return;
      }
      loadTemplateToEditor(id);
      toast(`Шаблон «${tpl.name}» открыт`);
    }
  });

  UI.bindDelegated("manualPresetBody", {
    "manual-preset-open": async (btn) => {
      const nextId = btn.dataset.presetId || null;
      if (state.manualPresetCreateMode || (nextId && nextId !== state.selectedManualPresetId)) {
        const confirmed = await resolveUnsavedChangesBeforeNavigation({
          actionLabel: "открыть другой типовой ответ",
          includeTemplate: false,
          includeManualPreset: true
        });
        if (!confirmed) return;
      }
      exitManualPresetCreateMode();
      state.selectedManualPresetId = nextId;
      renderManualPresetManager();
    }
  });

  UI.bindDelegated("clientsDbBody", {
    "client-open-dialog": (btn) => {
      void openDialogByPhone(btn.dataset.clientPhone || "");
    },
    "client-fetch-debt": (btn) => {
      const externalClientId = String(btn.dataset.clientExternalId || "").trim();
      if (!externalClientId) {
        toast("Не удалось определить клиента для запроса суммы");
        return;
      }
      void fetchDebtByExternalClientId(externalClientId);
    },
    "client-toggle-stop": async (btn) => {
      const phone = btn.dataset.clientPhone || "";
      if (isPhoneInStopList(phone)) {
        const ok = await removePhoneFromStopList(phone);
        if (!ok) return;
        toast("Клиент убран из стоп-листа");
        return;
      }
      const ok = await addPhoneToStopList(phone, "Добавлено вручную из базы клиентов", "База клиентов");
      if (!ok) return;
      toast("Клиент добавлен в стоп-лист");
    },
    "client-return-plan": (btn) => {
      if (state.runRuntime) {
        toast("Во время запуска нельзя менять план вручную");
        return;
      }
      const id = Number(btn.dataset.clientId);
      state.excludedClientIds.delete(id);
      if (state.planPrepared) {
        void rebuildPlannedQueue(false);
      }
      renderClientsDb();
      toast("Клиент возвращен в план");
    }
  });

  $("clientsDbBody").addEventListener("change", (event) => {
    const checkbox = event.target.closest("[data-action='client-select-row']");
    if (!checkbox) return;
    const clientId = Number(checkbox.dataset.clientId);
    if (!clientId) return;
    if (checkbox.checked) {
      state.selectedClients.add(clientId);
    } else {
      state.selectedClients.delete(clientId);
    }
    renderClientsDb();
  });

  UI.bindDelegated("queueBody", {
    "queue-open-dialog": (btn) => {
      void openDialogByPhone(btn.dataset.qPhone || "");
    },
    "queue-fetch-debt": (btn) => {
      const externalClientId = String(btn.dataset.qExternalId || "").trim();
      if (!externalClientId) {
        toast("Не удалось определить клиента для запроса суммы");
        return;
      }
      void fetchDebtByExternalClientId(externalClientId);
    },
    "queue-toggle-stop": async (btn) => {
      const phone = btn.dataset.qPhone || "";
      if (isPhoneInStopList(phone)) {
        const ok = await removePhoneFromStopList(phone);
        if (!ok) return;
        toast("Клиент убран из стоп-листа");
        return;
      }
      const ok = await addPhoneToStopList(phone, "Добавлено из плановой очереди", "Очередь");
      if (!ok) return;
      toast("Клиент перенесен в стоп-лист");
    },
    "queue-remove-plan": async (btn) => {
      if (state.runRuntime) {
        toast("Во время запуска нельзя менять план вручную");
        return;
      }
      const id = Number(btn.dataset.qId);
      if (!id) return;
      const result = await removeQueueJobsFromPlan([id], { silent: true });
      if (!result) return;
      if (result.removed > 0) {
        toast("Клиент исключен из плановой очереди");
        return;
      }
      toast("Не удалось исключить клиента из плановой очереди");
    }
  });

  $("queueBody").addEventListener("change", (event) => {
    const check = event.target.closest("[data-action='queue-select-row']");
    if (check) {
      const jobId = Number(check.dataset.qId);
      if (!jobId) return;
      if (check.checked) {
        state.selectedQueue.add(jobId);
      } else {
        state.selectedQueue.delete(jobId);
      }
      renderQueue();
      return;
    }

    const select = event.target.closest("[data-action='queue-set-template']");
    if (!select) return;
    if (state.runRuntime) {
      renderQueue();
      toast("Во время запуска нельзя менять шаблон в очереди");
      return;
    }
    const jobId = Number(select.dataset.qId);
    const job = state.queue.find((q) => q.id === jobId);
    if (!job) return;
    const nextTemplateId = Number(select.value);
    const nextTemplate = getTemplateById(nextTemplateId);
    if (!nextTemplate) {
      renderQueue();
      toast("Выбранный шаблон не найден");
      return;
    }
    if (!canApplyTemplateToOverdue(nextTemplate, job.daysOverdue)) {
      renderQueue();
      toast(`Шаблон «${templateDisplayName(nextTemplate)}» не подходит клиенту ${job.phone} по типу просрочки`);
      return;
    }
    job.templateId = nextTemplateId;
    renderQueue();
    toast(`Для клиента ${job.phone} назначен шаблон «${templateDisplayName(nextTemplate)}»`);
  });

  UI.bindDelegated("stopListBody", {
    "stop-remove": async (btn) => {
      const id = Number(btn.dataset.stopId);
      const entry = state.stoplist.find((x) => x.id === id);
      if (!entry) return;
      const ok = await removePhoneFromStopList(entry.phone);
      if (!ok) return;
      toast("Номер удален из стоп-листа");
    }
  });

  $("stopListBody").addEventListener("change", (event) => {
    const check = event.target.closest("[data-action='stop-select-row']");
    if (!check) return;
    const stopId = Number(check.dataset.stopId);
    if (!stopId) return;
    if (check.checked) {
      state.selectedStopList.add(stopId);
    } else {
      state.selectedStopList.delete(stopId);
    }
    renderStopList();
  });

  UI.bindDelegated("dialogList", {
    "dialog-select": (btn) => {
      state.selectedDialogId = String(btn.dataset.dialogId || "");
      const selected = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
      if (selected) {
        state.dialogForceScrollPhone = normalizePhone(selected.phone);
      }
      renderDialogs();
      renderChat();
      if (selected) {
        void refreshDialogMessagesFromBackend(selected.phone, { silent: true });
        void refreshDialogDraftFromBackend(selected.phone, { silent: true, applyToInput: true });
        if (state.dialogPreview.enabled) {
          void rebuildDialogPreviewForSelected({ silent: true });
        } else {
          syncDialogPreviewWithQueue();
          renderDialogPreviewPanel();
        }
      }
    }
  });

  $("runStart").addEventListener("click", () => {
    void startRun();
  });
  $("runSyncDb").addEventListener("click", () => {
    void syncClientsDatabase();
  });
  $("globalStart").addEventListener("click", () => {
    void startRun();
  });
  $("globalStop").addEventListener("click", () => {
    void stopRun();
  });
  $("alertSummary").addEventListener("click", async () => {
    const switched = await switchTab("run", { actionLabel: "перейти к уведомлениям" });
    if (!switched) return;
    requestAnimationFrame(() => {
      const alertsCard = $("alertsCard");
      if (!alertsCard) return;
      alertsCard.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });
  $("openManualPresetManager").addEventListener("click", async () => {
    const presetId = $("manualPreset").value || "";
    if (!presetId) return;
    const switched = await switchTab("settings", { actionLabel: "перейти к редактированию типового ответа" });
    if (!switched) return;
    exitManualPresetCreateMode();
    state.selectedManualPresetId = presetId;
    renderManualPresetManager();
    requestAnimationFrame(() => {
      const editor = $("manualPresetText");
      if (!editor) return;
      editor.scrollIntoView({ behavior: "smooth", block: "center" });
      editor.focus();
      editor.selectionStart = editor.selectionEnd = editor.value.length;
    });
  });
  $("dialogDeleteCurrent").addEventListener("click", deleteSelectedDialog);
  $("dialogPruneOld").addEventListener("click", pruneDialogsOlderThanDays);
  $("dialogPreviewToggle").addEventListener("change", () => {
    const enabled = $("dialogPreviewToggle").checked;
    state.dialogPreview.enabled = enabled;
    if (!enabled) {
      renderDialogPreviewPanel();
      return;
    }
    void rebuildDialogPreviewForSelected({ silent: true });
  });
  $("dialogPreviewEditor").addEventListener("input", () => {
    state.dialogPreview.editorText = $("dialogPreviewEditor").value;
    const baseline = String(state.dialogPreview.text || "").trim();
    const current = String(state.dialogPreview.editorText || "").trim();
    state.dialogPreview.editorDirty = baseline !== current;
    renderDialogPreviewPanel();
  });
  $("dialogPreviewSave").addEventListener("click", () => {
    void saveDialogPreviewOverride();
  });
  $("dialogPreviewReset").addEventListener("click", () => {
    void clearDialogPreviewOverride();
  });
  const runStopBtn = $("runStop");
  if (runStopBtn) {
    runStopBtn.addEventListener("click", () => {
      void stopRun();
    });
  }
  $("runFilterApply").addEventListener("click", () => {
    collectRunFiltersFromUI();
    renderRunFilterSummary();
    void rebuildPlannedQueue(true);
  });
  $("runSelectAll").addEventListener("click", () => {
    setFilterCheckboxes(".run-tz, .run-overdue", true);
    toast("Все фильтры включены");
  });
  $("runClearAll").addEventListener("click", () => {
    document.querySelectorAll(".run-tz, .run-overdue").forEach((n) => { n.checked = false; });
    clearExactDay();
    refreshRunFiltersUI();
    toast("Все чекбоксы фильтров отключены");
  });
  $("runTzSelectAll").addEventListener("click", () => {
    setFilterCheckboxes(".run-tz", true);
  });
  $("runTzClearAll").addEventListener("click", () => {
    setFilterCheckboxes(".run-tz", false);
  });
  $("runOverdueSelectAll").addEventListener("click", () => {
    setFilterCheckboxes(".run-overdue", true);
  });
  $("runOverdueClearAll").addEventListener("click", () => {
    document.querySelectorAll(".run-overdue").forEach((n) => { n.checked = false; });
    clearExactDay();
    refreshRunFiltersUI();
  });
  $("runFilterReset").addEventListener("click", resetRunFilters);
  document.querySelectorAll(".run-tz, .run-overdue").forEach((el) => {
    el.addEventListener("change", () => {
      refreshRunFiltersUI();
    });
  });
  $("runExactDay").addEventListener("input", () => {
    refreshRunFiltersUI();
  });

  if ($("clientsSearch")) {
    $("clientsSearch").addEventListener("input", () => {
      state.clientsViewFilters.search = $("clientsSearch").value || "";
      renderClientsDb();
    });
  }
  if ($("clientsFilterTz")) {
    $("clientsFilterTz").addEventListener("change", () => {
      state.clientsViewFilters.tz = $("clientsFilterTz").value;
      renderClientsDb();
    });
  }
  if ($("clientsFilterOverdue")) {
    $("clientsFilterOverdue").addEventListener("change", () => {
      state.clientsViewFilters.overdue = $("clientsFilterOverdue").value;
      renderClientsDb();
    });
  }
  $("alertViewFilter").addEventListener("change", () => {
    state.alertView = $("alertViewFilter").value;
    void refreshAlertsFromBackend({ silent: true });
  });

  $("saveAll").addEventListener("click", async () => {
    const button = $("saveAll");
    button.disabled = true;
    try {
      await saveSettings();
    } finally {
      button.disabled = false;
    }
  });
  $("cfgPasswordToggle").addEventListener("click", () => {
    const input = $("cfgPassword");
    if (!input) return;
    const showPassword = input.type === "password";
    input.type = showPassword ? "text" : "password";
    $("cfgPasswordToggle").textContent = showPassword ? "Скрыть" : "Показать";
    $("cfgPasswordToggle").setAttribute("aria-pressed", showPassword ? "true" : "false");
  });
  $("cfgGap").addEventListener("input", () => {
    void refreshRunForecastFromBackend({ silent: true });
  });
  $("cfgWorkWindowStart").addEventListener("input", () => {
    void refreshRunForecastFromBackend({ silent: true });
  });
  $("cfgWorkWindowEnd").addEventListener("input", () => {
    void refreshRunForecastFromBackend({ silent: true });
  });
  $("tplType").addEventListener("change", () => {
    ensureTemplateCreateModeFromEditorInput();
    renderTemplateEditorState();
  });
  $("tplName").addEventListener("input", () => {
    ensureTemplateCreateModeFromEditorInput();
    renderTemplateEditorState();
  });
  $("tplText").addEventListener("input", () => {
    ensureTemplateCreateModeFromEditorInput();
    renderTemplateEditorState();
  });
  $("tplNew").addEventListener("click", async () => {
    if (!state.templateCreateMode) {
      const confirmed = await resolveUnsavedChangesBeforeNavigation({
        actionLabel: "создать новый шаблон",
        includeTemplate: true,
        includeManualPreset: false
      });
      if (!confirmed) return;
    }
    createTemplate();
    toast("Режим создания нового шаблона включен");
  });

  $("clientsSelectAllRows").addEventListener("change", () => {
    setVisibleClientSelection($("clientsSelectAllRows").checked);
  });
  $("clientsSelectVisible").addEventListener("click", () => {
    setAllClientSelection();
  });
  $("clientsClearSelection").addEventListener("click", () => {
    clearClientsSelection();
  });
  $("clientsBulkAddStop").addEventListener("click", () => {
    void bulkAddSelectedClientsToStopList();
  });
  $("clientsBulkRemoveStop").addEventListener("click", () => {
    void bulkRemoveSelectedClientsFromStopList();
  });
  $("clientsBulkReturnPlan").addEventListener("click", bulkReturnSelectedClientsToPlan);

  $("queueSelectAllRows").addEventListener("change", () => {
    setVisibleQueueSelection($("queueSelectAllRows").checked);
  });
  $("queueSelectVisible").addEventListener("click", () => {
    setAllQueueSelection();
  });
  $("queueClearSelection").addEventListener("click", () => {
    clearQueueSelection();
  });
  $("queueBulkAddStop").addEventListener("click", () => {
    void bulkAddSelectedQueueToStopList();
  });
  $("queueBulkRemoveStop").addEventListener("click", () => {
    void bulkRemoveSelectedQueueFromStopList();
  });
  $("queueBulkRemovePlan").addEventListener("click", () => {
    void bulkRemoveSelectedFromPlan();
  });
  $("queueBulkSetTemplate").addEventListener("click", bulkAssignTemplateToQueue);

  $("stopSelectAllRows").addEventListener("change", () => {
    setVisibleStopSelection($("stopSelectAllRows").checked);
  });
  $("stopSelectVisible").addEventListener("click", () => {
    setAllStopSelection();
  });
  $("stopClearSelection").addEventListener("click", () => {
    clearStopSelection();
  });
  $("stopBulkRemove").addEventListener("click", () => {
    void bulkRemoveSelectedStopListEntries();
  });

  $("manualPreset").addEventListener("change", async () => {
    const nextId = $("manualPreset").value || null;
    if (nextId !== state.selectedManualPresetId || state.manualPresetCreateMode) {
      const confirmed = await resolveUnsavedChangesBeforeNavigation({
        actionLabel: "выбрать другой типовой ответ",
        includeTemplate: false,
        includeManualPreset: true
      });
      if (!confirmed) {
        renderManualPresetSelect();
        return;
      }
    }
    exitManualPresetCreateMode();
    state.selectedManualPresetId = nextId;
    renderManualPresetManager();
  });
  $("manualPresetTitle").addEventListener("input", renderManualPresetEditorState);
  $("manualPresetText").addEventListener("input", renderManualPresetEditorState);
  $("manualPresetNew").addEventListener("click", async () => {
    if (!state.manualPresetCreateMode) {
      const confirmed = await resolveUnsavedChangesBeforeNavigation({
        actionLabel: "создать новый типовой ответ",
        includeTemplate: false,
        includeManualPreset: true
      });
      if (!confirmed) return;
    }
    createManualPreset();
    toast("Режим создания нового типового ответа включен");
  });
  $("manualPresetSave").addEventListener("click", async () => {
    const ok = await saveManualPresetFromEditor();
    if (!ok) return;
    toast("Типовой ответ сохранен");
  });
  $("manualPresetCancel").addEventListener("click", () => {
    const ok = cancelManualPresetChanges();
    if (!ok) return;
    toast("Изменения типового ответа отменены");
  });
  $("manualPresetDelete").addEventListener("click", async () => {
    const ok = await deleteSelectedManualPreset();
    if (!ok) {
      toast("Не выбран типовой ответ для удаления");
      return;
    }
    toast("Типовой ответ удален");
  });

  $("addChannel").addEventListener("click", openChannelForm);
  $("cancelChannel").addEventListener("click", closeChannelForm);
  $("saveChannel").addEventListener("click", () => {
    void saveChannelFromForm();
  });
  $("checkChannels").addEventListener("click", () => {
    void checkChannels();
  });

  $("queueFilterStatus").addEventListener("change", renderQueue);
  $("queueFilterTz").addEventListener("change", renderQueue);
  $("queueFilterOverdue").addEventListener("change", renderQueue);
  if ($("queueSearch")) {
    $("queueSearch").addEventListener("input", renderQueue);
  }
  if ($("stopSearch")) {
    $("stopSearch").addEventListener("input", renderStopList);
  }
  $("dialogPruneDays").addEventListener("input", () => {
    const value = Number($("dialogPruneDays").value);
    if (Number.isFinite(value) && value >= 1) {
      $("dialogPruneDays").value = String(Math.floor(value));
    }
  });
  $("queueRetryErrors").addEventListener("click", async () => {
    const retryable = state.queue.filter((q) => q.status === "failed" || q.status === "stopped").length;
    if (retryable === 0) {
      renderQueueRetryActionState();
      toast("Нет задач со статусами «Ошибка» или «Остановлено»");
      return;
    }

    try {
      const payload = {};
      if (state.queueSessionId) {
        payload.runSessionId = state.queueSessionId;
      }
      const result = await fetchApiJson("/api/queue/retry-errors", {
        method: "POST",
        body: JSON.stringify(payload)
      });

      await refreshQueueFromBackend({ silent: true, runSessionId: state.queueSessionId || null });
      await refreshRunStatusFromBackend({ silent: true, runSessionId: state.queueSessionId || null });
      renderQueue();
      renderDbSyncState();
      updateMetrics();
      renderRunForecast();

      const retried = Number(result?.retried || 0);
      const fromFailed = Number(result?.fromFailed || 0);
      const fromStopped = Number(result?.fromStopped || 0);
      toast(retried > 0
        ? `Переотправка включена для ${retried} задач (ошибка: ${fromFailed}, остановлено: ${fromStopped})`
        : "Нет задач со статусами «Ошибка» или «Остановлено»");
      if (retried > 0) {
        addRunLog(`Переотправка: переведено в «Повтор» ${retried} задач (ошибка: ${fromFailed}, остановлено: ${fromStopped}).`);
      }
    } catch (error) {
      toast(`Не удалось переотправить задачи: ${error?.message || "ошибка backend"}`);
    }
  });

  document.querySelectorAll(".chip").forEach((chip) => {
    chip.addEventListener("click", () => {
      const input = $("tplText");
      const token = chip.dataset.token;
      const start = input.selectionStart;
      const end = input.selectionEnd;
      input.value = input.value.slice(0, start) + token + input.value.slice(end);
      ensureTemplateCreateModeFromEditorInput();
      input.focus();
      input.selectionStart = input.selectionEnd = start + token.length;
      renderTemplateEditorState();
    });
  });

  $("tplDraft").addEventListener("click", async () => {
    await applyTemplateEditorChanges("draft", "Шаблон сохранен как черновик");
  });
  $("tplPublish").addEventListener("click", async () => {
    await applyTemplateEditorChanges("active", "Шаблон опубликован");
  });
  $("tplCancel").addEventListener("click", cancelTemplateEditorChanges);

  $("toStopList").addEventListener("click", async () => {
    const dialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
    if (!dialog) return;
    const ok = await addPhoneToStopList(dialog.phone, "Добавлено вручную из диалога", "Диалог");
    if (ok) toast("Номер клиента добавлен в стоп-лист");
  });

  $("manualSend").addEventListener("click", async () => {
    const dialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
    if (!dialog) return;
    const rawText = $("manualInput").value.trim();
    if (!rawText) {
      toast("Введите текст ручного сообщения");
      return;
    }
    const text = applyManualVariables(dialog, rawText).trim();
    if (!text) {
      toast("После подстановки переменных текст сообщения пустой");
      return;
    }
    try {
      const apiPhone = toApiPhone(dialog.phone);
      const timezoneOffset = resolveDialogTimezoneOffset(dialog);
      const payload = { text };
      if (Number.isFinite(Number(timezoneOffset))) {
        payload.timezoneOffset = Number(timezoneOffset);
      }
      const result = await fetchApiJson(`/api/dialogs/by-phone/${encodeURIComponent(apiPhone)}/send`, {
        method: "POST",
        body: JSON.stringify(payload)
      });
      clearDraftSaveTimer(dialog.phone);
      await fetchApiJson(`/api/dialogs/by-phone/${encodeURIComponent(apiPhone)}/draft`, { method: "DELETE" });
      dialog.manualDraftText = "";
      dialog.manualDraftLoaded = false;
      dialog.manualDraftDirty = false;
      $("manualInput").value = "";
      await refreshDialogMessagesFromBackend(dialog.phone, { silent: true });
      await refreshDialogsFromBackend({ silent: true, ensurePhone: dialog.phone });
      await refreshReportsFromBackend({ silent: true });
      addRunLog(`Ручное сообщение отправлено клиенту ${dialog.phone} через канал ${result?.channelName || "-"}.`);
      toast("Ручное сообщение отправлено");
    } catch (error) {
      toast(`Не удалось отправить ручное сообщение: ${error?.message || "ошибка backend"}`);
    }
  });

  $("manualDraft").addEventListener("click", () => {
    const dialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
    if (!dialog) return;
    const presetId = $("manualPreset").value;
    if (!presetId) {
      toast("Сначала создайте типовой ответ в настройках");
      return;
    }
    const draftText = buildManualPresetDraft(dialog, presetId);
    if (!draftText) {
      toast("Не удалось сформировать черновик из типового ответа");
      return;
    }
    $("manualInput").value = draftText;
    dialog.manualDraftText = draftText;
    dialog.manualDraftDirty = true;
    scheduleDialogDraftSave(dialog);
    const preset = getManualPresetById(presetId);
    toast(`Черновик создан из типового ответа: ${preset ? preset.title : presetId}`);
  });

  $("manualInput").addEventListener("input", () => {
    const dialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
    if (!dialog) return;
    dialog.manualDraftText = $("manualInput").value;
    dialog.manualDraftDirty = true;
    scheduleDialogDraftSave(dialog);
  });

  $("stopAddByPhone").addEventListener("click", async () => {
    const phone = $("stopPhoneInput").value.trim();
    const reason = $("stopReasonInput").value.trim();
    const ok = await addPhoneToStopList(phone, reason, "Ручной ввод");
    if (!ok) return;
    $("stopPhoneInput").value = "";
    $("stopReasonInput").value = "";
    toast("Номер добавлен в стоп-лист");
  });

  window.addEventListener("beforeunload", (event) => {
    stopEventStream();
    const hasUnsaved = hasTemplateUnsavedChanges() || hasManualPresetUnsavedChanges() || hasSettingsUnsavedChanges();
    if (!hasUnsaved) return;
    event.preventDefault();
    event.returnValue = "";
  });
}

async function init() {
  initButtonFeedback();
  bindEvents();
  startDialogPolling();
  startForecastPolling();
  await loadSettings();
  await Promise.all([
    refreshTemplatesFromBackend({ silent: true }),
    refreshManualPresetsFromBackend({ silent: true }),
    refreshChannelsFromBackend({ silent: true }),
    refreshStopListFromBackend({ silent: true }),
    refreshClientsSnapshotFromBackend({ silent: true }),
    refreshRunStatusFromBackend({ silent: true }),
    refreshQueueFromBackend({ silent: true }),
    refreshDialogsFromBackend({ silent: true }),
    refreshAlertsFromBackend({ silent: true }),
    refreshReportsFromBackend({ silent: true })
  ]);
  startEventStream({ resetSince: true });
  if (state.runRuntime) {
    startRunPolling();
  }
  collectRunFiltersFromUI();
  if ($("clientsSearch")) state.clientsViewFilters.search = $("clientsSearch").value || "";
  if ($("clientsFilterTz")) state.clientsViewFilters.tz = $("clientsFilterTz").value;
  if ($("clientsFilterOverdue")) state.clientsViewFilters.overdue = $("clientsFilterOverdue").value;
  renderRunFilterSummary();
  renderDbSyncState();
  await refreshRunForecastFromBackend({ silent: true });
  syncChannelAlertFlags();
  renderAlerts();
  renderChannels();
  renderTemplates();
  renderTemplateEditorState();
  renderManualPresetManager();
  renderClientsDb();
  renderQueue();
  renderDialogs();
  renderChat();
  renderStopList();
  renderBars();
  updateMetrics();
  addRunLog("Интерфейс загружен. Настройки и рабочие данные синхронизируются с backend API.");
}

void init();
