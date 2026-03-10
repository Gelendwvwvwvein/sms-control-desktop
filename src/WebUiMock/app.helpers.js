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
      const baseMessage = typeof payload === "object" && payload !== null
        ? (payload.message || payload.title || "")
        : "";
      const detail = typeof payload === "object" && payload !== null
        ? (payload.detail || "")
        : "";
      const fallbackMessage = typeof payload === "string" && payload.trim()
        ? payload.trim()
        : `HTTP ${response.status}`;
      const message = [baseMessage, detail].filter(Boolean).join(". ") || fallbackMessage;
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

  const sorted = [...candidates].sort((a, b) => {
    const byStatus = (QUEUE_STATUS_PRIORITY[a.status] ?? 99) - (QUEUE_STATUS_PRIORITY[b.status] ?? 99);
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

function getDebtBufferAmount() {
  const value = Number($("cfgDebtBufferAmount")?.value);
  if (!Number.isFinite(value) || value < 0) return DEFAULT_DEBT_BUFFER_AMOUNT;
  return Math.min(1000000, Math.round(value));
}

function getRecentSmsCooldownDays() {
  const value = Number($("cfgRecentSmsCooldownDays")?.value);
  if (!Number.isFinite(value) || value < 0) return 0;
  return Math.min(365, Math.round(value));
}

function formatRunSessionStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  return RUN_SESSION_STATUS_TEXT[normalized] || (normalized || "—");
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
  const rounded = Math.round((numeric + getDebtBufferAmount()) / 1000) * 1000;
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
  const normalizedDays = Number(days);
  if (!Number.isFinite(normalizedDays)) return "";
  const matched = getConfiguredRunOverdueOptions()
    .filter((item) => normalizedDays >= item.from && normalizedDays <= item.to)
    .sort((a, b) => {
      if (a.priority !== b.priority) return a.priority - b.priority;
      if (a.sortOrder !== b.sortOrder) return a.sortOrder - b.sortOrder;
      if (a.from !== b.from) return a.from - b.from;
      return a.to - b.to;
    })[0];
  return matched?.value || "";
}

function normalizeTemplateOverdueMode(mode) {
  return String(mode || "").trim().toLowerCase() === "exact" ? "exact" : "range";
}

function parseTemplateOptionalInt(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.trunc(parsed);
}

function normalizeTemplateRuleTypeId(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  const chars = raw
    .split("")
    .map((ch) => ((/[a-z0-9_-]/).test(ch) ? ch : "_"));
  let result = chars.join("");
  while (result.includes("__")) {
    result = result.replaceAll("__", "_");
  }
  return result.replace(/^_+|_+$/g, "");
}

function cloneTemplateRuleType(item) {
  return {
    id: item.id,
    name: item.name,
    overdueMode: item.overdueMode,
    overdueFromDays: item.overdueFromDays,
    overdueToDays: item.overdueToDays,
    overdueExactDay: item.overdueExactDay,
    autoAssign: item.autoAssign !== false,
    sortOrder: item.sortOrder
  };
}

function normalizeTemplateRuleTypes(items) {
  const source = Array.isArray(items) ? items : [];
  const sorted = source
    .filter((item) => item && typeof item === "object")
    .sort((a, b) => {
      const aOrder = Number.isFinite(Number(a.sortOrder)) ? Number(a.sortOrder) : Number.MAX_SAFE_INTEGER;
      const bOrder = Number.isFinite(Number(b.sortOrder)) ? Number(b.sortOrder) : Number.MAX_SAFE_INTEGER;
      if (aOrder !== bOrder) return aOrder - bOrder;
      return String(a.name || "").localeCompare(String(b.name || ""), "ru");
    });

  const result = [];
  const usedIds = new Set();
  let fallbackOrder = 10;

  sorted.forEach((item, index) => {
    const baseId = normalizeTemplateRuleTypeId(item.id) || `type_${index + 1}`;
    let resolvedId = baseId;
    let suffix = 2;
    while (usedIds.has(resolvedId)) {
      resolvedId = `${baseId}_${suffix}`;
      suffix += 1;
    }
    usedIds.add(resolvedId);

    const autoAssign = item.autoAssign !== false;
    const mode = autoAssign ? normalizeTemplateOverdueMode(item.overdueMode) : "range";
    const resolvedSortOrder = Number.isFinite(Number(item.sortOrder)) && Number(item.sortOrder) > 0
      ? Math.trunc(Number(item.sortOrder))
      : fallbackOrder;
    const normalized = {
      id: resolvedId,
      name: String(item.name || "").trim() || resolvedId,
      overdueMode: mode,
      overdueFromDays: null,
      overdueToDays: null,
      overdueExactDay: null,
      autoAssign,
      sortOrder: resolvedSortOrder
    };

    if (!autoAssign) {
      normalized.overdueFromDays = 0;
      normalized.overdueToDays = 0;
    } else if (mode === "exact") {
      normalized.overdueExactDay = Math.max(0, parseTemplateOptionalInt(item.overdueExactDay) ?? 0);
    } else {
      const from = Math.max(0, parseTemplateOptionalInt(item.overdueFromDays) ?? 0);
      const to = Math.max(from, parseTemplateOptionalInt(item.overdueToDays) ?? from);
      normalized.overdueFromDays = from;
      normalized.overdueToDays = to;
    }

    fallbackOrder = resolvedSortOrder + 10;
    result.push(normalized);
  });

  if (result.length === 0) {
    return DEFAULT_TEMPLATE_RULE_TYPES.map(cloneTemplateRuleType);
  }

  return result
    .sort((a, b) => {
      if (a.sortOrder !== b.sortOrder) return a.sortOrder - b.sortOrder;
      return String(a.name || "").localeCompare(String(b.name || ""), "ru");
    })
    .map(cloneTemplateRuleType);
}

function getTemplateRuleTypeById(kind) {
  const normalized = normalizeTemplateRuleTypeId(kind);
  if (!normalized) return null;
  return state.templateRuleTypes.find((item) => item.id === normalized) || null;
}

function getDefaultTemplateRuleType() {
  const fromState = getTemplateRuleTypeById(DEFAULT_TEMPLATE_KIND) || state.templateRuleTypes[0] || null;
  if (fromState) {
    return cloneTemplateRuleType(fromState);
  }
  const fallback = DEFAULT_TEMPLATE_RULE_TYPES.find((x) => x.id === DEFAULT_TEMPLATE_KIND) || DEFAULT_TEMPLATE_RULE_TYPES[0];
  return cloneTemplateRuleType(fallback);
}

function normalizeTemplateKind(kind, options = {}) {
  const { allowMissing = false } = options;
  const normalized = normalizeTemplateRuleTypeId(kind);
  if (normalized && getTemplateRuleTypeById(normalized)) {
    return normalized;
  }
  if (allowMissing && normalized) {
    return normalized;
  }
  return getDefaultTemplateRuleType().id || DEFAULT_TEMPLATE_KIND;
}

function getTemplateType(kind, options = {}) {
  const { allowMissing = false, fallbackTemplate = null } = options;
  const resolvedKind = normalizeTemplateKind(kind, { allowMissing });
  const matched = getTemplateRuleTypeById(resolvedKind);

  if (matched) {
    const autoAssign = matched.autoAssign !== false;
    const mode = autoAssign ? normalizeTemplateOverdueMode(matched.overdueMode) : "range";
    const minOverdue = autoAssign
      ? Math.max(0, parseTemplateOptionalInt(matched.overdueFromDays) ?? 0)
      : 0;
    const maxOverdue = autoAssign
      ? Math.max(minOverdue, parseTemplateOptionalInt(matched.overdueToDays) ?? minOverdue)
      : 0;
    return {
      ...cloneTemplateRuleType(matched),
      label: matched.name,
      overdueMode: mode,
      overdueFromDays: minOverdue,
      overdueToDays: maxOverdue,
      overdueExactDay: autoAssign && mode === "exact"
        ? Math.max(0, parseTemplateOptionalInt(matched.overdueExactDay) ?? 0)
        : null,
      autoAssign,
      minOverdue,
      maxOverdue,
      exists: true
    };
  }

  const fallbackType = getDefaultTemplateRuleType();
  const fallbackMode = normalizeTemplateOverdueMode(fallbackType.overdueMode);
  const fallbackFrom = Math.max(0, parseTemplateOptionalInt(fallbackType.overdueFromDays) ?? 0);
  const fallbackTo = Math.max(fallbackFrom, parseTemplateOptionalInt(fallbackType.overdueToDays) ?? fallbackFrom);
  const fallbackExact = Math.max(0, parseTemplateOptionalInt(fallbackType.overdueExactDay) ?? 0);

  if (allowMissing && resolvedKind) {
    const autoAssign = fallbackTemplate ? fallbackTemplate.autoAssign !== false : fallbackType.autoAssign !== false;
    const sourceMode = fallbackTemplate ? normalizeTemplateOverdueMode(fallbackTemplate.overdueMode) : fallbackMode;
    const mode = autoAssign ? sourceMode : "range";
    const from = autoAssign
      ? Math.max(0, parseTemplateOptionalInt(fallbackTemplate?.overdueFromDays) ?? fallbackFrom)
      : 0;
    const to = autoAssign
      ? Math.max(from, parseTemplateOptionalInt(fallbackTemplate?.overdueToDays) ?? fallbackTo)
      : 0;
    const exact = autoAssign
      ? Math.max(0, parseTemplateOptionalInt(fallbackTemplate?.overdueExactDay) ?? fallbackExact)
      : null;
    const label = String(kind || resolvedKind).trim() || resolvedKind;
    return {
      id: resolvedKind,
      name: label,
      label: `${label} (удален)`,
      overdueMode: mode,
      overdueFromDays: from,
      overdueToDays: to,
      overdueExactDay: mode === "exact" ? exact : null,
      autoAssign,
      sortOrder: Number.MAX_SAFE_INTEGER,
      minOverdue: from,
      maxOverdue: to,
      exists: false
    };
  }

  const fallbackAutoAssign = fallbackType.autoAssign !== false;
  const fallbackResolvedMode = fallbackAutoAssign ? fallbackMode : "range";
  const fallbackResolvedFrom = fallbackAutoAssign ? fallbackFrom : 0;
  const fallbackResolvedTo = fallbackAutoAssign ? fallbackTo : 0;
  return {
    ...cloneTemplateRuleType(fallbackType),
    label: fallbackType.name,
    overdueMode: fallbackResolvedMode,
    overdueFromDays: fallbackResolvedFrom,
    overdueToDays: fallbackResolvedTo,
    overdueExactDay: fallbackResolvedMode === "exact" ? fallbackExact : null,
    autoAssign: fallbackAutoAssign,
    minOverdue: fallbackResolvedFrom,
    maxOverdue: fallbackResolvedTo,
    exists: true
  };
}

function templateTypeLabel(kind, options = {}) {
  return getTemplateType(kind, {
    allowMissing: options.allowMissing === true,
    fallbackTemplate: options.fallbackTemplate || null
  }).label;
}

function formatTemplateRuleTypeRule(type) {
  if (type?.autoAssign === false) {
    return "Только ручное назначение";
  }
  const mode = normalizeTemplateOverdueMode(type?.overdueMode);
  if (mode === "exact") {
    const exact = Math.max(0, parseTemplateOptionalInt(type?.overdueExactDay) ?? 0);
    return `Точный день: ${exact}`;
  }
  const from = Math.max(0, parseTemplateOptionalInt(type?.overdueFromDays) ?? 0);
  const to = Math.max(from, parseTemplateOptionalInt(type?.overdueToDays) ?? from);
  return `Диапазон: ${from}-${to}`;
}

function getAppliedTemplateRuleTypes() {
  return normalizeTemplateRuleTypes(state.templateRuleTypes);
}

function getConfiguredRunOverdueOptions() {
  const byValue = new Map();
  getAppliedTemplateRuleTypes().forEach((type, index) => {
    if (type?.autoAssign === false) return;
    const mode = normalizeTemplateOverdueMode(type?.overdueMode);
    const sortOrder = Number.isFinite(Number(type?.sortOrder))
      ? Number(type.sortOrder)
      : ((index + 1) * 10);
    let from = 0;
    let to = 0;
    let value = "";
    let label = "";
    let priority = 0;

    if (mode === "exact") {
      const exact = Math.max(0, parseTemplateOptionalInt(type?.overdueExactDay) ?? 0);
      from = exact;
      to = exact;
      value = String(exact);
      label = `Точный день ${exact}`;
      priority = 0;
    } else {
      from = Math.max(0, parseTemplateOptionalInt(type?.overdueFromDays) ?? 0);
      to = Math.max(from, parseTemplateOptionalInt(type?.overdueToDays) ?? from);
      value = `${from}-${to}`;
      label = `Просрочка ${value}`;
      priority = Math.max(1, to - from + 1);
    }

    const existing = byValue.get(value);
    if (existing && existing.sortOrder <= sortOrder) {
      return;
    }

    byValue.set(value, {
      value,
      from,
      to,
      sortOrder,
      priority,
      label
    });
  });

  return Array.from(byValue.values()).sort((a, b) => {
    if (a.sortOrder !== b.sortOrder) return a.sortOrder - b.sortOrder;
    if (a.from !== b.from) return a.from - b.from;
    return a.to - b.to;
  });
}

function renderOverdueSelectOptions(selectId, options = {}) {
  const select = $(selectId);
  if (!select) return;

  const { preserveSelection = true, fallbackValue = "all" } = options;
  const configured = getConfiguredRunOverdueOptions();
  const currentValue = preserveSelection ? String(select.value || fallbackValue) : String(fallbackValue);
  const available = new Set(configured.map((item) => item.value));
  const nextValue = available.has(currentValue) ? currentValue : String(fallbackValue);

  select.innerHTML = [
    '<option value="all">Все</option>',
    ...configured.map((item) => `<option value="${item.value}">${item.label}</option>`)
  ].join("");
  select.value = nextValue;
}

function normalizeRunExactOverdueInput(value, options = {}) {
  const { strict = false } = options;
  const raw = String(value || "").trim();
  if (!raw) return "";

  if (/^https?:\/\//i.test(raw) || /rocketman/i.test(raw)) {
    return "";
  }

  const compact = raw.replace(/[^\d-]/g, "");
  if (!compact) return "";

  const dashIndex = compact.indexOf("-");
  if (dashIndex < 0) {
    const single = compact.replace(/-/g, "").slice(0, 3);
    if (!strict) return single;
    return /^\d{1,3}$/.test(single) ? String(Number(single)) : "";
  }

  const fromRaw = compact.slice(0, dashIndex).replace(/-/g, "").slice(0, 3);
  const toRaw = compact.slice(dashIndex + 1).replace(/-/g, "").slice(0, 3);
  const partial = `${fromRaw}${toRaw ? `-${toRaw}` : "-"}`;
  if (!strict) return partial;

  if (!/^\d{1,3}-\d{1,3}$/.test(partial)) return "";
  const from = Number(fromRaw);
  const to = Number(toRaw);
  if (!Number.isFinite(from) || !Number.isFinite(to) || from > to) return "";
  return `${from}-${to}`;
}

function parseRunExactOverdue(value) {
  const raw = normalizeRunExactOverdueInput(value, { strict: true });
  if (!raw) return null;

  const singleMatch = raw.match(/^(\d{1,3})$/);
  if (singleMatch) {
    const day = Number(singleMatch[1]);
    return Number.isFinite(day) ? { raw, normalized: String(day), from: day, to: day } : null;
  }

  const rangeMatch = raw.match(/^(\d{1,3})\s*-\s*(\d{1,3})$/);
  if (!rangeMatch) return null;

  const from = Number(rangeMatch[1]);
  const to = Number(rangeMatch[2]);
  if (!Number.isFinite(from) || !Number.isFinite(to) || from < 0 || to < from) {
    return null;
  }

  return {
    raw,
    normalized: `${from}-${to}`,
    from,
    to
  };
}

function getNormalizedRunExactOverdue() {
  return parseRunExactOverdue(state.runFilters.exactOverdue)?.normalized || "";
}

function renderRunOverdueFilterOptions(options = {}) {
  const { preserveSelection = true } = options;
  const container = $("runOverdueFilterBlock");
  if (!container) return;

  const overdueOptions = getConfiguredRunOverdueOptions();
  const selectAllBtn = $("runOverdueSelectAll");
  const clearAllBtn = $("runOverdueClearAll");
  if (selectAllBtn) selectAllBtn.disabled = overdueOptions.length === 0;
  if (clearAllBtn) clearAllBtn.disabled = overdueOptions.length === 0;

  if (overdueOptions.length === 0) {
    state.runFilters.overdueRanges = new Set();
    container.innerHTML = '<p class="muted-note">Авто-фильтры просрочки не настроены в типах шаблонов.</p>';
    return;
  }

  const availableValues = new Set(overdueOptions.map((item) => item.value));
  const nextSelection = preserveSelection
    ? new Set(Array.from(state.runFilters.overdueRanges).filter((value) => availableValues.has(value)))
    : new Set();
  if (nextSelection.size === 0) {
    overdueOptions.forEach((item) => nextSelection.add(item.value));
  }
  state.runFilters.overdueRanges = nextSelection;

  container.innerHTML = overdueOptions.map((item) => `
    <label><input type="checkbox" class="run-overdue" value="${item.value}" ${state.runFilters.overdueRanges.has(item.value) ? "checked" : ""}> ${item.label}</label>
  `).join("");
}

function renderConfiguredOverdueFilters(options = {}) {
  const { preserveSelection = true } = options;
  renderRunOverdueFilterOptions({ preserveSelection });
  renderOverdueSelectOptions("clientsFilterOverdue", {
    preserveSelection,
    fallbackValue: state.clientsViewFilters.overdue || "all"
  });
  renderOverdueSelectOptions("queueFilterOverdue", {
    preserveSelection,
    fallbackValue: $("queueFilterOverdue")?.value || "all"
  });
  if ($("clientsFilterOverdue")) {
    state.clientsViewFilters.overdue = $("clientsFilterOverdue").value || "all";
  }
}

function templateTypeRule(kind, options = {}) {
  const type = getTemplateType(kind, {
    allowMissing: true,
    fallbackTemplate: options.fallbackTemplate || null
  });
  if (type.exists === false) {
    return `Тип «${type.name}» удален из настроек. Выберите актуальный тип и сохраните шаблон.`;
  }
  if (type.autoAssign === false) {
    return `Тип «${type.name}». Только ручное назначение: в автоподборе очереди не участвует, правило просрочки не требуется.`;
  }
  return `Тип «${type.name}». ${formatTemplateRuleTypeRule(type)}. Тип участвует в автоподборе очереди.`;
}

function templateKindSortOrder(kind) {
  const type = getTemplateType(kind, { allowMissing: true });
  return Number.isFinite(Number(type.sortOrder)) ? Number(type.sortOrder) : Number.MAX_SAFE_INTEGER;
}

function resolveTemplateRuleFields(template) {
  const fallback = getTemplateType(template?.kind, { allowMissing: true, fallbackTemplate: template || null });
  const mode = normalizeTemplateOverdueMode(template?.overdueMode || fallback.overdueMode);
  const fromDays = parseTemplateOptionalInt(template?.overdueFromDays);
  const toDays = parseTemplateOptionalInt(template?.overdueToDays);
  const exactDay = parseTemplateOptionalInt(template?.overdueExactDay);
  if (mode === "exact") {
    return {
      mode,
      fromDays: null,
      toDays: null,
      exactDay: Math.max(0, exactDay ?? parseTemplateOptionalInt(fallback.overdueExactDay) ?? 0)
    };
  }

  const fallbackFrom = Math.max(0, parseTemplateOptionalInt(fallback.overdueFromDays) ?? fallback.minOverdue ?? 0);
  const fallbackTo = Math.max(fallbackFrom, parseTemplateOptionalInt(fallback.overdueToDays) ?? fallback.maxOverdue ?? fallbackFrom);
  const resolvedFrom = Math.max(0, fromDays ?? fallbackFrom);
  const resolvedTo = Math.max(resolvedFrom, toDays ?? fallbackTo);
  return {
    mode,
    fromDays: resolvedFrom,
    toDays: resolvedTo,
    exactDay: null
  };
}

function formatTemplateOverdueRule(template) {
  if (isTemplateManualOnly(template)) {
    return "Только ручное назначение";
  }
  const rule = resolveTemplateRuleFields(template);
  if (rule.mode === "exact" && rule.exactDay !== null) {
    return `Точный день: ${rule.exactDay}`;
  }
  return `Диапазон: ${rule.fromDays}-${rule.toDays}`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function toSingleLineText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function templateOptionLabel(template) {
  if (!template) return "шаблон";
  return `${templateTypeLabel(template.kind, { allowMissing: true, fallbackTemplate: template })}: ${template.name} (${formatTemplateOverdueRule(template)})`;
}

function templateCommentPreview(commentText, maxLength = 90) {
  const singleLine = toSingleLineText(commentText);
  if (!singleLine) return "";
  if (singleLine.length <= maxLength) return singleLine;
  return `${singleLine.slice(0, Math.max(1, maxLength - 1))}\u2026`;
}

function normalizeCommentRules(rules) {
  const source = rules && typeof rules === "object" ? rules : {};
  const pick = (key) => {
    const fallback = DEFAULT_COMMENT_RULES[key];
    const value = String(source[key] ?? "").trim();
    return value || fallback;
  };
  return {
    sms2: pick("sms2"),
    sms3: pick("sms3"),
    ka1: pick("ka1"),
    kaN: pick("kaN"),
    kaFinal: pick("kaFinal")
  };
}

function isTemplateEligibleForOverdue(template, daysOverdue, options = {}) {
  const { allowManualOnly = false } = options;
  if (!template) return false;
  if (isTemplateManualOnly(template)) {
    return allowManualOnly;
  }

  const rule = resolveTemplateRuleFields(template);
  if (rule.mode === "exact") {
    return rule.exactDay !== null && daysOverdue === rule.exactDay;
  }
  return daysOverdue >= rule.fromDays && daysOverdue <= rule.toDays;
}

function isTemplateManualOnly(template) {
  if (!template) return false;
  const kind = normalizeTemplateKind(template.kind, { allowMissing: true });
  return resolveTemplateAutoAssignByKind(kind, { fallbackTemplate: template }) === false;
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
    getNormalizedRunExactOverdue() !== ""
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
  const exactOverdue = getNormalizedRunExactOverdue();
  return {
    snapshotId: state.clientsDb.snapshotId || null,
    timezoneOffsets: Array.from(state.runFilters.tz).sort((a, b) => a - b),
    overdueRanges: exactOverdue ? [] : Array.from(state.runFilters.overdueRanges),
    exactDay: null,
    exactOverdue
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
    debtErrorDetail: item.debtErrorDetail || "",
    inPlan: Boolean(item.inPlan),
    inPlanRunSessionId: item.inPlanRunSessionId == null ? null : Number(item.inPlanRunSessionId)
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

function mapRunHistoryDtoToUi(item) {
  return {
    id: Number(item?.id || 0),
    mode: String(item?.mode || "").trim(),
    status: String(item?.status || "").trim().toLowerCase(),
    createdAtUtc: String(item?.createdAtUtc || ""),
    startedAtUtc: String(item?.startedAtUtc || ""),
    finishedAtUtc: String(item?.finishedAtUtc || ""),
    snapshotId: item?.snapshotId == null ? null : Number(item.snapshotId),
    notes: String(item?.notes || "").trim(),
    totalJobs: Number(item?.totalJobs || 0),
    queuedJobs: Number(item?.queuedJobs || 0),
    runningJobs: Number(item?.runningJobs || 0),
    retryJobs: Number(item?.retryJobs || 0),
    stoppedJobs: Number(item?.stoppedJobs || 0),
    sentJobs: Number(item?.sentJobs || 0),
    failedJobs: Number(item?.failedJobs || 0)
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
    lastCheckedAtUtc: item.lastCheckedAtUtc || "",
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
  const kind = normalizeTemplateKind(item.kind, { allowMissing: true });
  const fallback = getTemplateType(kind, { allowMissing: true, fallbackTemplate: item || null });
  const mode = normalizeTemplateOverdueMode(item.overdueMode || fallback.overdueMode);
  const fromDays = parseTemplateOptionalInt(item.overdueFromDays);
  const toDays = parseTemplateOptionalInt(item.overdueToDays);
  const exactDay = parseTemplateOptionalInt(item.overdueExactDay);
  const resolvedFrom = Math.max(0, fromDays ?? fallback.minOverdue ?? 0);
  const resolvedTo = Math.max(resolvedFrom, toDays ?? fallback.maxOverdue ?? resolvedFrom);
  const resolvedExact = Math.max(0, exactDay ?? parseTemplateOptionalInt(fallback.overdueExactDay) ?? 0);
  return {
    id: Number(item.id),
    kind,
    status: String(item.status || "draft"),
    name: String(item.name || "").trim(),
    text: String(item.text || "").trim(),
    overdueMode: mode,
    overdueFromDays: mode === "range" ? resolvedFrom : null,
    overdueToDays: mode === "range" ? resolvedTo : null,
    overdueExactDay: mode === "exact" ? resolvedExact : null,
    overdueText: String(item.overdueText || "").trim() || (mode === "exact"
      ? `Точный день: ${resolvedExact}`
      : `Диапазон: ${resolvedFrom}-${resolvedTo}`),
    autoAssign: fallback.autoAssign !== false,
    commentText: String(item.commentText || "").trim()
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
    contractNumber: String(item.contractNumber || "").trim(),
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

