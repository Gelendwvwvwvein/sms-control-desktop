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

function channelStatusText(channelOrStatus) {
  if (channelOrStatus && typeof channelOrStatus === "object") {
    const status = channelOrStatus.status || "unknown";
    if (status === "unknown") {
      return channelOrStatus.lastCheckedAtUtc ? "Gateway достижим" : CHANNEL_STATUS_TEXT.unknown;
    }
    return CHANNEL_STATUS_TEXT[status] || CHANNEL_STATUS_TEXT.unknown;
  }

  return CHANNEL_STATUS_TEXT[channelOrStatus] || CHANNEL_STATUS_TEXT.unknown;
}

function isPhoneInStopList(phone) {
  const p = normalizePhone(phone);
  return state.stoplist.some((s) => normalizePhone(s.phone) === p);
}

function statusCell(status) {
  return `<span class="pill ${status}">${STATUS_TEXT[status] || status}</span>`;
}

function queueStatusCell(job) {
  const base = statusCell(job.status);
  const code = String(job.lastErrorCode || "").trim();
  const detail = String(job.lastErrorDetail || "").trim();
  if (!code && !detail) return base;

  const full = [code, detail].filter((x) => x).join(": ");
  const short = code || detail;
  return `${base}<small class="queue-status-error" title="${escapeHtml(full)}">${escapeHtml(short)}</small>`;
}

function shouldPassRunFilter(job) {
  const exactOverdue = parseRunExactOverdue(state.runFilters.exactOverdue);
  const hasAnyFilter =
    state.runFilters.tz.size > 0 ||
    state.runFilters.overdueRanges.size > 0 ||
    Boolean(exactOverdue);
  if (!hasAnyFilter) return false;

  const tzFilterActive = state.runFilters.tz.size > 0;
  if (tzFilterActive && !state.runFilters.tz.has(job.tzOffset)) return false;

  if (exactOverdue) {
    return job.daysOverdue >= exactOverdue.from && job.daysOverdue <= exactOverdue.to;
  }

  const overdueFilterActive = state.runFilters.overdueRanges.size > 0;
  if (!overdueFilterActive) return true;
  return Array.from(state.runFilters.overdueRanges).some((value) => {
    const range = parseRunExactOverdue(value);
    return range && job.daysOverdue >= range.from && job.daysOverdue <= range.to;
  });
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
  return state.clients.some((client) =>
    client.id === clientId &&
    client.inPlan === true &&
    Number(client.inPlanRunSessionId || 0) > 0);
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
    await refreshPlanningSessionViews({
      silent: true,
      runSessionId: state.queueSessionId,
      includeHistory: true
    });
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

  await refreshDialogWorkspaceByPhone(apiPhone, {
    silent: true,
    ensurePhone: apiPhone,
    refreshDraft: true,
    applyDraftToInput: true
  });
  if (state.dialogPreview.enabled) {
    await rebuildDialogPreviewForSelected({ silent: true });
  } else {
    syncDialogPreviewWithQueue();
    renderDialogPreviewPanel();
  }
}

function normalizeOptionalRunSessionId(runSessionId) {
  const normalized = Number(runSessionId || 0);
  return Number.isFinite(normalized) && normalized > 0 ? normalized : null;
}

function currentQueueRunSessionId() {
  return normalizeOptionalRunSessionId(state.queueSessionId);
}

async function refreshPlanningSessionViews(options = {}) {
  const { silent = true, includeStatus = true, includeHistory = false, includeForecast = false } = options;
  const runSessionId = Object.prototype.hasOwnProperty.call(options, "runSessionId")
    ? normalizeOptionalRunSessionId(options.runSessionId)
    : currentQueueRunSessionId();

  const refreshTasks = [refreshQueueFromBackend({ silent, runSessionId })];
  if (includeStatus) {
    refreshTasks.push(refreshRunStatusFromBackend({ silent, runSessionId }));
  }

  await Promise.all(refreshTasks);

  if (includeHistory) {
    await refreshRunHistoryFromBackend({ silent });
  }
  if (includeForecast) {
    await refreshRunForecastFromBackend({ silent });
  }
}

async function refreshChannelMonitoringViews(options = {}) {
  const { silent = true, includeReports = false, includeForecast = false, rerenderQueue = false } = options;
  const refreshTasks = [
    refreshChannelsFromBackend({ silent }),
    refreshAlertsFromBackend({ silent })
  ];
  if (includeReports) {
    refreshTasks.push(refreshReportsFromBackend({ silent }));
  }

  await Promise.all(refreshTasks);

  if (rerenderQueue) {
    renderQueue();
  }
  if (includeForecast) {
    await refreshRunForecastFromBackend({ silent });
  }
}

async function refreshDialogWorkspaceByPhone(phone, options = {}) {
  const {
    silent = true,
    ensurePhone = phone,
    refreshMessages = true,
    refreshDialogs = true,
    refreshDraft = false,
    applyDraftToInput = false
  } = options;

  const apiPhone = toApiPhone(phone);
  if (!apiPhone) return false;

  if (refreshMessages) {
    await refreshDialogMessagesFromBackend(apiPhone, { silent });
  }
  if (refreshDialogs) {
    await refreshDialogsFromBackend({ silent, ensurePhone: ensurePhone || apiPhone });
  }
  if (refreshDraft) {
    await refreshDialogDraftFromBackend(apiPhone, { silent, applyToInput: applyDraftToInput });
  }

  return true;
}

async function refreshDebtDependentViews(options = {}) {
  const { silent = true } = options;
  await refreshClientsSnapshotFromBackend({ silent });
  await refreshQueueFromBackend({ silent, runSessionId: currentQueueRunSessionId() });
  renderDialogs();
  renderChat();
}

function collectNormalizedPhones(rows) {
  return [...new Set((rows || []).map((row) => normalizePhone(row?.phone)).filter(Boolean))];
}

function getValidatedStopListPhones(rows, options = {}) {
  const {
    emptySelectionMessage = "Сначала выберите строки",
    emptyPhoneMessage = "У выбранных клиентов нет валидных телефонов",
    invalidPhoneMessage = "Операция недоступна: у части выбранных клиентов нет валидного номера телефона.",
    membership = "any",
    membershipErrorMessage = ""
  } = options;

  if (!Array.isArray(rows) || rows.length === 0) {
    toast(emptySelectionMessage);
    return null;
  }

  const phones = collectNormalizedPhones(rows);
  if (phones.length === 0) {
    toast(emptyPhoneMessage);
    return null;
  }

  if (rows.some((row) => !normalizePhone(row?.phone))) {
    toast(invalidPhoneMessage);
    return null;
  }

  if (membership === "outside" && rows.some((row) => isPhoneInStopList(row.phone))) {
    toast(membershipErrorMessage || "Операция недоступна: среди выбранных есть номера из стоп-листа.");
    return null;
  }

  if (membership === "inside" && rows.some((row) => !isPhoneInStopList(row.phone))) {
    toast(membershipErrorMessage || "Операция недоступна: среди выбранных есть номера вне стоп-листа.");
    return null;
  }

  return phones;
}

function refreshAfterStopListChange(options = {}) {
  const { stopListAlreadyRendered = false, dialogsAlreadyRendered = false } = options;
  if (!stopListAlreadyRendered) {
    renderStopList();
  }
  if (!dialogsAlreadyRendered) {
    renderDialogs();
  }
  refreshPlanningViews();
}

async function bulkAddPhonesToStopList(options = {}) {
  const {
    phones,
    reason,
    source,
    selectionSet = null,
    renderSelection = null,
    emptyResultMessage = "Выбранные номера уже в стоп-листе"
  } = options;

  try {
    const result = await fetchApiJson("/api/stop-list/bulk/add", {
      method: "POST",
      body: JSON.stringify({ phones, reason, source })
    });
    const added = Number(result?.added ?? 0);
    if (added > 0) {
      await removePendingQueueJobsByPhones(phones, { silent: true });
    }

    await refreshStopListFromBackend({ silent: true });
    refreshAfterStopListChange({ stopListAlreadyRendered: true, dialogsAlreadyRendered: true });

    if (selectionSet instanceof Set) {
      selectionSet.clear();
    }
    if (typeof renderSelection === "function") {
      renderSelection();
    }

    toast(added > 0 ? `В стоп-лист добавлено: ${added}` : emptyResultMessage);
    return added;
  } catch (error) {
    toast(`Не удалось добавить в стоп-лист: ${error?.message || "ошибка backend"}`);
    return null;
  }
}

async function bulkRemovePhonesFromStopList(options = {}) {
  const {
    phones,
    selectionSet = null,
    renderSelection = null,
    emptyResultMessage = "Выбранные номера не найдены в стоп-листе"
  } = options;

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
    refreshAfterStopListChange({ stopListAlreadyRendered: true, dialogsAlreadyRendered: true });

    if (selectionSet instanceof Set) {
      selectionSet.clear();
    }
    if (typeof renderSelection === "function") {
      renderSelection();
    }

    toast(removed > 0 ? `Из стоп-листа удалено: ${removed}` : emptyResultMessage);
    return removed;
  } catch (error) {
    toast(`Не удалось убрать из стоп-листа: ${error?.message || "ошибка backend"}`);
    return null;
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
      await refreshPlanningSessionViews({ silent: true });
      await refreshChannelMonitoringViews({ silent: true, includeReports: true });
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
      await refreshPlanningSessionViews({ silent: true });
      await refreshChannelMonitoringViews({ silent: true, includeReports: true });
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
  const hasExplicitRunSessionId = Object.prototype.hasOwnProperty.call(options, "runSessionId");
  const effectiveRunSessionId = hasExplicitRunSessionId
    ? Number(runSessionId || 0)
    : Number(state.runHistorySelectedSessionId || 0);
  try {
    const path = effectiveRunSessionId > 0
      ? `/api/run/status?runSessionId=${encodeURIComponent(effectiveRunSessionId)}`
      : "/api/run/status";
    const data = await fetchApiJson(path);
    applyRunStatusToUi(data);
    if (activeTabId() === "run") {
      void refreshRunHistoryFromBackend({ silent: true });
    }
    renderDbSyncState();
    return data;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось получить статус запуска: ${error?.message || "ошибка backend"}`);
    }
    return null;
  }
}

async function openQueueFromHistorySession(runSessionId, options = {}) {
  const { switchToQueueTab = true } = options;
  const sessionId = Number(runSessionId || 0);
  if (!Number.isFinite(sessionId) || sessionId <= 0) return false;

  const loaded = await refreshQueueFromBackend({ silent: false, runSessionId: sessionId });
  if (!loaded) return false;

  state.runHistorySelectedSessionId = sessionId;
  renderRunHistory();

  await refreshRunStatusFromBackend({ silent: true, runSessionId: sessionId });
  if (switchToQueueTab) {
    await switchTab("queue", { actionLabel: "открыть очередь из истории" });
  }
  toast(`Открыта очередь из сессии #${sessionId}`);
  return true;
}

async function openLatestQueueSession(options = {}) {
  const { switchToQueueTab = false } = options;
  const loaded = await refreshQueueFromBackend({ silent: false, runSessionId: null });
  if (!loaded) return false;

  state.runHistorySelectedSessionId = null;
  renderRunHistory();
  await refreshRunStatusFromBackend({ silent: true });

  if (switchToQueueTab) {
    await switchTab("queue", { actionLabel: "открыть актуальную очередь" });
  }
  toast("Открыта актуальная очередь");
  return true;
}

async function refreshRunHistoryFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const data = await fetchApiJson("/api/run/history?limit=100&offset=0");
    state.runHistoryTotal = Number(data?.total || 0);
    state.runHistory = Array.isArray(data?.items) ? data.items.map(mapRunHistoryDtoToUi) : [];
    if (state.runHistorySelectedSessionId) {
      const selectedExists = state.runHistory.some((item) => Number(item.id) === Number(state.runHistorySelectedSessionId));
      if (!selectedExists) {
        state.runHistorySelectedSessionId = null;
      }
    }
    renderRunHistory();
    return true;
  } catch (error) {
    if (!silent) {
      toast(`Не удалось загрузить историю запусков: ${error?.message || "ошибка backend"}`);
    }
    return false;
  }
}

async function clearRunHistory() {
  if (state.runHistory.length === 0) {
    toast("История запусков уже пуста");
    return;
  }

  const confirmed = window.confirm("Очистить историю запусков? Активная сессия и текущий план удалены не будут.");
  if (!confirmed) return;

  try {
    const result = await fetchApiJson("/api/run/history", { method: "DELETE" });
    await refreshPlanningSessionViews({
      silent: true,
      runSessionId: normalizeOptionalRunSessionId(state.runHistorySelectedSessionId),
      includeHistory: true
    });
    const deletedSessions = Number(result?.deletedSessions || 0);
    const deletedEvents = Number(result?.deletedEvents || 0);
    toast(
      deletedSessions > 0
        ? `История запусков очищена: удалено сессий ${deletedSessions}, событий ${deletedEvents}`
        : "Удалять нечего: защищены только активная или текущая плановая сессия"
    );
  } catch (error) {
    toast(`Не удалось очистить историю запусков: ${error?.message || "ошибка backend"}`);
  }
}

async function refreshTemplatesFromBackend(options = {}) {
  const { silent = false } = options;
  try {
    const data = await fetchApiJson("/api/templates");
    state.templates = Array.isArray(data) ? data.map(mapTemplateDtoToUi) : [];
    if (!state.templateCreateMode) {
      const selectedExists = state.selectedTemplateId !== null &&
        state.templates.some((tpl) => tpl.id === state.selectedTemplateId);
      if (!selectedExists) {
        state.selectedTemplateId = state.templates.length > 0 ? state.templates[0].id : null;
      }
    }

    if (!state.templateCreateMode && state.selectedTemplateId !== null) {
      loadTemplateToEditor(state.selectedTemplateId);
    } else {
      renderTemplates();
      renderTemplateEditorState();
    }
    renderTemplateRuleTypeSettings();
    renderTemplateCommentSettings();
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
  const hasExplicitRunSessionId = Object.prototype.hasOwnProperty.call(options, "runSessionId");
  const preferredRunSessionId = hasExplicitRunSessionId
    ? Number(runSessionId || 0)
    : Number(state.runHistorySelectedSessionId || 0);
  const requestedSessionId = Number.isFinite(preferredRunSessionId) && preferredRunSessionId > 0
    ? preferredRunSessionId
    : 0;
  try {
    const path = requestedSessionId > 0
      ? `/api/queue?runSessionId=${encodeURIComponent(requestedSessionId)}&limit=5000&offset=0`
      : "/api/queue?limit=5000&offset=0";
    const data = await fetchApiJson(path);
    if (!data?.hasSession) {
      state.queue = [];
      state.selectedQueue.clear();
      state.planPrepared = false;
      state.queueSessionId = null;
      state.queueSession = null;
      if (requestedSessionId > 0) {
        state.runHistorySelectedSessionId = null;
      }
      resetDialogPreview();
      renderQueue();
      renderRunHistory();
      renderDialogPreviewPanel();
      updateMetrics();
      renderDbSyncState();
      return true;
    }

    state.queueSessionId = Number(data.session?.id || 0);
    state.queueSession = data.session || null;
    if (requestedSessionId > 0) {
      state.runHistorySelectedSessionId = state.queueSessionId || requestedSessionId;
    } else if (state.runHistorySelectedSessionId && Number(state.runHistorySelectedSessionId) !== Number(state.queueSessionId || 0)) {
      state.runHistorySelectedSessionId = null;
    }
    state.queue = (Array.isArray(data.items) ? data.items : []).map(mapQueueJobDtoToUi);
    state.planPrepared = Number(data.totalJobsInSession || 0) > 0;
    syncDialogPreviewWithQueue();
    renderQueue();
    renderRunHistory();
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
  const selectedSessionStatus = String(state.queueSession?.status || "").trim().toLowerCase();
  const resumingStoppedSession = selectedSessionStatus === "stopped";
  if (!resumingStoppedSession && !state.clientsDb.actualized) {
    toast("Сначала выполните «Актуализировать базу клиентов»");
    return;
  }
  if (!resumingStoppedSession && !hasExpectedSnapshotModeLoaded()) {
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
  if (state.planStale && !resumingStoppedSession) {
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
    await refreshPlanningSessionViews({
      silent: true,
      runSessionId: currentQueueRunSessionId(),
      includeStatus: false,
      includeHistory: true,
      includeForecast: true
    });
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
      runSessionId: currentQueueRunSessionId(),
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
    await refreshPlanningSessionViews({
      silent: true,
      runSessionId: currentQueueRunSessionId(),
      includeStatus: false,
      includeHistory: true,
      includeForecast: true
    });
    addRunLog(result?.message || reason);
  } catch (error) {
    toast(`Не удалось остановить цикл: ${error?.message || "ошибка backend"}`);
  } finally {
    renderDbSyncState();
  }
}

async function shutdownApplicationFromUi() {
  const message = state.runRuntime
    ? "Сейчас выполняется рассылка. Остановить цикл и завершить приложение?"
    : "Завершить приложение?";
  const confirmed = window.confirm(message);
  if (!confirmed) return;

  if (state.runRuntime) {
    await stopRun("Остановлено оператором при завершении приложения.");
  }

  try {
    const result = await fetchApiJson("/api/app/shutdown", {
      method: "POST"
    });
    addRunLog(result?.message || "Приложение завершает работу.");
    toast("Приложение завершает работу");
    setTimeout(() => {
      try {
        window.close();
      } catch {
        // no-op
      }
    }, 400);
  } catch (error) {
    toast(`Не удалось завершить приложение: ${error?.message || "ошибка backend"}`);
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
  const parsedDebtBufferAmount = Number(draft.debtBufferAmount);
  const payload = {
    loginUrl: draft.loginUrl,
    login: draft.login,
    password: draft.password,
    gap: Math.max(1, Math.round(Number(draft.gap) || 8)),
    debtBufferAmount: Number.isFinite(parsedDebtBufferAmount)
      ? Math.max(0, Math.min(1000000, Math.round(parsedDebtBufferAmount)))
      : DEFAULT_DEBT_BUFFER_AMOUNT,
    recentSmsCooldownDays: Math.max(0, Math.min(365, Math.round(Number(draft.recentSmsCooldownDays) || 0))),
    allowLiveDispatch: draft.allowLiveDispatch !== false,
    workWindowStart: draft.workWindowStart,
    workWindowEnd: draft.workWindowEnd,
    commentRules: {
      sms2: draft.commentRules.sms2,
      sms3: draft.commentRules.sms3,
      ka1: draft.commentRules.ka1,
      kaN: draft.commentRules.kaN,
      kaFinal: draft.commentRules.kaFinal
    },
    templateRuleTypes: draft.templateRuleTypes.map((item) => ({
      id: item.id,
      name: item.name,
      overdueMode: item.overdueMode,
      overdueFromDays: item.overdueFromDays,
      overdueToDays: item.overdueToDays,
      overdueExactDay: item.overdueExactDay,
      autoAssign: item.autoAssign !== false,
      sortOrder: item.sortOrder
    }))
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
      debtBufferAmount: saved.debtBufferAmount,
      recentSmsCooldownDays: saved.recentSmsCooldownDays,
      allowLiveDispatch: saved.allowLiveDispatch,
      workWindowStart: saved.workWindowStart,
      workWindowEnd: saved.workWindowEnd,
      commentRules: saved.commentRules,
      templateRuleTypes: saved.templateRuleTypes
    });
    if (saved.commentRules) {
      state.commentRules = normalizeCommentRules(saved.commentRules);
    }
    const templatesRefreshed = await refreshTemplatesFromBackend({ silent: true });
    if (!templatesRefreshed && !silent) {
      toast("Настройки сохранены, но не удалось обновить список шаблонов. Обновите вкладку «Шаблоны».");
    }
    setSettingsBaselineFromUI();
    renderConfiguredOverdueFilters({ preserveSelection: true });
    collectRunFiltersFromUI();
    renderRunFilterSummary();
    await refreshDebtDependentViews({ silent: true });
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
      debtBufferAmount: data.debtBufferAmount,
      recentSmsCooldownDays: data.recentSmsCooldownDays,
      allowLiveDispatch: data.allowLiveDispatch,
      workWindowStart: data.workWindowStart,
      workWindowEnd: data.workWindowEnd,
      commentRules: data.commentRules,
      templateRuleTypes: data.templateRuleTypes
    });
    if (data.commentRules) {
      state.commentRules = normalizeCommentRules(data.commentRules);
    }
  } catch (error) {
    addRunLog(`Не удалось загрузить настройки из backend: ${error?.message || "ошибка"}. Используются текущие значения формы.`);
    toast("Не удалось загрузить настройки из backend");
  }

  setSettingsBaselineFromUI();
  renderConfiguredOverdueFilters({ preserveSelection: true });
  collectRunFiltersFromUI();
  renderRunFilterSummary();
  renderTemplateRuleTypeSettings();
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
    const result = await fetchApiJson(`/api/channels/${encodeURIComponent(channelId)}/check?timeoutMs=15000`, {
      method: "POST"
    });
    const detail = String(result?.detail || "").trim();
    const detailSuffix = detail ? ` (${detail})` : "";
    if (result?.status === "online") {
      if (!silent) toast(`${channel.name}: канал работает${detailSuffix}`);
      return true;
    }
    if (result?.status === "offline") {
      if (!silent) toast(`${channel.name}: канал отключен вручную${detailSuffix}`);
      return false;
    }
    if (result?.status === "unknown") {
      if (!silent) toast(`${channel.name}: gateway достижим, но probe-проверка неинформативна${detailSuffix}`);
      return true;
    }
    if (!silent) toast(`${channel.name}: канал не прошел проверку${detailSuffix}`);
    return false;
  } catch (error) {
    if (!silent) toast(`${channel.name}: ошибка проверки канала (${error?.message || "backend"})`);
    return false;
  } finally {
    await refreshChannelMonitoringViews({ silent: true, includeForecast: true, rerenderQueue: true });
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
    await refreshChannelMonitoringViews({ silent: true, includeForecast: true, rerenderQueue: true });

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
    const result = await fetchApiJson("/api/channels/check?timeoutMs=15000", { method: "POST" });
    await refreshChannelMonitoringViews({ silent: true });
    const online = Number(result?.online || 0);
    const unknown = Number(result?.unknown || 0);
    const total = Number(result?.total || state.channels.length);
    if (unknown > 0) {
      toast(`Проверка каналов завершена: онлайн ${online}, неинформативный probe ${unknown}, всего ${total}`);
      return;
    }
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
    const queueRemoval = await removePendingQueueJobsByPhones([normalized], { silent: true });
    if (!deferRender) {
      refreshAfterStopListChange({ stopListAlreadyRendered: true, dialogsAlreadyRendered: true });
    }
    if (!silent && Number(queueRemoval?.removed ?? 0) > 0) {
      addRunLog(`Из очереди удалено pending-задач: ${Number(queueRemoval?.removed ?? 0)} (stop-list ${normalized}).`);
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
      refreshAfterStopListChange({ stopListAlreadyRendered: true, dialogsAlreadyRendered: true });
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
  const phones = getValidatedStopListPhones(selected, {
    emptySelectionMessage: "Сначала выберите клиентов в таблице",
    emptyPhoneMessage: "У выбранных клиентов нет валидных телефонов",
    membership: "outside",
    membershipErrorMessage: "Операция недоступна: среди выбранных есть клиенты, уже добавленные в стоп-лист."
  });
  if (!phones) return;

  await bulkAddPhonesToStopList({
    phones,
    reason: "Добавлено массово из базы клиентов",
    source: "База клиентов (массово)",
    selectionSet: state.selectedClients,
    renderSelection: renderClientsDb,
    emptyResultMessage: "Выбранные номера уже в стоп-листе"
  });
}

async function bulkRemoveSelectedClientsFromStopList() {
  const selected = getSelectedClientRows();
  const phones = getValidatedStopListPhones(selected, {
    emptySelectionMessage: "Сначала выберите клиентов в таблице",
    emptyPhoneMessage: "У выбранных клиентов нет валидных телефонов",
    membership: "inside",
    membershipErrorMessage: "Операция недоступна: среди выбранных есть клиенты, которых нет в стоп-листе."
  });
  if (!phones) return;

  await bulkRemovePhonesFromStopList({
    phones,
    selectionSet: state.selectedClients,
    renderSelection: renderClientsDb,
    emptyResultMessage: "Выбранные номера не найдены в стоп-листе"
  });
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
  const returnableCount = selected.filter((client) => state.excludedClientIds.has(client.id)).length;
  if (returnableCount !== selected.length) {
    toast("Операция недоступна: среди выбранных есть клиенты, которые уже находятся в плане.");
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
  const phones = getValidatedStopListPhones(selected, {
    emptySelectionMessage: "Сначала выберите клиентов в очереди",
    emptyPhoneMessage: "У выбранных клиентов нет валидных телефонов",
    membership: "outside",
    membershipErrorMessage: "Операция недоступна: среди выбранных есть клиенты, уже добавленные в стоп-лист."
  });
  if (!phones) return;

  await bulkAddPhonesToStopList({
    phones,
    reason: "Добавлено массово из очереди",
    source: "Очередь (массово)",
    selectionSet: state.selectedQueue,
    renderSelection: renderQueue,
    emptyResultMessage: "Выбранные номера уже в стоп-листе"
  });
}

async function bulkRemoveSelectedQueueFromStopList() {
  const selected = getSelectedQueueRows();
  const phones = getValidatedStopListPhones(selected, {
    emptySelectionMessage: "Сначала выберите клиентов в очереди",
    emptyPhoneMessage: "У выбранных клиентов нет валидных телефонов",
    membership: "inside",
    membershipErrorMessage: "Операция недоступна: среди выбранных есть клиенты, которых нет в стоп-листе."
  });
  if (!phones) return;

  await bulkRemovePhonesFromStopList({
    phones,
    selectionSet: state.selectedQueue,
    renderSelection: renderQueue,
    emptyResultMessage: "Выбранные номера не найдены в стоп-листе"
  });
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
    refreshAfterStopListChange({ stopListAlreadyRendered: true, dialogsAlreadyRendered: true });
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

    const resolvedRunSessionId = normalizeOptionalRunSessionId(result?.runSessionId || state.queueSessionId);
    await refreshPlanningSessionViews({
      silent: true,
      runSessionId: resolvedRunSessionId
    });
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

function getRemovableQueueJobIdsByPhones(phones) {
  const phoneSet = new Set(
    (phones || [])
      .map((phone) => normalizePhone(phone))
      .filter(Boolean)
  );
  if (phoneSet.size === 0) return [];

  return state.queue
    .filter((job) =>
      phoneSet.has(normalizePhone(job.phone)) &&
      ["queued", "retry", "stopped"].includes(job.status))
    .map((job) => job.id);
}

async function removePendingQueueJobsByPhones(phones, options = {}) {
  const jobIds = getRemovableQueueJobIdsByPhones(phones);
  if (jobIds.length === 0) {
    return { removed: 0, skipped: 0, remainingJobs: state.queue.length, runSessionId: state.queueSessionId || 0 };
  }

  return removeQueueJobsFromPlan(jobIds, options);
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
  const { silent = false, skipUiRefresh = false } = options;
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

    if (!skipUiRefresh) {
      await refreshDebtDependentViews({ silent: true });
    }
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

function collectExternalClientIds(rows) {
  return Array.from(new Set(
    (rows || [])
      .map((row) => String(row?.externalClientId || "").trim())
      .filter((id) => id.length > 0)
  ));
}

async function bulkFetchDebtByRows(rows, options = {}) {
  const { emptySelectionMessage, emptyExternalIdMessage } = options;
  if (state.bulkDebtInProgress) {
    toast("Обновление суммы долга уже выполняется. Дождитесь завершения.");
    return;
  }
  if (!Array.isArray(rows) || rows.length === 0) {
    toast(emptySelectionMessage || "Сначала выберите клиентов");
    return;
  }

  const externalIds = collectExternalClientIds(rows);
  const missingExternalIdCount = rows.filter((row) => String(row?.externalClientId || "").trim() === "").length;
  if (missingExternalIdCount > 0) {
    toast("Операция недоступна: у части выбранных клиентов нет externalClientId. Оставьте только клиентов, для которых действие применимо.");
    return;
  }
  if (externalIds.length === 0) {
    toast(emptyExternalIdMessage || "У выбранных клиентов отсутствует externalClientId");
    return;
  }

  state.bulkDebtInProgress = true;
  renderClientsBulkUi();
  renderQueueBulkUi();
  try {
    let success = 0;
    let failed = 0;
    for (const externalId of externalIds) {
      const result = await fetchDebtByExternalClientId(externalId, { silent: true, skipUiRefresh: true });
      if (result) {
        success += 1;
      } else {
        failed += 1;
      }
    }

    await refreshDebtDependentViews({ silent: true });
    renderClientsDb();
    renderQueue();

    if (failed === 0) {
      toast(`Сумма долга обновлена для ${success} клиентов`);
      return;
    }
    toast(`Сумма долга обновлена для ${success} клиентов, ошибок: ${failed}`);
  } finally {
    state.bulkDebtInProgress = false;
    renderClientsBulkUi();
    renderQueueBulkUi();
  }
}

async function bulkFetchDebtForSelectedClients() {
  await bulkFetchDebtByRows(getSelectedClientRows(), {
    emptySelectionMessage: "Сначала выберите клиентов в базе",
    emptyExternalIdMessage: "У выбранных клиентов нет externalClientId для запроса суммы"
  });
}

async function bulkFetchDebtForSelectedQueue() {
  await bulkFetchDebtByRows(getSelectedQueueRows(), {
    emptySelectionMessage: "Сначала выберите клиентов в очереди",
    emptyExternalIdMessage: "У выбранных задач нет externalClientId для запроса суммы"
  });
}

async function bulkRemoveSelectedFromPlan() {
  const selected = getSelectedQueueRows();
  if (selected.length === 0) {
    toast("Сначала выберите клиентов в очереди");
    return;
  }
  const allRemovable = selected.every((q) => QUEUE_REMOVABLE_STATUSES.has(q.status));
  if (!allRemovable) {
    toast("Операция недоступна: среди выбранных есть задачи, которые нельзя удалить из плана.");
    return;
  }
  const removableJobIds = new Set(selected.map((q) => q.id));
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
  const hasInvalidStatus = selected.some((q) => !["queued", "retry"].includes(q.status));
  if (hasInvalidStatus) {
    toast("Операция недоступна: шаблон можно массово назначать только задачам со статусом «В очереди» или «Повтор».");
    return;
  }
  const hasOverdueMismatch = selected.some((q) => !canApplyTemplateToOverdue(template, q.daysOverdue));
  if (hasOverdueMismatch) {
    toast(`Операция недоступна: шаблон «${templateDisplayName(template)}» не подходит части выбранных клиентов по правилу просрочки.`);
    return;
  }
  const removableJobIds = selected.map((q) => q.id);
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
    await refreshPlanningSessionViews({ silent: true, includeStatus: false });
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

async function retryQueueErrors() {
  if (state.runRuntime) {
    renderQueueRetryActionState();
    toast("Переотправка ошибок недоступна во время активного запуска");
    return;
  }

  const retryable = state.queue.filter((q) => QUEUE_RETRYABLE_STATUSES.has(q.status)).length;
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

    await refreshPlanningSessionViews({ silent: true });
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
    await refreshChannelMonitoringViews({ silent: true });
    toast(`Не удалось переотправить задачи: ${error?.message || "ошибка backend"}`);
  }
}

async function addSelectedDialogPhoneToStopList() {
  const dialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
  if (!dialog) return;

  const ok = await addPhoneToStopList(dialog.phone, "Добавлено вручную из диалога", "Диалог");
  if (ok) {
    toast("Номер клиента добавлен в стоп-лист");
  }
}

async function sendManualMessageFromSelectedDialog() {
  const dialog = state.dialogs.find((d) => String(d.id) === String(state.selectedDialogId));
  if (!dialog) return;
  if (isPhoneInStopList(dialog.phone)) {
    toast("Ручная отправка запрещена: номер находится в стоп-листе");
    return;
  }

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

    await refreshDialogWorkspaceByPhone(dialog.phone, { silent: true });
    await refreshChannelMonitoringViews({ silent: true, includeReports: true });

    addRunLog(`Ручное сообщение отправлено клиенту ${dialog.phone} через канал ${result?.channelName || "-"}.`);
    toast("Ручное сообщение отправлено");
  } catch (error) {
    toast(`Не удалось отправить ручное сообщение: ${error?.message || "ошибка backend"}`);
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

  UI.bindDelegated("typeDefBody", {
    "type-def-open": (btn) => {
      const nextId = normalizeTemplateRuleTypeId(btn.dataset.typeId || "");
      if (!nextId) return;
      if (state.templateRuleTypeCreateMode || hasTemplateRuleTypeUnsavedChanges()) {
        const confirmed = window.confirm("Есть несохраненные изменения в типе шаблона. Перейти к другому типу без сохранения?");
        if (!confirmed) return;
      }
      exitTemplateRuleTypeCreateMode();
      state.selectedTemplateRuleTypeId = nextId;
      renderTemplateRuleTypeSettings();
    }
  });

  UI.bindDelegated("cfgTemplateCommentBody", {
    "template-comment-open": (btn) => {
      const templateId = Number(btn.dataset.tplId || 0);
      const ok = trySelectTemplateCommentTemplate(templateId);
      if (!ok) return;
      const input = $("cfgTemplateCommentText");
      if (input) {
        input.focus();
        input.selectionStart = input.selectionEnd = input.value.length;
      }
    }
  });

  UI.bindDelegated("clientsDbBody", {
    "client-open-dialog": (btn) => {
      void openDialogByPhone(btn.dataset.clientPhone || "");
    },
    "client-toggle-stop": async (btn) => {
      const phone = btn.dataset.clientPhone || "";
      if (!normalizePhone(phone)) {
        toast("Операция недоступна: у клиента нет валидного номера телефона.");
        return;
      }
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
    "queue-toggle-stop": async (btn) => {
      const phone = btn.dataset.qPhone || "";
      if (!normalizePhone(phone)) {
        toast("Операция недоступна: у клиента нет валидного номера телефона.");
        return;
      }
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
      toast(`Шаблон «${templateDisplayName(nextTemplate)}» не подходит клиенту ${job.phone} по правилу просрочки`);
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

  UI.bindDelegated("runHistoryBody", {
    "run-history-open": async (btn) => {
      const sessionId = Number(btn.dataset.runSessionId || 0);
      if (!sessionId) return;
      await openQueueFromHistorySession(sessionId, { switchToQueueTab: true });
    }
  });

  $("runStart").addEventListener("click", () => {
    void startRun();
  });
  $("runSyncDb").addEventListener("click", () => {
    void syncClientsDatabase();
  });
  if ($("runHistoryRefresh")) {
    $("runHistoryRefresh").addEventListener("click", () => {
      void refreshRunHistoryFromBackend();
    });
  }
  if ($("runHistoryClear")) {
    $("runHistoryClear").addEventListener("click", () => {
      void clearRunHistory();
    });
  }
  if ($("runHistoryOpenLatest")) {
    $("runHistoryOpenLatest").addEventListener("click", () => {
      void openLatestQueueSession({ switchToQueueTab: true });
    });
  }
  $("globalStart").addEventListener("click", () => {
    void startRun();
  });
  $("globalStop").addEventListener("click", () => {
    void stopRun();
  });
  const globalExitBtn = $("globalExit");
  if (globalExitBtn) {
    globalExitBtn.addEventListener("click", () => {
      void shutdownApplicationFromUi();
    });
  }
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
  document.querySelectorAll(".run-tz").forEach((el) => {
    el.addEventListener("change", () => {
      refreshRunFiltersUI();
    });
  });
  if ($("runOverdueFilterBlock")) {
    $("runOverdueFilterBlock").addEventListener("change", () => {
      refreshRunFiltersUI();
    });
  }
  $("runExactDay").addEventListener("input", (event) => {
    const input = event.target;
    if (!(input instanceof HTMLInputElement)) {
      refreshRunFiltersUI();
      return;
    }
    const normalized = normalizeRunExactOverdueInput(input.value, { strict: false });
    if (input.value !== normalized) {
      input.value = normalized;
    }
    refreshRunFiltersUI();
  });
  $("runExactDay").addEventListener("blur", (event) => {
    const input = event.target;
    if (!(input instanceof HTMLInputElement)) return;
    const normalized = normalizeRunExactOverdueInput(input.value, { strict: true });
    if (input.value !== normalized) {
      input.value = normalized;
      refreshRunFiltersUI();
    }
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
      if (hasTemplateRuleTypeUnsavedChanges()) {
        const typeSaved = saveTemplateRuleTypeFromEditor();
        if (!typeSaved) return;
      }
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
    renderRunFilterSummary();
  });
  if ($("cfgDebtBufferAmount")) {
    $("cfgDebtBufferAmount").addEventListener("input", () => {
      renderClientsDb();
      renderQueue();
      renderDialogs();
      renderChat();
    });
  }
  if ($("cfgRecentSmsCooldownDays")) {
    $("cfgRecentSmsCooldownDays").addEventListener("input", () => {
      void refreshRunForecastFromBackend({ silent: true });
      renderRunFilterSummary();
    });
  }
  $("cfgWorkWindowStart").addEventListener("input", () => {
    void refreshRunForecastFromBackend({ silent: true });
  });
  $("cfgWorkWindowEnd").addEventListener("input", () => {
    void refreshRunForecastFromBackend({ silent: true });
  });
  if ($("cfgTemplateCommentTemplate")) {
    $("cfgTemplateCommentTemplate").addEventListener("change", () => {
      const nextId = Number($("cfgTemplateCommentTemplate").value || 0) || null;
      const ok = trySelectTemplateCommentTemplate(nextId);
      if (!ok) {
        renderTemplateCommentSettings();
      }
    });
  }
  if ($("cfgTemplateCommentText")) {
    $("cfgTemplateCommentText").addEventListener("input", () => {
      renderTemplateCommentSettingsState();
    });
  }
  if ($("cfgTemplateCommentClear")) {
    $("cfgTemplateCommentClear").addEventListener("click", () => {
      const input = $("cfgTemplateCommentText");
      if (!input) return;
      input.value = "";
      renderTemplateCommentSettingsState();
      input.focus();
    });
  }
  if ($("cfgTemplateCommentSave")) {
    $("cfgTemplateCommentSave").addEventListener("click", async () => {
      await saveTemplateCommentFromSettings();
    });
  }
  if ($("typeDefName")) {
    $("typeDefName").addEventListener("input", renderTemplateRuleTypeEditorState);
  }
  if ($("typeDefMode")) {
    $("typeDefMode").addEventListener("change", () => {
      renderTemplateRuleTypeEditorState();
    });
  }
  if ($("typeDefFrom")) {
    $("typeDefFrom").addEventListener("input", renderTemplateRuleTypeEditorState);
  }
  if ($("typeDefTo")) {
    $("typeDefTo").addEventListener("input", renderTemplateRuleTypeEditorState);
  }
  if ($("typeDefExact")) {
    $("typeDefExact").addEventListener("input", renderTemplateRuleTypeEditorState);
  }
  if ($("typeDefAutoAssign")) {
    $("typeDefAutoAssign").addEventListener("change", renderTemplateRuleTypeEditorState);
  }
  if ($("typeDefNew")) {
    $("typeDefNew").addEventListener("click", () => {
      if (hasTemplateRuleTypeUnsavedChanges()) {
        const confirmed = window.confirm("Есть несохраненные изменения в типе шаблона. Создать новый тип без сохранения текущих изменений?");
        if (!confirmed) return;
      }
      createTemplateRuleType();
      toast("Режим создания нового типа шаблона включен");
    });
  }
  if ($("typeDefSave")) {
    $("typeDefSave").addEventListener("click", () => {
      const ok = saveTemplateRuleTypeFromEditor();
      if (!ok) return;
      toast("Тип шаблона сохранен. Нажмите «Сохранить настройки», чтобы применить изменения.");
    });
  }
  if ($("typeDefCancel")) {
    $("typeDefCancel").addEventListener("click", () => {
      const ok = cancelTemplateRuleTypeChanges();
      if (!ok) return;
      toast("Изменения типа шаблона отменены");
    });
  }
  if ($("typeDefDelete")) {
    $("typeDefDelete").addEventListener("click", () => {
      const current = getTemplateRuleTypeById(state.selectedTemplateRuleTypeId);
      if (!current) {
        toast("Сначала выберите тип шаблона");
        return;
      }
      const confirmed = window.confirm(`Удалить тип «${current.name}»?`);
      if (!confirmed) return;
      const ok = deleteSelectedTemplateRuleType();
      if (!ok) return;
      toast("Тип шаблона удален. Нажмите «Сохранить настройки», чтобы применить изменения.");
    });
  }
  $("tplType").addEventListener("change", () => {
    ensureTemplateCreateModeFromEditorInput();
    const selectedKind = normalizeTemplateKind($("tplType").value, { allowMissing: true });
    $("tplType").value = selectedKind;
    applyTemplateTypeDefaultsToEditor(selectedKind);
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
  $("clientsBulkFetchDebt").addEventListener("click", () => {
    void bulkFetchDebtForSelectedClients();
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
  $("queueBulkFetchDebt").addEventListener("click", () => {
    void bulkFetchDebtForSelectedQueue();
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
  $("queueBulkTemplate").addEventListener("change", () => {
    renderQueueBulkUi();
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
  if ($("dialogsSearch")) {
    $("dialogsSearch").addEventListener("input", () => {
      renderDialogs();
      renderChat();
    });
  }
  $("dialogPruneDays").addEventListener("input", () => {
    const value = Number($("dialogPruneDays").value);
    if (Number.isFinite(value) && value >= 1) {
      $("dialogPruneDays").value = String(Math.floor(value));
    }
  });
  $("queueRetryErrors").addEventListener("click", () => {
    void retryQueueErrors();
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

  $("toStopList").addEventListener("click", () => {
    void addSelectedDialogPhoneToStopList();
  });

  $("manualSend").addEventListener("click", () => {
    void sendManualMessageFromSelectedDialog();
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
  const refreshResults = await Promise.allSettled([
    refreshTemplatesFromBackend({ silent: true }),
    refreshManualPresetsFromBackend({ silent: true }),
    refreshChannelsFromBackend({ silent: true }),
    refreshStopListFromBackend({ silent: true }),
    refreshClientsSnapshotFromBackend({ silent: true }),
    refreshRunStatusFromBackend({ silent: true }),
    refreshRunHistoryFromBackend({ silent: true }),
    refreshQueueFromBackend({ silent: true }),
    refreshDialogsFromBackend({ silent: true }),
    refreshAlertsFromBackend({ silent: true }),
    refreshReportsFromBackend({ silent: true })
  ]);
  refreshResults.forEach((result) => {
    if (result.status !== "rejected") return;
    addRunLog(`Часть данных интерфейса не загрузилась: ${result.reason?.message || "ошибка фронтенда"}`);
  });
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
  renderRunHistory();
  renderTemplateRuleTypeSettings();
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
