function renderClientsBulkUi(visibleRows = getVisibleClientRows()) {
  const visibleIds = visibleRows.map((c) => c.id);
  const hasAnyRows = state.clients.length > 0;
  const selectedRows = getSelectedClientRows();
  const selectedTotal = selectedRows.length;
  const selectedVisible = visibleIds.filter((id) => state.selectedClients.has(id)).length;
  const selectedWithExternalId = selectedRows.filter((c) => String(c.externalClientId || "").trim() !== "").length;
  const selectedWithPhone = selectedRows.filter((c) => normalizePhone(c.phone)).length;
  const returnableCount = selectedRows.filter((c) => state.excludedClientIds.has(c.id)).length;
  const selectedInStopList = selectedRows.filter((c) => isPhoneInStopList(c.phone)).length;
  const selectedOutStopList = selectedTotal - selectedInStopList;
  const allWithExternalId = selectedTotal > 0 && selectedWithExternalId === selectedTotal;
  const allWithPhone = selectedTotal > 0 && selectedWithPhone === selectedTotal;
  const canBulkAddToStopList = allWithPhone && selectedOutStopList === selectedTotal;
  const canBulkRemoveFromStopList = allWithPhone && selectedInStopList === selectedTotal;
  const canBulkFetchDebt = allWithExternalId && !state.bulkDebtInProgress;
  const canBulkReturnToPlan = selectedTotal > 0 && returnableCount === selectedTotal && !state.runRuntime;

  $("clientsBulkMeta").textContent = `Выбрано: ${selectedTotal} (видимо: ${selectedVisible})`;
  $("clientsSelectVisible").disabled = !hasAnyRows;
  $("clientsClearSelection").disabled = selectedTotal === 0;
  $("clientsBulkFetchDebt").disabled = !canBulkFetchDebt;
  $("clientsBulkAddStop").disabled = !canBulkAddToStopList;
  $("clientsBulkRemoveStop").disabled = !canBulkRemoveFromStopList;
  $("clientsBulkReturnPlan").disabled = !canBulkReturnToPlan;

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
  const selectedWithExternalId = selectedRows.filter((q) => String(q.externalClientId || "").trim() !== "").length;
  const selectedWithPhone = selectedRows.filter((q) => normalizePhone(q.phone)).length;
  const selectedInStopList = selectedRows.filter((q) => isPhoneInStopList(q.phone)).length;
  const selectedOutStopList = selectedTotal - selectedInStopList;
  const allWithExternalId = selectedTotal > 0 && selectedWithExternalId === selectedTotal;
  const allWithPhone = selectedTotal > 0 && selectedWithPhone === selectedTotal;
  const allRemovableFromPlan = selectedTotal > 0 && selectedRows.every((q) => QUEUE_REMOVABLE_STATUSES.has(q.status));
  const allTemplateAssignableByStatus = selectedTotal > 0 && selectedRows.every((q) => QUEUE_TEMPLATE_ASSIGNABLE_STATUSES.has(q.status));
  const canBulkFetchDebt = allWithExternalId && !state.bulkDebtInProgress;
  const canBulkAddToStopList = allWithPhone && selectedOutStopList === selectedTotal;
  const canBulkRemoveFromStopList = allWithPhone && selectedInStopList === selectedTotal;
  let canBulkSetTemplate = !state.runRuntime && allTemplateAssignableByStatus;

  $("queueBulkMeta").textContent = `Выбрано: ${selectedTotal} (видимо: ${selectedVisible})`;
  $("queueSelectVisible").disabled = !hasAnyRows;
  $("queueClearSelection").disabled = selectedTotal === 0;
  $("queueBulkFetchDebt").disabled = !canBulkFetchDebt;
  $("queueBulkAddStop").disabled = !canBulkAddToStopList;
  $("queueBulkRemoveStop").disabled = !canBulkRemoveFromStopList;
  $("queueBulkRemovePlan").disabled = Boolean(state.runRuntime) || !allRemovableFromPlan;

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
    canBulkSetTemplate = false;
  } else {
    const current = templateSelect.value;
    templateSelect.innerHTML = activeTemplates.map((tpl) => `
      <option value="${tpl.id}">${escapeHtml(templateOptionLabel(tpl))}</option>
    `).join("");
    if (current && activeTemplates.some((tpl) => String(tpl.id) === current)) {
      templateSelect.value = current;
    }
    templateSelect.disabled = false;

    const selectedTemplateId = Number(templateSelect.value || 0);
    const selectedTemplate = getTemplateById(selectedTemplateId);
    if (!selectedTemplate) {
      canBulkSetTemplate = false;
    } else if (selectedTotal > 0) {
      const allOverdueCompatible = selectedRows.every((q) => canApplyTemplateToOverdue(selectedTemplate, q.daysOverdue));
      if (!allOverdueCompatible) {
        canBulkSetTemplate = false;
      }
    }
  }
  $("queueBulkSetTemplate").disabled = !canBulkSetTemplate;

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
  const sessionStatus = String(state.queueSession?.status || "").trim().toLowerCase();

  retryBtn.disabled = totalRetryable === 0 || Boolean(state.runRuntime);
  if (hint) {
    const baseText = totalRetryable > 0
      ? `Переотправляются задачи со статусами «Ошибка» и «Остановлено». Сейчас: ошибка ${failedCount}, остановлено ${stoppedCount}.`
      : "Переотправляются задачи со статусами «Ошибка» и «Остановлено». Сейчас задач для переотправки нет.";
    const resumeText = sessionStatus === "stopped"
      ? " Сессия остановлена: после переотправки можно продолжить рассылку кнопкой «Старт»."
      : "";
    hint.textContent = `${baseText}${resumeText}`;
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
  const exactInput = $("runExactDay");
  const normalizedExact = normalizeRunExactOverdueInput(exactInput?.value || "", { strict: true });
  if (exactInput && exactInput.value !== normalizedExact) {
    exactInput.value = normalizedExact;
  }
  state.runFilters.exactOverdue = normalizedExact;
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
  const selectedSessionStatus = String(state.queueSession?.status || "").trim().toLowerCase();
  const resumingStoppedSession = selectedSessionStatus === "stopped";

  if (syncBtn) {
    syncBtn.textContent = "Актуализировать базу клиентов";
  }

  if (syncBtn) syncBtn.disabled = state.clientsDb.syncing || Boolean(state.runRuntime);
  if (stopBtn) stopBtn.disabled = !state.runRuntime;
  if (globalStopBtn) globalStopBtn.disabled = !state.runRuntime;
  const startBlocked =
    (!resumingStoppedSession && !state.clientsDb.actualized) ||
    state.clientsDb.syncing ||
    !state.planPrepared ||
    (state.planStale && !resumingStoppedSession) ||
    state.queue.length === 0 ||
    !state.runCanStart ||
    (!resumingStoppedSession && sourceModeMismatch);
  if (startBtn) startBtn.disabled = startBlocked;
  if (globalStartBtn) globalStartBtn.disabled = startBlocked;

  if (!statusEl) return;
  if (resumingStoppedSession) {
    setNotice("dbSyncStatus", "Открыта остановленная сессия из истории. Можно продолжить рассылку кнопкой «Старт».", "info");
    return;
  }
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
  const exactOverdue = parseRunExactOverdue(state.runFilters.exactOverdue);
  const recentSmsCooldownDays = getRecentSmsCooldownDays();
  const hasAnyFilter =
    state.runFilters.tz.size > 0 ||
    state.runFilters.overdueRanges.size > 0 ||
    Boolean(exactOverdue);
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
  const exact = exactOverdue?.normalized || "";
  const cooldownText = recentSmsCooldownDays > 0
    ? `, повторно не писать ${recentSmsCooldownDays} дн. после успешной SMS`
    : "";
  const text = exact
    ? `Фильтр: пояса [${tzText}], точная просрочка ${exact} дней${cooldownText}`
    : `Фильтр: пояса [${tzText}], фильтры просрочки [${rangesText}]${cooldownText}`;
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
  const excludedByRecentSms = Math.max(0, Number(preview.excludedByRecentSms || 0));
  const recentSmsCooldownDays = Math.max(0, Number(preview.appliedFilter?.recentSmsCooldownDays || 0));

  $("planClients").textContent = String(jobsCount);
  $("planGap").textContent = String(gapMinutes);
  $("planTzWait").textContent = formatDurationMinutes(tzWaitMinutes);
  $("planGapWait").textContent = formatDurationMinutes(gapWaitMinutes);
  $("planTotal").textContent = formatDurationMinutes(totalWaitMinutes);
  $("planFinish").textContent = jobsCount > 0 ? toMskDateTimeOrEmpty(finishAtUtc) : "--:--";

  if (jobsCount === 0) {
    const recentSmsHint = excludedByRecentSms > 0 && recentSmsCooldownDays > 0
      ? ` Исключено по недавней отправке SMS: ${excludedByRecentSms} за последние ${recentSmsCooldownDays} дн.`
      : "";
    setNotice("planHint", `По текущему фильтру нет клиентов для плановой очереди.${recentSmsHint}`, "warning");
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
  const recentSmsHint = excludedByRecentSms > 0 && recentSmsCooldownDays > 0
    ? ` Исключено по недавней отправке SMS: ${excludedByRecentSms} за последние ${recentSmsCooldownDays} дн.`
    : "";
  setNotice(
    "planHint",
    `Онлайн-каналов: ${onlineChannelsCount}, в расчете использовано: ${channelsUsed}. Рабочее окно ${workWindowLabel} учитывается для всех исходящих сообщений по локальному времени клиента.${recentSmsHint}`,
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

function toSafeCounter(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return 0;
  return Math.floor(parsed);
}

function buildRunHistoryProgress(item) {
  const sentJobs = toSafeCounter(item.sentJobs);
  const failedJobs = toSafeCounter(item.failedJobs);
  const stoppedJobs = toSafeCounter(item.stoppedJobs);
  const queuedJobs = toSafeCounter(item.queuedJobs);
  const runningJobs = toSafeCounter(item.runningJobs);
  const retryJobs = toSafeCounter(item.retryJobs);

  let totalJobs = toSafeCounter(item.totalJobs);
  if (totalJobs <= 0) {
    totalJobs = sentJobs + failedJobs + stoppedJobs + queuedJobs + runningJobs + retryJobs;
  }

  const doneJobs = sentJobs + failedJobs + stoppedJobs;
  const activeJobs = runningJobs + retryJobs;
  const donePercent = totalJobs > 0 ? Math.round((doneJobs / totalJobs) * 100) : 0;
  const activePercent = totalJobs > 0 ? Math.round((activeJobs / totalJobs) * 100) : 0;
  const safeDonePercent = Math.max(0, Math.min(100, donePercent));
  const safeActivePercent = Math.max(0, Math.min(100 - safeDonePercent, activePercent));

  return {
    totalJobs,
    sentJobs,
    failedJobs,
    stoppedJobs,
    queuedJobs,
    runningJobs,
    retryJobs,
    doneJobs,
    donePercent: safeDonePercent,
    activePercent: safeActivePercent
  };
}

function renderRunHistory() {
  const body = $("runHistoryBody");
  if (!body) return;

  const clearBtn = $("runHistoryClear");
  const refreshBtn = $("runHistoryRefresh");
  if (clearBtn) clearBtn.disabled = state.runHistory.length === 0;
  if (refreshBtn) refreshBtn.disabled = false;

  if (state.runHistory.length === 0) {
    body.innerHTML = '<tr><td colspan="7"><p class="muted-note">История запусков пока пуста.</p></td></tr>';
    return;
  }

  body.innerHTML = state.runHistory.map((item) => {
    const progress = buildRunHistoryProgress(item);
    const progressHeader = progress.totalJobs > 0
      ? `${progress.doneJobs} / ${progress.totalJobs}`
      : "0 / 0";
    const progressPercent = progress.totalJobs > 0 ? `${progress.donePercent}%` : "—";
    const progressDetail = progress.totalJobs > 0
      ? `Отправлено: ${progress.sentJobs}, ошибки: ${progress.failedJobs}, остановлено: ${progress.stoppedJobs}, в работе: ${progress.runningJobs + progress.retryJobs}, в очереди: ${progress.queuedJobs}`
      : "В этой сессии нет задач очереди";
    const progressDetailShort = progress.totalJobs > 0
      ? `Ок: ${progress.sentJobs} · Ош: ${progress.failedJobs} · Стоп: ${progress.stoppedJobs} · Раб: ${progress.runningJobs + progress.retryJobs} · Оч: ${progress.queuedJobs}`
      : "Нет задач";
    const notes = escapeHtml(item.notes || "—");
    const mode = escapeHtml(item.mode || "run");
    const isSelected = Number(state.runHistorySelectedSessionId || 0) === Number(item.id || 0);
    const openBtnText = isSelected ? "Открыто в очереди" : "Открыть в очереди";
    return `
      <tr class="${isSelected ? "active-row" : ""}">
        <td>
          #${item.id} <small class="muted-note">${mode}</small>
          <div class="actions slim run-history-row-actions">
            <button class="ghost-btn" data-action="run-history-open" data-run-session-id="${item.id}">${openBtnText}</button>
          </div>
        </td>
        <td>${toMskDateTimeOrEmpty(item.createdAtUtc) || "—"}</td>
        <td>${toMskDateTimeOrEmpty(item.startedAtUtc) || "—"}</td>
        <td>${toMskDateTimeOrEmpty(item.finishedAtUtc) || "—"}</td>
        <td><span class="pill ${escapeHtml(item.status || "planned")}">${escapeHtml(formatRunSessionStatus(item.status))}</span></td>
        <td class="run-history-progress-cell">
          <div class="run-history-progress" title="${escapeHtml(progressDetail)}">
            <div class="run-history-progress-head">
              <strong>${escapeHtml(progressHeader)}</strong>
              <span>${escapeHtml(progressPercent)}</span>
            </div>
            <div class="run-history-progress-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${progress.donePercent}">
              <span class="run-history-progress-bar-done" style="width:${progress.donePercent}%"></span>
              <span class="run-history-progress-bar-active" style="left:${progress.donePercent}%;width:${progress.activePercent}%"></span>
            </div>
            <div class="run-history-progress-meta">${escapeHtml(progressDetailShort)}</div>
          </div>
        </td>
        <td>${notes}</td>
      </tr>
    `;
  }).join("");
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
      <td><span class="channel-pill ${c.status}">${channelStatusText(c)}</span></td>
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
  $("templatesBody").innerHTML = state.templates.map((t) => {
    const nameText = escapeHtml(toSingleLineText(t.name));
    const typeText = escapeHtml(templateTypeLabel(t.kind, { allowMissing: true, fallbackTemplate: t }));
    const statusText = t.status === "active" ? "Актуальный" : "Черновик";
    const statusClass = t.status === "active" ? "active" : "draft";
    const ruleText = escapeHtml(formatTemplateOverdueRule(t));
    const commentRaw = String(t.commentText || "").trim();
    const commentFullText = toSingleLineText(commentRaw);
    const commentPreview = commentFullText
      ? escapeHtml(templateCommentPreview(commentFullText))
      : "-";
    const commentTitleAttr = commentFullText
      ? ` title="${escapeHtml(commentFullText)}"`
      : "";

    return `
      <tr class="${selectedId === t.id ? "active-row" : ""}" data-action="template-open" data-tpl-id="${t.id}">
        <td class="template-name-cell"><span title="${nameText}">${nameText}</span></td>
        <td>${typeText}</td>
        <td><span class="pill ${statusClass}">${statusText}</span></td>
        <td>${ruleText}</td>
        <td class="template-comment-cell"><span${commentTitleAttr}>${commentPreview}</span></td>
        <td><button class="ghost-btn" data-action="template-open" data-tpl-id="${t.id}">Открыть</button></td>
      </tr>
    `;
  }).join("");
}

function getTemplateById(templateId) {
  return state.templates.find((x) => x.id === templateId) || null;
}

function templateDisplayName(template) {
  return templateOptionLabel(template);
}

function canApplyTemplateToOverdue(template, daysOverdue) {
  if (!template) return false;
  return isTemplateEligibleForOverdue(template, daysOverdue, { allowManualOnly: true });
}

function buildTemplateRuleDraftFromType(kind, options = {}) {
  const type = getTemplateType(kind, {
    allowMissing: true,
    fallbackTemplate: options.fallbackTemplate || null
  });
  const mode = normalizeTemplateOverdueMode(type.overdueMode);
  const fromDays = Math.max(0, parseTemplateOptionalInt(type.overdueFromDays) ?? type.minOverdue ?? 0);
  const toDays = Math.max(fromDays, parseTemplateOptionalInt(type.overdueToDays) ?? type.maxOverdue ?? fromDays);
  const exactDay = Math.max(0, parseTemplateOptionalInt(type.overdueExactDay) ?? 0);
  return {
    overdueMode: mode,
    overdueFromDays: mode === "range" ? fromDays : null,
    overdueToDays: mode === "range" ? toDays : null,
    overdueExactDay: mode === "exact" ? exactDay : null,
    autoAssign: type.autoAssign !== false
  };
}

function resetTemplateCreateDraft() {
  const fallback = getTemplateType(DEFAULT_TEMPLATE_KIND);
  state.templateCreateDraft = {
    kind: fallback.id,
    ...buildTemplateRuleDraftFromType(fallback.id),
    name: "",
    text: ""
  };
}

function exitTemplateCreateMode() {
  state.templateCreateMode = false;
  resetTemplateCreateDraft();
}

function nextTemplateRuleTypeSortOrder() {
  if (state.templateRuleTypes.length === 0) return 10;
  const maxOrder = Math.max(...state.templateRuleTypes.map((item) => Number(item.sortOrder) || 0));
  return Math.max(10, maxOrder + 10);
}

function resetTemplateRuleTypeCreateDraft() {
  const fallback = getDefaultTemplateRuleType();
  state.templateRuleTypeCreateDraft = {
    id: "",
    name: "",
    overdueMode: fallback.overdueMode,
    overdueFromDays: fallback.overdueFromDays,
    overdueToDays: fallback.overdueToDays,
    overdueExactDay: fallback.overdueExactDay,
    autoAssign: fallback.autoAssign !== false,
    sortOrder: nextTemplateRuleTypeSortOrder()
  };
}

function exitTemplateRuleTypeCreateMode() {
  state.templateRuleTypeCreateMode = false;
  resetTemplateRuleTypeCreateDraft();
}

function ensureTemplateRuleTypeSelection() {
  if (state.templateRuleTypeCreateMode) return;
  if (state.templateRuleTypes.length === 0) {
    state.selectedTemplateRuleTypeId = null;
    return;
  }
  if (!getTemplateRuleTypeById(state.selectedTemplateRuleTypeId)) {
    state.selectedTemplateRuleTypeId = state.templateRuleTypes[0].id;
  }
}

function setTemplateRuleTypeModeVisibility(modeRaw, options = {}) {
  const { manualOnly = false } = options;
  const mode = normalizeTemplateOverdueMode(modeRaw);
  const modeWrap = $("typeDefModeWrap");
  const fromWrap = $("typeDefFromWrap");
  const toWrap = $("typeDefToWrap");
  const exactWrap = $("typeDefExactWrap");
  if (!modeWrap || !fromWrap || !toWrap || !exactWrap) return;
  if (manualOnly) {
    modeWrap.style.display = "none";
    fromWrap.style.display = "none";
    toWrap.style.display = "none";
    exactWrap.style.display = "none";
    return;
  }
  modeWrap.style.display = "";
  const isExact = mode === "exact";
  fromWrap.style.display = isExact ? "none" : "";
  toWrap.style.display = isExact ? "none" : "";
  exactWrap.style.display = isExact ? "" : "none";
}

function readTemplateRuleTypeDraftFromEditor() {
  const manualOnly = $("typeDefAutoAssign").checked;
  return {
    name: $("typeDefName").value.trim(),
    overdueMode: normalizeTemplateOverdueMode($("typeDefMode").value),
    overdueFromDays: parseTemplateOptionalInt($("typeDefFrom").value),
    overdueToDays: parseTemplateOptionalInt($("typeDefTo").value),
    overdueExactDay: parseTemplateOptionalInt($("typeDefExact").value),
    autoAssign: !manualOnly
  };
}

function validateTemplateRuleTypeDraft(draft) {
  if (!String(draft.name || "").trim()) {
    return "Название типа обязательно.";
  }
  if (draft.autoAssign === false) {
    return "";
  }
  const mode = normalizeTemplateOverdueMode(draft.overdueMode);
  if (mode === "exact") {
    if (draft.overdueExactDay === null || draft.overdueExactDay < 0) {
      return "Для режима «точный день» укажите число >= 0.";
    }
    return "";
  }
  if (draft.overdueFromDays === null || draft.overdueToDays === null) {
    return "Для диапазона заполните оба поля: «от» и «до».";
  }
  if (draft.overdueFromDays < 0 || draft.overdueToDays < 0) {
    return "Диапазон просрочки должен быть >= 0.";
  }
  if (draft.overdueToDays < draft.overdueFromDays) {
    return "В диапазоне значение «до» не может быть меньше «от».";
  }
  return "";
}

function setTemplateRuleTypeEditorStatus(text, tone = "warning") {
  setNotice("typeDefEditorStatus", text, tone);
}

function renderTemplateRuleTypeSettings() {
  const body = $("typeDefBody");
  if (!body) return;

  ensureTemplateRuleTypeSelection();

  if (state.templateRuleTypes.length === 0) {
    body.innerHTML = UI.emptyRow(3, "Типы шаблонов еще не созданы");
  } else {
    body.innerHTML = state.templateRuleTypes.map((type) => `
      <tr class="${!state.templateRuleTypeCreateMode && type.id === state.selectedTemplateRuleTypeId ? "active-row" : ""}">
        <td>${escapeHtml(type.name)}</td>
        <td>${escapeHtml(formatTemplateRuleTypeRule(type))}</td>
        <td><button class="ghost-btn" data-action="type-def-open" data-type-id="${escapeHtml(type.id)}">Открыть</button></td>
      </tr>
    `).join("");
  }

  if (state.templateRuleTypeCreateMode) {
    $("typeDefName").value = state.templateRuleTypeCreateDraft.name || "";
    $("typeDefMode").value = normalizeTemplateOverdueMode(state.templateRuleTypeCreateDraft.overdueMode);
    $("typeDefFrom").value = state.templateRuleTypeCreateDraft.overdueFromDays ?? "";
    $("typeDefTo").value = state.templateRuleTypeCreateDraft.overdueToDays ?? "";
    $("typeDefExact").value = state.templateRuleTypeCreateDraft.overdueExactDay ?? "";
    $("typeDefAutoAssign").checked = state.templateRuleTypeCreateDraft.autoAssign === false;
  } else {
    const selected = getTemplateRuleTypeById(state.selectedTemplateRuleTypeId);
    if (selected) {
      $("typeDefName").value = selected.name || "";
      $("typeDefMode").value = normalizeTemplateOverdueMode(selected.overdueMode);
      $("typeDefFrom").value = selected.overdueFromDays ?? "";
      $("typeDefTo").value = selected.overdueToDays ?? "";
      $("typeDefExact").value = selected.overdueExactDay ?? "";
      $("typeDefAutoAssign").checked = selected.autoAssign === false;
    } else {
      $("typeDefName").value = "";
      $("typeDefMode").value = DEFAULT_TEMPLATE_OVERDUE_MODE;
      $("typeDefFrom").value = "";
      $("typeDefTo").value = "";
      $("typeDefExact").value = "";
      $("typeDefAutoAssign").checked = false;
    }
  }

  renderTemplateRuleTypeEditorState();
  renderTemplateTypeSelect();
}

function renderTemplateRuleTypeEditorState() {
  const hasRows = state.templateRuleTypes.length > 0;
  $("typeDefNew").disabled = state.templateRuleTypeCreateMode;
  const currentDraft = readTemplateRuleTypeDraftFromEditor();
  setTemplateRuleTypeModeVisibility(currentDraft.overdueMode, { manualOnly: currentDraft.autoAssign === false });

  if (state.templateRuleTypeCreateMode) {
    const draft = currentDraft;
    const validationError = validateTemplateRuleTypeDraft(draft);
    state.templateRuleTypeCreateDraft = {
      ...state.templateRuleTypeCreateDraft,
      ...draft
    };
    const hasAnyInput = Boolean(
      draft.name ||
      draft.overdueFromDays !== null ||
      draft.overdueToDays !== null ||
      draft.overdueExactDay !== null ||
      draft.autoAssign === false
    );
    const canSave = !validationError && Boolean(draft.name);
    $("typeDefSave").disabled = !canSave;
    $("typeDefCancel").disabled = false;
    $("typeDefDelete").disabled = true;
    if (!hasAnyInput) {
      setTemplateRuleTypeEditorStatus("Заполните поля и сохраните новый тип шаблона.", "warning");
      return;
    }
    if (!canSave) {
      setTemplateRuleTypeEditorStatus(validationError || "Название типа обязательно.", "warning");
      return;
    }
    setTemplateRuleTypeEditorStatus("Нажмите «Сохранить тип», чтобы добавить новый тип шаблона.", "info");
    return;
  }

  const selected = getTemplateRuleTypeById(state.selectedTemplateRuleTypeId);
  if (!selected) {
    $("typeDefSave").disabled = true;
    $("typeDefCancel").disabled = true;
    $("typeDefDelete").disabled = !hasRows;
    setTemplateRuleTypeEditorStatus("Выберите тип из списка или создайте новый.", "warning");
    return;
  }

  const draft = currentDraft;
  const validationError = validateTemplateRuleTypeDraft(draft);
  const dirty =
    draft.name !== selected.name ||
    normalizeTemplateOverdueMode(draft.overdueMode) !== normalizeTemplateOverdueMode(selected.overdueMode) ||
    draft.overdueFromDays !== parseTemplateOptionalInt(selected.overdueFromDays) ||
    draft.overdueToDays !== parseTemplateOptionalInt(selected.overdueToDays) ||
    draft.overdueExactDay !== parseTemplateOptionalInt(selected.overdueExactDay) ||
    draft.autoAssign !== (selected.autoAssign !== false);

  const canSave = !validationError && dirty;
  $("typeDefSave").disabled = !canSave;
  $("typeDefCancel").disabled = !dirty;
  $("typeDefDelete").disabled = state.templateRuleTypes.length <= 1;

  if (validationError) {
    setTemplateRuleTypeEditorStatus(validationError, "warning");
    return;
  }
  if (dirty) {
    setTemplateRuleTypeEditorStatus(`Есть несохраненные изменения в типе «${selected.name}».`, "warning");
    return;
  }
  setTemplateRuleTypeEditorStatus(`Тип «${selected.name}» открыт. Изменений нет.`, "info");
}

function createTemplateRuleType() {
  state.templateRuleTypeCreateMode = true;
  resetTemplateRuleTypeCreateDraft();
  renderTemplateRuleTypeSettings();
  $("typeDefName").focus();
}

function saveTemplateRuleTypeFromEditor() {
  const draft = readTemplateRuleTypeDraftFromEditor();
  const validationError = validateTemplateRuleTypeDraft(draft);
  if (validationError) {
    toast(validationError);
    return false;
  }

  const autoAssign = draft.autoAssign !== false;
  const normalizedMode = autoAssign ? normalizeTemplateOverdueMode(draft.overdueMode) : "range";
  const payload = {
    id: "",
    name: draft.name,
    overdueMode: normalizedMode,
    overdueFromDays: null,
    overdueToDays: null,
    overdueExactDay: null,
    autoAssign,
    sortOrder: 0
  };

  if (!autoAssign) {
    payload.overdueFromDays = 0;
    payload.overdueToDays = 0;
  } else if (normalizedMode === "exact") {
    payload.overdueExactDay = Math.max(0, draft.overdueExactDay ?? 0);
  } else {
    const from = Math.max(0, draft.overdueFromDays ?? 0);
    const to = Math.max(from, draft.overdueToDays ?? from);
    payload.overdueFromDays = from;
    payload.overdueToDays = to;
  }

  const current = state.templateRuleTypeCreateMode
    ? null
    : getTemplateRuleTypeById(state.selectedTemplateRuleTypeId);

  const nextItems = state.templateRuleTypes.map(cloneTemplateRuleType);
  if (current) {
    payload.id = current.id;
    payload.sortOrder = current.sortOrder;
    const idx = nextItems.findIndex((x) => x.id === current.id);
    if (idx >= 0) {
      nextItems[idx] = payload;
    } else {
      nextItems.push(payload);
    }
  } else {
    const usedIds = new Set(nextItems.map((x) => x.id));
    const baseId = normalizeTemplateRuleTypeId(payload.name) || `type_${nextItems.length + 1}`;
    let resolvedId = baseId;
    let suffix = 2;
    while (usedIds.has(resolvedId)) {
      resolvedId = `${baseId}_${suffix}`;
      suffix += 1;
    }
    payload.id = resolvedId;
    payload.sortOrder = nextTemplateRuleTypeSortOrder();
    nextItems.push(payload);
  }

  state.templateRuleTypes = normalizeTemplateRuleTypes(nextItems);
  state.templateRuleTypeCreateMode = false;
  state.selectedTemplateRuleTypeId = payload.id;
  resetTemplateRuleTypeCreateDraft();
  renderTemplateRuleTypeSettings();
  renderConfiguredOverdueFilters({ preserveSelection: true });
  collectRunFiltersFromUI();
  renderRunFilterSummary();
  renderTemplates();
  renderTemplateEditorState();
  renderClientsDb();
  renderQueue();
  return true;
}

function cancelTemplateRuleTypeChanges() {
  if (state.templateRuleTypeCreateMode) {
    exitTemplateRuleTypeCreateMode();
    renderTemplateRuleTypeSettings();
    return true;
  }
  const current = getTemplateRuleTypeById(state.selectedTemplateRuleTypeId);
  if (!current) return false;
  renderTemplateRuleTypeSettings();
  return true;
}

function deleteSelectedTemplateRuleType() {
  if (state.templateRuleTypeCreateMode) {
    return false;
  }
  const current = getTemplateRuleTypeById(state.selectedTemplateRuleTypeId);
  if (!current) return false;
  if (state.templateRuleTypes.length <= 1) {
    toast("Нельзя удалить последний тип шаблона.");
    return false;
  }

  const usedInTemplates = state.templates.filter((template) => normalizeTemplateKind(template.kind, { allowMissing: true }) === current.id);
  if (usedInTemplates.length > 0) {
    toast("Тип используется в существующих шаблонах. Сначала переведите эти шаблоны на другой тип.");
    return false;
  }

  state.templateRuleTypes = normalizeTemplateRuleTypes(state.templateRuleTypes.filter((item) => item.id !== current.id));
  state.selectedTemplateRuleTypeId = state.templateRuleTypes[0]?.id || null;
  renderTemplateRuleTypeSettings();
  renderConfiguredOverdueFilters({ preserveSelection: true });
  collectRunFiltersFromUI();
  renderRunFilterSummary();
  renderTemplates();
  renderTemplateEditorState();
  renderClientsDb();
  renderQueue();
  return true;
}

function hasTemplateRuleTypeUnsavedChanges() {
  if (!$("typeDefName")) return false;
  const draft = readTemplateRuleTypeDraftFromEditor();
  if (state.templateRuleTypeCreateMode) {
    return Boolean(String(draft.name || "").trim());
  }
  const selected = getTemplateRuleTypeById(state.selectedTemplateRuleTypeId);
  if (!selected) return false;
  return (
    draft.name !== selected.name ||
    normalizeTemplateOverdueMode(draft.overdueMode) !== normalizeTemplateOverdueMode(selected.overdueMode) ||
    draft.overdueFromDays !== parseTemplateOptionalInt(selected.overdueFromDays) ||
    draft.overdueToDays !== parseTemplateOptionalInt(selected.overdueToDays) ||
    draft.overdueExactDay !== parseTemplateOptionalInt(selected.overdueExactDay) ||
    draft.autoAssign !== (selected.autoAssign !== false)
  );
}

function renderTemplateTypeSelect() {
  const select = $("tplType");
  if (!select) return;
  const preferredKind = normalizeTemplateKind(
    select.value || state.templateCreateDraft.kind || state.templateEditorBaseline.kind || DEFAULT_TEMPLATE_KIND,
    { allowMissing: true }
  );
  const options = state.templateRuleTypes.map((type) => `
    <option value="${type.id}">${escapeHtml(type.name)}</option>
  `);
  if (preferredKind && !state.templateRuleTypes.some((item) => item.id === preferredKind)) {
    options.push(`<option value="${escapeHtml(preferredKind)}">${escapeHtml(templateTypeLabel(preferredKind, { allowMissing: true }))}</option>`);
  }
  select.innerHTML = options.length > 0 ? options.join("") : '<option value="">Типы не настроены</option>';
  if (preferredKind) {
    select.value = preferredKind;
  } else if (state.templateRuleTypes.length > 0) {
    select.value = state.templateRuleTypes[0].id;
  }
  select.disabled = state.templateRuleTypes.length === 0;
}

function syncTemplateTypeRule(kind, options = {}) {
  setNotice("tplTypeRule", templateTypeRule(kind, options), "info");
}

function resolveTemplateAutoAssignByKind(kind, options = {}) {
  const type = getTemplateType(kind, {
    allowMissing: true,
    fallbackTemplate: options.fallbackTemplate || null
  });
  return type.autoAssign !== false;
}

function setTemplateRuleModeVisibility(_modeRaw, _options = {}) {
  // Правило просрочки определяется типом шаблона и не редактируется в шаблоне вручную.
}

function readTemplateRuleDraftFromEditor(options = {}) {
  const selectedKind = normalizeTemplateKind(
    options.kind || $("tplType")?.value || DEFAULT_TEMPLATE_KIND,
    { allowMissing: true }
  );
  const source = buildTemplateRuleDraftFromType(selectedKind, {
    fallbackTemplate: options.fallbackTemplate || null
  });

  const mode = normalizeTemplateOverdueMode(source.overdueMode || source.mode);
  const fromDays = parseTemplateOptionalInt(source.overdueFromDays ?? source.fromDays);
  const toDays = parseTemplateOptionalInt(source.overdueToDays ?? source.toDays);
  const exactDay = parseTemplateOptionalInt(source.overdueExactDay ?? source.exactDay);
  const autoAssign = source.autoAssign !== false;

  if ($("tplOverdueMode")) $("tplOverdueMode").value = mode;
  if ($("tplOverdueFrom")) $("tplOverdueFrom").value = fromDays ?? "";
  if ($("tplOverdueTo")) $("tplOverdueTo").value = toDays ?? "";
  if ($("tplOverdueExact")) $("tplOverdueExact").value = exactDay ?? "";
  if ($("tplAutoAssign")) $("tplAutoAssign").checked = autoAssign;

  return { mode, fromDays, toDays, exactDay, autoAssign };
}

function validateTemplateRuleDraft(draft, options = {}) {
  if (options.manualOnly || draft.autoAssign === false) {
    return "";
  }
  const mode = normalizeTemplateOverdueMode(draft.mode);
  if (mode === "exact") {
    if (draft.exactDay === null || draft.exactDay < 0) {
      return "Для режима «точный день» укажите число >= 0.";
    }
    return "";
  }

  if (draft.fromDays === null || draft.toDays === null) {
    return "Для диапазона заполните оба поля: «от» и «до».";
  }
  if (draft.fromDays < 0 || draft.toDays < 0) {
    return "Диапазон просрочки должен быть >= 0.";
  }
  if (draft.toDays < draft.fromDays) {
    return "В диапазоне значение «до» не может быть меньше «от».";
  }
  return "";
}

function applyTemplateRuleDraftToEditor(draft) {
  if ($("tplOverdueMode")) $("tplOverdueMode").value = normalizeTemplateOverdueMode(draft.overdueMode || draft.mode);
  if ($("tplOverdueFrom")) $("tplOverdueFrom").value = draft.overdueFromDays ?? draft.fromDays ?? "";
  if ($("tplOverdueTo")) $("tplOverdueTo").value = draft.overdueToDays ?? draft.toDays ?? "";
  if ($("tplOverdueExact")) $("tplOverdueExact").value = draft.overdueExactDay ?? draft.exactDay ?? "";
  $("tplAutoAssign").checked = draft.autoAssign !== false;
  setTemplateRuleModeVisibility(draft.overdueMode || draft.mode, { manualOnly: draft.autoAssign === false });
}

function applyTemplateTypeDefaultsToEditor(kind, options = {}) {
  const normalizedKind = normalizeTemplateKind(kind, { allowMissing: true });
  applyTemplateRuleDraftToEditor(buildTemplateRuleDraftFromType(normalizedKind, options));
  syncTemplateTypeRule(normalizedKind, options);
}

function setTemplateEditorStatus(text, tone = "warning") {
  setNotice("tplEditorStatus", text, tone);
}

function renderTemplateEditorState() {
  renderTemplateTypeSelect();
  const typeSelect = $("tplType");
  const nameInput = $("tplName");
  const textInput = $("tplText");
  const autoAssignInput = $("tplAutoAssign");
  if (!typeSelect || !nameInput || !textInput || !autoAssignInput) return;

  if (state.templateCreateMode) {
    const kind = normalizeTemplateKind(typeSelect.value || state.templateCreateDraft.kind, { allowMissing: true });
    autoAssignInput.checked = resolveTemplateAutoAssignByKind(kind);
    const ruleDraft = readTemplateRuleDraftFromEditor({ kind });
    const name = nameInput.value.trim();
    const text = textInput.value.trim();
    const defaultType = getDefaultTemplateRuleType();
    const defaultRule = buildTemplateRuleDraftFromType(defaultType.id);
    state.templateCreateDraft = {
      kind,
      overdueMode: ruleDraft.mode,
      overdueFromDays: ruleDraft.fromDays,
      overdueToDays: ruleDraft.toDays,
      overdueExactDay: ruleDraft.exactDay,
      autoAssign: ruleDraft.autoAssign,
      name,
      text
    };
    syncTemplateTypeRule(kind);
    setTemplateRuleModeVisibility(ruleDraft.mode, { manualOnly: ruleDraft.autoAssign === false });

    const ruleValidation = validateTemplateRuleDraft(ruleDraft, { manualOnly: ruleDraft.autoAssign === false });
    const hasAnyInput = Boolean(
      name ||
      text ||
      kind !== defaultType.id ||
      normalizeTemplateOverdueMode(ruleDraft.mode) !== normalizeTemplateOverdueMode(defaultRule.overdueMode) ||
      ruleDraft.fromDays !== parseTemplateOptionalInt(defaultRule.overdueFromDays) ||
      ruleDraft.toDays !== parseTemplateOptionalInt(defaultRule.overdueToDays) ||
      ruleDraft.exactDay !== parseTemplateOptionalInt(defaultRule.overdueExactDay) ||
      ruleDraft.autoAssign !== (defaultRule.autoAssign !== false)
    );
    const canSave = Boolean(name && text && !ruleValidation);

    $("tplDraft").disabled = !canSave;
    $("tplPublish").disabled = !canSave;
    $("tplCancel").disabled = false;

    if (!hasAnyInput) {
      setTemplateEditorStatus("Заполните поля и сохраните новый шаблон.", "warning");
      return;
    }
    if (!canSave) {
      const baseMessage = !name || !text
        ? "Название и текст нового шаблона обязательны."
        : ruleValidation;
      setTemplateEditorStatus(baseMessage, "warning");
      return;
    }
    setTemplateEditorStatus("Новый шаблон готов к сохранению. Выберите статус: черновик или активный.", "info");
    return;
  }

  const template = getTemplateById(state.selectedTemplateId);
  if (!template) {
    const fallbackType = getDefaultTemplateRuleType();
    typeSelect.value = fallbackType.id;
    syncTemplateTypeRule(fallbackType.id);
    applyTemplateRuleDraftToEditor(buildTemplateRuleDraftFromType(fallbackType.id));
    nameInput.value = "";
    textInput.value = "";
    $("tplDraft").disabled = true;
    $("tplPublish").disabled = true;
    $("tplCancel").disabled = true;
    setTemplateEditorStatus("Выберите шаблон из списка или нажмите «Создать новый шаблон».", "warning");
    return;
  }

  if (!typeSelect.value) {
    typeSelect.value = normalizeTemplateKind(template.kind, { allowMissing: true });
  }
  const selectedKind = normalizeTemplateKind(typeSelect.value, { allowMissing: true });
  autoAssignInput.checked = resolveTemplateAutoAssignByKind(selectedKind, { fallbackTemplate: template });
  const ruleDraft = readTemplateRuleDraftFromEditor({ kind: selectedKind, fallbackTemplate: template });
  setTemplateRuleModeVisibility(ruleDraft.mode, { manualOnly: ruleDraft.autoAssign === false });
  const ruleValidation = validateTemplateRuleDraft(ruleDraft, { manualOnly: ruleDraft.autoAssign === false });
  syncTemplateTypeRule(selectedKind, { fallbackTemplate: template });

  const dirty =
    nameInput.value.trim() !== state.templateEditorBaseline.name ||
    textInput.value.trim() !== state.templateEditorBaseline.text ||
    selectedKind !== state.templateEditorBaseline.kind ||
    normalizeTemplateOverdueMode(ruleDraft.mode) !== normalizeTemplateOverdueMode(state.templateEditorBaseline.overdueMode) ||
    ruleDraft.fromDays !== parseTemplateOptionalInt(state.templateEditorBaseline.overdueFromDays) ||
    ruleDraft.toDays !== parseTemplateOptionalInt(state.templateEditorBaseline.overdueToDays) ||
    ruleDraft.exactDay !== parseTemplateOptionalInt(state.templateEditorBaseline.overdueExactDay) ||
    ruleDraft.autoAssign !== Boolean(state.templateEditorBaseline.autoAssign);

  const canSaveCurrent = !ruleValidation;
  $("tplDraft").disabled = !canSaveCurrent;
  $("tplPublish").disabled = !canSaveCurrent;
  $("tplCancel").disabled = !dirty;

  if (ruleValidation) {
    setTemplateEditorStatus(ruleValidation, "warning");
    return;
  }

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
  template.kind = normalizeTemplateKind(template.kind, { allowMissing: true });
  $("tplType").value = template.kind;
  const baselineRule = buildTemplateRuleDraftFromType(template.kind, { fallbackTemplate: template });
  applyTemplateRuleDraftToEditor(baselineRule);
  $("tplName").value = template.name || "";
  $("tplText").value = template.text || "";
  state.templateEditorBaseline = {
    kind: template.kind,
    overdueMode: normalizeTemplateOverdueMode(baselineRule.overdueMode),
    overdueFromDays: parseTemplateOptionalInt(baselineRule.overdueFromDays),
    overdueToDays: parseTemplateOptionalInt(baselineRule.overdueToDays),
    overdueExactDay: parseTemplateOptionalInt(baselineRule.overdueExactDay),
    autoAssign: baselineRule.autoAssign !== false,
    name: $("tplName").value.trim(),
    text: $("tplText").value.trim()
  };
  renderTemplates();
  renderTemplateEditorState();
  return true;
}

async function applyTemplateEditorChanges(nextStatus, successText) {
  const kind = normalizeTemplateKind($("tplType").value, { allowMissing: true });
  const currentTemplate = state.templateCreateMode ? null : getTemplateById(state.selectedTemplateId);
  const ruleDraft = readTemplateRuleDraftFromEditor({ kind, fallbackTemplate: currentTemplate });
  const autoAssign = ruleDraft.autoAssign;
  const manualOnly = autoAssign === false;
  const ruleValidation = validateTemplateRuleDraft(ruleDraft, { manualOnly });
  const name = $("tplName").value.trim();
  const text = $("tplText").value.trim();
  if (!name || !text || ruleValidation) {
    if (ruleValidation) {
      toast(ruleValidation);
      return false;
    }
    toast("Название и текст шаблона обязательны");
    return false;
  }

  const status = nextStatus || (currentTemplate?.status || "draft");
  const resolvedCommentText = state.templateCreateMode
    ? ""
    : String(currentTemplate?.commentText || "").trim();
  if (status === "active" && !resolvedCommentText) {
    toast("Для активного шаблона задайте комментарий в разделе «Настройки» → «Комментарии в договор»");
    return false;
  }
  const payload = {
    kind,
    name,
    text,
    status,
    overdueMode: manualOnly ? "range" : ruleDraft.mode,
    overdueFromDays: manualOnly ? 0 : (ruleDraft.mode === "range" ? ruleDraft.fromDays : null),
    overdueToDays: manualOnly ? 0 : (ruleDraft.mode === "range" ? ruleDraft.toDays : null),
    overdueExactDay: manualOnly ? null : (ruleDraft.mode === "exact" ? ruleDraft.exactDay : null),
    autoAssign,
    commentText: resolvedCommentText
  };

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
      const fallbackType = getDefaultTemplateRuleType();
      $("tplType").value = fallbackType.id;
      applyTemplateRuleDraftToEditor(buildTemplateRuleDraftFromType(fallbackType.id));
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
  applyTemplateRuleDraftToEditor(state.templateEditorBaseline);
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
  $("tplType").value = state.templateCreateDraft.kind;
  applyTemplateRuleDraftToEditor(state.templateCreateDraft);
  $("tplName").value = "";
  $("tplText").value = "";
  renderTemplates();
  renderTemplateEditorState();
  $("tplName").focus();
}

function ensureTemplateCreateModeFromEditorInput() {
  if (state.templateCreateMode || state.selectedTemplateId !== null) return false;
  const kind = normalizeTemplateKind($("tplType").value || DEFAULT_TEMPLATE_KIND, { allowMissing: true });
  const ruleDraft = readTemplateRuleDraftFromEditor({ kind });
  state.templateCreateMode = true;
  state.templateCreateDraft = {
    kind,
    overdueMode: ruleDraft.mode,
    overdueFromDays: ruleDraft.fromDays,
    overdueToDays: ruleDraft.toDays,
    overdueExactDay: ruleDraft.exactDay,
    autoAssign: ruleDraft.autoAssign,
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

function sortedTemplatesForCommentSettings() {
  return [...state.templates].sort((a, b) => {
    const statusWeight = (a.status === "active" ? 0 : 1) - (b.status === "active" ? 0 : 1);
    if (statusWeight !== 0) return statusWeight;
    const byKind = templateKindSortOrder(a.kind) - templateKindSortOrder(b.kind);
    if (byKind !== 0) return byKind;
    return String(a.name || "").localeCompare(String(b.name || ""), "ru");
  });
}

function templateCommentOptionLabel(template) {
  const statusText = template.status === "active" ? "Актуальный" : "Черновик";
  return `${templateOptionLabel(template)} · ${statusText}`;
}

function templateCommentListStatus(template) {
  const status = template.status === "active" ? "Актуальный" : "Черновик";
  const comment = String(template.commentText || "").trim();
  if (!comment) {
    return `${status} · комментарий не задан`;
  }
  return `${status} · ${templateCommentPreview(comment, 80)}`;
}

function hasTemplateCommentUnsavedChanges() {
  const select = $("cfgTemplateCommentTemplate");
  const input = $("cfgTemplateCommentText");
  if (!select || !input) return false;
  const template = getTemplateById(Number(select.value || 0));
  if (!template) return false;
  return input.value.trim() !== String(template.commentText || "").trim();
}

function renderTemplateCommentList(templates, selectedTemplateId) {
  const body = $("cfgTemplateCommentBody");
  if (!body) return;
  if (!Array.isArray(templates) || templates.length === 0) {
    body.innerHTML = UI.emptyRow(3, "Шаблоны не созданы");
    return;
  }

  body.innerHTML = templates.map((template) => {
    const label = escapeHtml(templateOptionLabel(template));
    const statusAndComment = escapeHtml(templateCommentListStatus(template));
    const isSelected = Number(selectedTemplateId) === Number(template.id);
    return `
      <tr class="${isSelected ? "active-row" : ""}">
        <td class="cell-ellipsis"><span title="${label}">${label}</span></td>
        <td class="cell-ellipsis"><span title="${statusAndComment}">${statusAndComment}</span></td>
        <td><button class="ghost-btn" data-action="template-comment-open" data-tpl-id="${template.id}">Открыть</button></td>
      </tr>
    `;
  }).join("");
}

function trySelectTemplateCommentTemplate(templateId, options = {}) {
  const { skipUnsavedGuard = false } = options;
  const nextId = Number(templateId || 0);
  if (!nextId) return false;
  const currentId = Number(state.settingsTemplateCommentTemplateId || 0);
  if (!skipUnsavedGuard && currentId > 0 && currentId !== nextId && hasTemplateCommentUnsavedChanges()) {
    const confirmed = window.confirm("Есть несохраненные изменения комментария. Переключиться на другой шаблон без сохранения?");
    if (!confirmed) return false;
  }

  state.settingsTemplateCommentTemplateId = nextId;
  renderTemplateCommentSettings();
  return true;
}

function renderTemplateCommentSettingsState() {
  const select = $("cfgTemplateCommentTemplate");
  const input = $("cfgTemplateCommentText");
  const saveBtn = $("cfgTemplateCommentSave");
  const clearBtn = $("cfgTemplateCommentClear");
  if (!select || !input || !saveBtn) return;

  const templateId = Number(select.value || 0);
  const template = getTemplateById(templateId);
  if (!template) {
    saveBtn.disabled = true;
    if (clearBtn) clearBtn.disabled = true;
    input.disabled = true;
    setNotice("cfgTemplateCommentStatus", "Сначала создайте хотя бы один шаблон.", "warning");
    return;
  }

  input.disabled = false;
  const baseline = String(template.commentText || "").trim();
  const current = input.value.trim();
  const dirty = current !== baseline;
  const activeWithoutComment = template.status === "active" && !current;
  saveBtn.disabled = !dirty || activeWithoutComment;
  if (clearBtn) {
    clearBtn.disabled = current.length === 0;
  }

  if (activeWithoutComment) {
    setNotice("cfgTemplateCommentStatus", `Шаблон «${template.name}» активен. Заполните комментарий перед сохранением.`, "warning");
    return;
  }

  if (dirty) {
    setNotice("cfgTemplateCommentStatus", `Есть несохраненные изменения комментария для шаблона «${template.name}».`, "warning");
    return;
  }

  setNotice("cfgTemplateCommentStatus", `Комментарий шаблона «${template.name}» сохранен.`, "info");
}

function renderTemplateCommentSettings() {
  const select = $("cfgTemplateCommentTemplate");
  const input = $("cfgTemplateCommentText");
  const saveBtn = $("cfgTemplateCommentSave");
  const clearBtn = $("cfgTemplateCommentClear");
  if (!select || !input || !saveBtn) return;

  const templates = sortedTemplatesForCommentSettings();
  if (templates.length === 0) {
    state.settingsTemplateCommentTemplateId = null;
    select.innerHTML = '<option value="">Шаблоны не созданы</option>';
    select.disabled = true;
    input.value = "";
    input.disabled = true;
    saveBtn.disabled = true;
    if (clearBtn) clearBtn.disabled = true;
    renderTemplateCommentList([], null);
    setNotice("cfgTemplateCommentStatus", "Сначала создайте шаблон, затем задайте комментарий.", "warning");
    return;
  }

  const selectedCandidate = Number(state.settingsTemplateCommentTemplateId || select.value || 0);
  const selectedTemplate = templates.find((t) => t.id === selectedCandidate) || templates[0];
  state.settingsTemplateCommentTemplateId = selectedTemplate.id;

  renderTemplateCommentList(templates, selectedTemplate.id);

  select.innerHTML = templates.map((tpl) => `
    <option value="${tpl.id}">${escapeHtml(templateCommentOptionLabel(tpl))}</option>
  `).join("");
  select.value = String(selectedTemplate.id);

  input.value = String(selectedTemplate.commentText || "");
  renderTemplateCommentSettingsState();
}

async function saveTemplateCommentFromSettings() {
  const select = $("cfgTemplateCommentTemplate");
  const input = $("cfgTemplateCommentText");
  if (!select || !input) return false;

  const templateId = Number(select.value || 0);
  const template = getTemplateById(templateId);
  if (!template) {
    toast("Сначала выберите шаблон для комментария");
    return false;
  }

  const commentText = input.value.trim();
  if (template.status === "active" && !commentText) {
    toast("Для активного шаблона комментарий обязателен");
    renderTemplateCommentSettingsState();
    return false;
  }

  const mode = normalizeTemplateOverdueMode(template.overdueMode);
  const autoAssign = resolveTemplateAutoAssignByKind(template.kind, { fallbackTemplate: template });
  const payload = {
    kind: template.kind,
    name: template.name,
    text: template.text,
    status: template.status,
    overdueMode: mode,
    overdueFromDays: mode === "range" ? parseTemplateOptionalInt(template.overdueFromDays) : null,
    overdueToDays: mode === "range" ? parseTemplateOptionalInt(template.overdueToDays) : null,
    overdueExactDay: mode === "exact" ? parseTemplateOptionalInt(template.overdueExactDay) : null,
    autoAssign,
    commentText
  };

  try {
    await fetchApiJson(`/api/templates/${encodeURIComponent(template.id)}`, {
      method: "PUT",
      body: JSON.stringify(payload)
    });
    state.settingsTemplateCommentTemplateId = template.id;
    await refreshTemplatesFromBackend({ silent: true });
    toast(`Комментарий для шаблона «${template.name}» сохранен`);
    return true;
  } catch (error) {
    toast(`Не удалось сохранить комментарий шаблона: ${error?.message || "ошибка backend"}`);
    return false;
  }
}

function readSettingsDraftFromUI() {
  return {
    loginUrl: $("cfgLoginUrl").value.trim(),
    login: $("cfgLogin").value.trim(),
    password: $("cfgPassword").value,
    gap: $("cfgGap").value.trim(),
    debtBufferAmount: $("cfgDebtBufferAmount").value.trim(),
    recentSmsCooldownDays: $("cfgRecentSmsCooldownDays").value.trim(),
    allowLiveDispatch: $("cfgAllowLiveDispatch")?.checked !== false,
    workWindowStart: $("cfgWorkWindowStart").value.trim() || DEFAULT_WORK_WINDOW_START,
    workWindowEnd: $("cfgWorkWindowEnd").value.trim() || DEFAULT_WORK_WINDOW_END,
    commentRules: normalizeCommentRules(state.commentRules),
    templateRuleTypes: normalizeTemplateRuleTypes(state.templateRuleTypes)
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
  if (Object.prototype.hasOwnProperty.call(settings, "debtBufferAmount")) {
    $("cfgDebtBufferAmount").value = settings.debtBufferAmount ?? DEFAULT_DEBT_BUFFER_AMOUNT;
  }
  if (Object.prototype.hasOwnProperty.call(settings, "recentSmsCooldownDays")) {
    $("cfgRecentSmsCooldownDays").value = settings.recentSmsCooldownDays ?? 0;
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
  if (Object.prototype.hasOwnProperty.call(settings, "commentRules")) {
    state.commentRules = normalizeCommentRules(settings.commentRules);
  }
  if (Object.prototype.hasOwnProperty.call(settings, "templateRuleTypes")) {
    state.templateRuleTypes = normalizeTemplateRuleTypes(settings.templateRuleTypes);
    if (!state.templateRuleTypeCreateMode && !getTemplateRuleTypeById(state.selectedTemplateRuleTypeId)) {
      state.selectedTemplateRuleTypeId = state.templateRuleTypes[0]?.id || null;
    }
    if (state.templateRuleTypeCreateMode) {
      resetTemplateRuleTypeCreateDraft();
    }
    renderTemplateRuleTypeSettings();
  }
}

function setSettingsBaselineFromUI() {
  state.settingsBaseline = readSettingsDraftFromUI();
}

function hasSettingsUnsavedChanges() {
  if (!state.settingsBaseline) return false;
  const current = readSettingsDraftFromUI();
  return JSON.stringify(current) !== JSON.stringify(state.settingsBaseline) || hasTemplateRuleTypeUnsavedChanges();
}

function hasTemplateUnsavedChanges() {
  const typeSelect = $("tplType");
  const nameInput = $("tplName");
  const textInput = $("tplText");
  const autoAssignInput = $("tplAutoAssign");
  if (!nameInput || !textInput || !typeSelect) return false;
  if (!autoAssignInput) return false;
  const kind = normalizeTemplateKind(typeSelect.value, { allowMissing: true });
  const template = getTemplateById(state.selectedTemplateId);
  const ruleDraft = readTemplateRuleDraftFromEditor({
    kind,
    fallbackTemplate: state.templateCreateMode ? null : template
  });
  if (state.templateCreateMode) {
    const fallbackType = getDefaultTemplateRuleType();
    const fallbackRule = buildTemplateRuleDraftFromType(fallbackType.id);
    return Boolean(
      nameInput.value.trim() ||
      textInput.value.trim() ||
      kind !== fallbackType.id ||
      normalizeTemplateOverdueMode(ruleDraft.mode) !== normalizeTemplateOverdueMode(fallbackRule.overdueMode) ||
      ruleDraft.fromDays !== parseTemplateOptionalInt(fallbackRule.overdueFromDays) ||
      ruleDraft.toDays !== parseTemplateOptionalInt(fallbackRule.overdueToDays) ||
      ruleDraft.exactDay !== parseTemplateOptionalInt(fallbackRule.overdueExactDay) ||
      ruleDraft.autoAssign !== (fallbackRule.autoAssign !== false)
    );
  }
  if (!template) return false;
  return (
    kind !== state.templateEditorBaseline.kind ||
    normalizeTemplateOverdueMode(ruleDraft.mode) !== normalizeTemplateOverdueMode(state.templateEditorBaseline.overdueMode) ||
    ruleDraft.fromDays !== parseTemplateOptionalInt(state.templateEditorBaseline.overdueFromDays) ||
    ruleDraft.toDays !== parseTemplateOptionalInt(state.templateEditorBaseline.overdueToDays) ||
    ruleDraft.exactDay !== parseTemplateOptionalInt(state.templateEditorBaseline.overdueExactDay) ||
    ruleDraft.autoAssign !== Boolean(state.templateEditorBaseline.autoAssign) ||
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
      if (hasTemplateRuleTypeUnsavedChanges()) {
        const typeSaved = saveTemplateRuleTypeFromEditor();
        if (!typeSaved) return false;
      }
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
    if (state.templateRuleTypeCreateMode) {
      exitTemplateRuleTypeCreateMode();
    }
    renderConfiguredOverdueFilters({ preserveSelection: true });
    collectRunFiltersFromUI();
    renderRunFilterSummary();
    renderTemplateRuleTypeSettings();
    renderClientsDb();
    renderQueue();
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
    <option value="${tpl.id}" ${tpl.id === currentTemplateId ? "selected" : ""}>${escapeHtml(templateOptionLabel(tpl))}</option>
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
    const hasPhone = Boolean(normalizePhone(client.phone));
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
            <button class="ghost-btn" data-action="client-open-dialog" data-client-phone="${client.phone}" ${hasPhone ? "" : "disabled"}>В диалог</button>
            <button class="ghost-btn" data-action="client-toggle-stop" data-client-phone="${client.phone}" ${hasPhone ? "" : "disabled"}>${inStop ? "Убрать из стоп-листа" : "В стоп-лист"}</button>
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
      <td class="queue-status-cell">${queueStatusCell(q)}</td>
      <td>${q.attempts}/${q.maxAttempts}</td>
      <td class="cell-ellipsis"><span>${dialogStatusByPhone(q.phone).text}</span></td>
      <td>
        <div class="actions slim">
          <button class="ghost-btn" data-action="queue-open-dialog" data-q-phone="${q.phone}" ${normalizePhone(q.phone) ? "" : "disabled"}>В диалог</button>
          <button class="ghost-btn" data-action="queue-toggle-stop" data-q-id="${q.id}" data-q-phone="${q.phone}" ${normalizePhone(q.phone) ? "" : "disabled"}>${isPhoneInStopList(q.phone) ? "Убрать из стоп-листа" : "В стоп-лист"}</button>
          <button class="ghost-btn" data-action="queue-remove-plan" data-q-id="${q.id}" ${state.runRuntime || !["queued", "retry", "stopped"].includes(q.status) ? "disabled" : ""}>Убрать из плана</button>
        </div>
      </td>
    </tr>
  `).join("");
  $("queueBody").querySelectorAll("[data-action='queue-remove-plan']").forEach((btn) => {
    const jobId = Number(btn.dataset.qId);
    const job = state.queue.find((item) => Number(item.id) === jobId);
    btn.disabled = !job || !["queued", "retry", "stopped"].includes(job.status);
  });
  renderQueueBulkUi(rows);
  renderQueueRetryActionState();
}

function renderDialogs() {
  const list = $("dialogList");
  const search = (($("dialogsSearch")?.value) || "").trim().toLowerCase();
  const searchPhone = normalizePhone(search);
  const sortedDialogs = [...state.dialogs]
    .filter((dialog) => {
      if (!search) return true;
      const inFio = String(dialog.fio || "").toLowerCase().includes(search);
      const inPhone = searchPhone ? normalizePhone(dialog.phone).includes(searchPhone) : false;
      const inContract = String(dialog.contractNumber || "").toLowerCase().includes(search);
      return inFio || inPhone || inContract;
    })
    .sort((a, b) => dialogLastActivityUtcMs(b) - dialogLastActivityUtcMs(a));
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
      <small class="contact-meta">Договор: ${d.contractNumber || "-"}</small>
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
  const manualSendBlocked = !hasSelected || isPhoneInStopList(selectedDialog?.phone || "");
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
  if ($("manualSend")) {
    $("manualSend").disabled = manualSendBlocked;
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
