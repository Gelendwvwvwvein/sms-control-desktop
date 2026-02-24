(function initUiKit(global) {
  const toastState = {
    timer: null
  };

  function $(id) {
    return document.getElementById(id);
  }

  function setText(id, text) {
    const el = $(id);
    if (!el) return;
    el.textContent = text;
  }

  function setHtml(id, html) {
    const el = $(id);
    if (!el) return;
    el.innerHTML = html;
  }

  function notice(id, text, tone = "warning") {
    const el = $(id);
    if (!el) return;
    el.textContent = text;
    el.classList.add("ui-notice");
    el.classList.remove("ui-notice-warning", "ui-notice-info", "ui-notice-success");
    el.classList.add(`ui-notice-${tone}`);
  }

  function toast(toastId, text, durationMs = 1900) {
    const el = $(toastId);
    if (!el) return;
    el.textContent = text;
    el.classList.add("visible");
    if (toastState.timer) {
      clearTimeout(toastState.timer);
    }
    toastState.timer = setTimeout(() => {
      el.classList.remove("visible");
      toastState.timer = null;
    }, durationMs);
  }

  function emptyRow(colspan, text) {
    return `<tr><td colspan="${colspan}" class="muted-note">${text}</td></tr>`;
  }

  function bindDelegated(rootId, handlers) {
    const root = $(rootId);
    if (!root) return;
    root.addEventListener("click", (event) => {
      const trigger = event.target.closest("[data-action]");
      if (!trigger || !root.contains(trigger)) return;
      const action = trigger.dataset.action;
      const fn = handlers[action];
      if (!fn) return;
      fn(trigger, event);
    });
  }

  global.UiKit = {
    $,
    setText,
    setHtml,
    notice,
    toast,
    emptyRow,
    bindDelegated
  };
})(window);
