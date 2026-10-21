(() => {
  const refreshStorageKey = 'kafkaesque.consumerLag.refresh';
  const refreshIntervals = new Set(['off', '5', '10', '30', '60']);

  let automaticRefreshPending = false;
  let refreshTimer;
  let toastTimer;

  const refreshRoot = () => document.querySelector('[data-consumer-lag-refresh]');
  const refreshControl = () => document.querySelector('#consumer-lag-refresh');

  const savedRefreshInterval = () => {
    try {
      const saved = window.localStorage.getItem(refreshStorageKey);
      return refreshIntervals.has(saved) ? saved : '10';
    } catch {
      return '10';
    }
  };

  const saveRefreshInterval = (value) => {
    try {
      window.localStorage.setItem(refreshStorageKey, value);
    } catch {
      // The current page still uses the selected value when storage is unavailable.
    }
  };

  const formatRefreshTimestamp = () => {
    const timestamp = document.querySelector('#last-refreshed');
    if (!timestamp) return;

    const refreshedAt = new Date(timestamp.dateTime);
    if (Number.isNaN(refreshedAt.getTime())) return;

    let timeZone = 'UTC';
    try {
      timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
      const formatted = new Intl.DateTimeFormat('en-GB', {
        day: '2-digit',
        month: 'short',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZone,
      }).format(refreshedAt);
      timestamp.textContent = `${formatted} (${timeZone})`;
    } catch {
      timestamp.textContent = timestamp.dataset.utcFallback;
    }
  };

  const errorContent = (responseText) => {
    const response = new DOMParser().parseFromString(responseText, 'text/html');
    return {
      title: response.querySelector('.error-panel h2')?.textContent?.trim() || 'Request failed',
      message: response.querySelector('.error-panel p:last-of-type')?.textContent?.trim() || 'The request could not be completed.',
    };
  };

  const showToast = ({ title, message }, severity) => {
    let region = document.querySelector('#toast-region');
    if (!region) {
      region = document.createElement('div');
      region.id = 'toast-region';
      region.className = 'toast-region';
      region.setAttribute('aria-live', 'polite');
      region.setAttribute('aria-atomic', 'true');
      document.body.append(region);
    }

    const toast = document.createElement('section');
    toast.className = `toast ${severity}`;
    toast.setAttribute('role', severity === 'critical' ? 'alert' : 'status');

    const content = document.createElement('div');
    const heading = document.createElement('strong');
    const description = document.createElement('p');
    heading.textContent = title;
    description.textContent = message;
    content.append(heading, description);

    const dismiss = document.createElement('button');
    dismiss.type = 'button';
    dismiss.className = 'toast-dismiss';
    dismiss.setAttribute('aria-label', 'Dismiss notification');
    dismiss.textContent = '×';
    dismiss.addEventListener('click', () => toast.remove());

    toast.append(content, dismiss);
    region.replaceChildren(toast);

    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => toast.remove(), 8000);
  };

  const clearRefreshTimer = () => {
    window.clearTimeout(refreshTimer);
    refreshTimer = undefined;
  };

  const scheduleRefresh = () => {
    clearRefreshTimer();

    const root = refreshRoot();
    const control = refreshControl();
    if (!root || !control || control.value === 'off' || document.hidden || automaticRefreshPending) {
      return;
    }

    refreshTimer = window.setTimeout(runAutomaticRefresh, Number(control.value) * 1000);
  };

  const initializeRefreshControl = () => {
    formatRefreshTimestamp();

    const control = refreshControl();
    if (!control) return;

    control.value = savedRefreshInterval();
    scheduleRefresh();
  };

  const runAutomaticRefresh = () => {
    const root = refreshRoot();
    if (!root || document.hidden || !window.htmx) {
      scheduleRefresh();
      return;
    }

    automaticRefreshPending = true;
    clearRefreshTimer();

    const request = window.htmx.ajax('GET', window.location.pathname + window.location.search, {
      source: root,
      target: '#consumer-group-detail',
      swap: 'outerHTML',
      push: false,
    });

    Promise.resolve(request)
      .catch(() => {
        showToast({
          title: 'Refresh failed',
          message: 'The latest consumer lag could not be loaded. Retrying automatically.',
        }, 'warning');
      })
      .finally(() => {
        automaticRefreshPending = false;
        initializeRefreshControl();
      });
  };

  document.addEventListener('htmx:beforeSwap', (event) => {
    const status = event.detail.xhr.status;
    if (status < 400 || event.detail.xhr.getResponseHeader('HX-Redirect')) return;

    event.detail.shouldSwap = false;
    event.detail.isError = false;
    showToast(errorContent(event.detail.xhr.responseText), automaticRefreshPending ? 'warning' : 'critical');
  });

  document.addEventListener('htmx:sendError', () => {
    showToast({
      title: automaticRefreshPending ? 'Refresh failed' : 'Request failed',
      message: automaticRefreshPending
        ? 'The latest consumer lag could not be loaded. Retrying automatically.'
        : 'The request could not reach Kafkaesque. Try again.',
    }, automaticRefreshPending ? 'warning' : 'critical');
  });

  document.addEventListener('htmx:beforeRequest', clearRefreshTimer);
  document.addEventListener('htmx:afterRequest', () => {
    if (!automaticRefreshPending) scheduleRefresh();
  });
  document.addEventListener('htmx:afterSwap', () => {
    initializeRefreshControl();
    syncFilterPageSize();
  });

  document.addEventListener('change', (event) => {
    if (event.target.id !== 'consumer-lag-refresh') return;

    saveRefreshInterval(event.target.value);
    scheduleRefresh();
  });

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      clearRefreshTimer();
      return;
    }
    scheduleRefresh();
  });

  const initializeUserForm = () => {
    const form = document.querySelector('[data-user-form]');
    if (!form) return;

    const email = form.querySelector('#user-email');
    const username = form.querySelector('#user-username');
    const useEmail = form.querySelector('#use-email-as-username');
    const password = form.querySelector('#new-user-password');
    const policy = form.querySelector('[data-password-policy]');
    const meter = form.querySelector('[data-password-meter]');
    const summary = form.querySelector('[data-password-summary]');
    let manualUsername = username.value;
    let passwordTimer;

    const syncUsername = () => {
      if (!useEmail.checked) return;
      username.value = email.value.trim().toLowerCase();
    };

    useEmail.addEventListener('change', () => {
      if (useEmail.checked) {
        manualUsername = username.value;
        username.readOnly = true;
        syncUsername();
        return;
      }

      username.readOnly = false;
      username.value = manualUsername;
      username.focus();
    });

    email.addEventListener('input', syncUsername);
    username.addEventListener('input', () => {
      if (!useEmail.checked) manualUsername = username.value;
    });

    const updatePasswordPolicy = () => {
      const value = password.value;
      const length = [...value].length;
      const checks = {
        length: length >= 8 && length <= 99,
        uppercase: /[A-Z]/.test(value),
        lowercase: /[a-z]/.test(value),
        number: /[0-9]/.test(value),
        special: /[!@#$%^&*()_]/.test(value),
      };

      let metCount = 0;
      policy.querySelectorAll('[data-password-rule]').forEach((item) => {
        const met = checks[item.dataset.passwordRule];
        if (met) metCount += 1;

        item.classList.toggle('healthy', met);
        item.classList.toggle('critical', !met);
        item.querySelector('[data-rule-icon]').textContent = met ? '✓' : '×';
        item.querySelector('[data-rule-status]').textContent = met ? 'Met' : 'Not met';

        const segment = meter.querySelector(`[data-password-segment="${item.dataset.passwordRule}"]`);
        segment.classList.toggle('healthy', met);
        segment.classList.toggle('critical', !met);
      });

      const progress = `${metCount} of 5 password requirements met`;
      meter.setAttribute('aria-valuenow', String(metCount));
      meter.setAttribute('aria-valuetext', progress);
      summary.textContent = progress;
    };

    password.addEventListener('input', () => {
      window.clearTimeout(passwordTimer);
      passwordTimer = window.setTimeout(updatePasswordPolicy, 250);
    });

    syncUsername();
  };

  const initializeActionMenus = () => {
    const positionMenu = (menu) => {
      const trigger = document.querySelector(`[popovertarget="${menu.id}"]`);
      if (!trigger) return;

      const gap = 6;
      const padding = 8;
      const triggerBounds = trigger.getBoundingClientRect();
      const menuBounds = menu.getBoundingClientRect();
      let top = triggerBounds.bottom + gap;
      if (top + menuBounds.height > window.innerHeight - padding) {
        top = triggerBounds.top - menuBounds.height - gap;
      }
      const left = Math.min(
        Math.max(padding, triggerBounds.right - menuBounds.width),
        window.innerWidth - menuBounds.width - padding,
      );
      menu.style.top = `${Math.max(padding, top)}px`;
      menu.style.left = `${left}px`;
    };

    document.addEventListener('beforetoggle', (event) => {
      if (!event.target.matches('.action-menu-items[popover]') || event.newState !== 'open') return;
      window.requestAnimationFrame(() => positionMenu(event.target));
    }, true);

    const repositionOpenMenus = () => {
      document.querySelectorAll('.action-menu-items:popover-open').forEach(positionMenu);
    };
    window.addEventListener('resize', repositionOpenMenus);
    document.addEventListener('scroll', repositionOpenMenus, true);
  };

  function syncFilterPageSize() {
    const selected = document.querySelector('.page-size-form select[name="page_size"]')?.value;
    if (!selected) return;

    document.querySelectorAll('.filter-form input[name="page_size"]').forEach((input) => {
      input.value = selected;
    });
  }

  const sidebar = document.querySelector('#sidebar');
  const closeButton = document.querySelector('#drawer-close');
  const overlay = document.querySelector('#drawer-overlay');

  if (sidebar && closeButton && overlay) {
    const openButton = () => document.querySelector('#menu-button');
    const focusable = () => [...sidebar.querySelectorAll('a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])')];

    const open = () => {
      sidebar.classList.add('open');
      document.body.classList.add('nav-open');
      openButton()?.setAttribute('aria-expanded', 'true');
      overlay.hidden = false;
      closeButton.focus();
    };

    const close = (restoreFocus = true) => {
      sidebar.classList.remove('open');
      document.body.classList.remove('nav-open');
      openButton()?.setAttribute('aria-expanded', 'false');
      overlay.hidden = true;
      if (restoreFocus) openButton()?.focus();
    };

    document.addEventListener('click', (event) => {
      if (event.target.closest('#menu-button')) open();
    });
    closeButton.addEventListener('click', () => close());
    overlay.addEventListener('click', () => close());

    document.addEventListener('keydown', (event) => {
      if (!sidebar.classList.contains('open')) return;
      if (event.key === 'Escape') {
        event.preventDefault();
        close();
        return;
      }
      if (event.key !== 'Tab') return;

      const items = focusable();
      const first = items[0];
      const last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    });

    window.addEventListener('resize', () => {
      if (window.innerWidth >= 768 && sidebar.classList.contains('open')) close(false);
    });
  }

  initializeRefreshControl();
  initializeUserForm();
  initializeActionMenus();
  syncFilterPageSize();
})();
