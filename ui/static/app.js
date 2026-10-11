(() => {
  document.addEventListener('htmx:beforeSwap', (event) => {
    if ([400, 404, 502, 504].includes(event.detail.xhr.status)) {
      event.detail.shouldSwap = true;
      event.detail.isError = false;
    }
  });

  const sidebar = document.querySelector('#sidebar');
  const openButton = document.querySelector('#menu-button');
  const closeButton = document.querySelector('#drawer-close');
  const overlay = document.querySelector('#drawer-overlay');
  if (!sidebar || !openButton || !closeButton || !overlay) return;

  const focusable = () => [...sidebar.querySelectorAll('a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])')];

  const open = () => {
    sidebar.classList.add('open');
    document.body.classList.add('nav-open');
    openButton.setAttribute('aria-expanded', 'true');
    overlay.hidden = false;
    closeButton.focus();
  };

  const close = (restoreFocus = true) => {
    sidebar.classList.remove('open');
    document.body.classList.remove('nav-open');
    openButton.setAttribute('aria-expanded', 'false');
    overlay.hidden = true;
    if (restoreFocus) openButton.focus();
  };

  openButton.addEventListener('click', open);
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
})();
