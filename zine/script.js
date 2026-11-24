(() => {
  'use strict';

  /* ------------------------------------------------------------------------
     1. Copy-to-clipboard for Quickstart code cards
     ------------------------------------------------------------------------ */
  const copyButtons = document.querySelectorAll('[data-copy-target]');
  const liveRegion = document.getElementById('copy-status');

  async function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    let ok = false;
    try {
      ok = document.execCommand('copy');
    } finally {
      document.body.removeChild(ta);
    }
    return ok;
  }

  copyButtons.forEach((btn) => {
    const labelEl = btn.querySelector('[data-copy-label]');
    const defaultLabel = labelEl ? labelEl.textContent : 'Copy';
    let resetTimer = null;

    btn.addEventListener('click', async () => {
      const targetId = btn.getAttribute('data-copy-target');
      const codeEl = targetId ? document.getElementById(targetId) : null;
      if (!codeEl) return;

      const raw = codeEl.textContent.replace(/\n$/, '');
      try {
        await copyText(raw);
        btn.classList.add('is-copied');
        if (labelEl) labelEl.textContent = 'Copied!';
        if (liveRegion) liveRegion.textContent = 'Code snippet copied to clipboard.';

        window.clearTimeout(resetTimer);
        resetTimer = window.setTimeout(() => {
          btn.classList.remove('is-copied');
          if (labelEl) labelEl.textContent = defaultLabel;
        }, 2100);
      } catch (_) {
        if (labelEl) labelEl.textContent = 'Select & copy';
      }
    });
  });

  /* ------------------------------------------------------------------------
     2. Playful tap-to-puff pufferfish doodle
     ------------------------------------------------------------------------ */
  const pufferBtn = document.getElementById('puffer-stage');
  const speechEl = document.getElementById('puffer-speech');

  if (pufferBtn && speechEl) {
    const quips = [
      'blub! hello!',
      'puff!!',
      'your bucket is the db!',
      'swap base_url & swim!',
      'hello, blowfish!',
      'stateless & bubbly!'
    ];
    let idx = 0;
    let unpuffTimer = null;

    pufferBtn.addEventListener('click', () => {
      idx = (idx + 1) % quips.length;
      speechEl.textContent = quips[idx];
      pufferBtn.classList.add('is-puffed');
      pufferBtn.setAttribute('aria-pressed', 'true');

      window.clearTimeout(unpuffTimer);
      unpuffTimer = window.setTimeout(() => {
        pufferBtn.classList.remove('is-puffed');
        pufferBtn.setAttribute('aria-pressed', 'false');
      }, 1400);
    });
  }

  /* ------------------------------------------------------------------------
     3. Optional sketchbook paper toggle (respects prefers-color-scheme first)
     ------------------------------------------------------------------------ */
  const themeBtn = document.getElementById('theme-toggle');
  const themeLabel = document.getElementById('theme-toggle-label');
  const darkQuery = window.matchMedia('(prefers-color-scheme: dark)');

  function currentEffectiveTheme() {
    const explicit = document.documentElement.getAttribute('data-theme');
    if (explicit === 'dark' || explicit === 'light') return explicit;
    return darkQuery.matches ? 'dark' : 'light';
  }

  function syncThemeButton() {
    if (!themeBtn || !themeLabel) return;
    const isDark = currentEffectiveTheme() === 'dark';
    themeLabel.textContent = isDark ? 'Day paper' : 'Night paper';
    themeBtn.setAttribute('aria-label', isDark ? 'Switch to light sketchbook paper' : 'Switch to dark sketchbook paper');
  }

  if (themeBtn) {
    syncThemeButton();
    if (typeof darkQuery.addEventListener === 'function') {
      darkQuery.addEventListener('change', syncThemeButton);
    }

    themeBtn.addEventListener('click', () => {
      const next = currentEffectiveTheme() === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      syncThemeButton();
    });
  }
})();
