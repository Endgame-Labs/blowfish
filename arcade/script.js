/* blowfish arcade interactions — zero trackers, pure vanilla JS */
(() => {
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  const prefersLight = window.matchMedia("(prefers-color-scheme: light)");

  /* ------------------------------------------------------------------------
     Arcade Score Counter
     ------------------------------------------------------------------------ */
  const scoreEl = document.getElementById("arcade-score");
  const scoreWrap = scoreEl ? scoreEl.closest(".hud-score") : null;
  let score = 100;
  let bumpTimer = null;

  function addScore(points) {
    score = Math.min(999999, score + points);
    if (!scoreEl) return;
    scoreEl.textContent = String(score).padStart(6, "0");
    if (scoreWrap && !reducedMotion.matches) {
      scoreWrap.classList.add("score-bump");
      clearTimeout(bumpTimer);
      bumpTimer = setTimeout(() => scoreWrap.classList.remove("score-bump"), 420);
    }
  }

  /* ------------------------------------------------------------------------
     Interactive Pixel Pufferfish
     ------------------------------------------------------------------------ */
  const pufferBtn = document.getElementById("puffer-stage");
  const pufferBurst = document.getElementById("puffer-burst");
  const pufferFloat = document.getElementById("puffer-float");
  const mascotHint = document.getElementById("mascot-hint");

  const puffCalls = [
    "PUFF! +100",
    "SPIKY! +100",
    "BLOOP! +100",
    "1UP! +100",
    "MAX PUFF!"
  ];
  let puffCount = 0;
  let lastHoverScoreAt = 0;

  function spawnPixelBurst() {
    if (!pufferBurst || reducedMotion.matches) return;
    const vectors = [
      { dx: "-92px", dy: "-74px", coin: false },
      { dx: "94px",  dy: "-70px", coin: true },
      { dx: "-104px", dy: "8px",  coin: true },
      { dx: "106px", dy: "10px",  coin: false },
      { dx: "-72px", dy: "86px",  coin: false },
      { dx: "78px",  dy: "84px",  coin: true },
      { dx: "0px",   dy: "-102px", coin: true }
    ];
    vectors.forEach((vec) => {
      const dot = document.createElement("span");
      dot.className = vec.coin ? "burst-particle is-coin" : "burst-particle";
      dot.style.setProperty("--dx", vec.dx);
      dot.style.setProperty("--dy", vec.dy);
      pufferBurst.appendChild(dot);
      setTimeout(() => dot.remove(), 680);
    });
  }

  function triggerPuffFeedback() {
    puffCount += 1;
    if (pufferFloat) {
      pufferFloat.textContent = puffCalls[(puffCount - 1) % puffCalls.length];
    }
    addScore(100);
    spawnPixelBurst();
  }

  if (pufferBtn) {
    pufferBtn.addEventListener("pointerenter", (event) => {
      if (event.pointerType === "touch") return;
      const now = Date.now();
      if (now - lastHoverScoreAt > 500) {
        lastHoverScoreAt = now;
        triggerPuffFeedback();
      }
    });

    pufferBtn.addEventListener("click", () => {
      const puffed = pufferBtn.classList.toggle("is-puffed");
      pufferBtn.setAttribute("aria-pressed", String(puffed));
      if (puffed) {
        triggerPuffFeedback();
        if (mascotHint) {
          mascotHint.innerHTML = '<span class="blink-dot" aria-hidden="true">★</span> PUFFED! TAP AGAIN TO CHILL';
        }
      } else if (mascotHint) {
        mascotHint.innerHTML = '<span class="blink-dot" aria-hidden="true">▶</span> HOVER OR TAP TO PUFF!';
      }
    });
  }

  /* ------------------------------------------------------------------------
     Quickstart Copy Buttons
     ------------------------------------------------------------------------ */
  function fallbackCopy(text) {
    const area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.style.position = "fixed";
    area.style.top = "-9999px";
    document.body.appendChild(area);
    area.select();
    let ok = false;
    try {
      ok = document.execCommand("copy");
    } catch (_) {
      ok = false;
    }
    area.remove();
    return ok;
  }

  document.querySelectorAll(".copy-btn").forEach((btn) => {
    const targetId = btn.getAttribute("data-copy-target");
    const labelEl = btn.querySelector(".copy-label");
    let resetTimer = null;

    btn.addEventListener("click", async () => {
      const codeEl = targetId ? document.getElementById(targetId) : null;
      if (!codeEl) return;
      const text = codeEl.textContent || "";

      let copied = false;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        try {
          await navigator.clipboard.writeText(text);
          copied = true;
        } catch (_) {
          copied = fallbackCopy(text);
        }
      } else {
        copied = fallbackCopy(text);
      }

      if (copied) {
        addScore(50);
        btn.classList.add("is-copied");
        if (labelEl) labelEl.textContent = "COPIED! +50";
        clearTimeout(resetTimer);
        resetTimer = setTimeout(() => {
          btn.classList.remove("is-copied");
          if (labelEl) labelEl.textContent = "COPY";
        }, 1800);
      } else if (labelEl) {
        labelEl.textContent = "SELECT TO COPY";
        clearTimeout(resetTimer);
        resetTimer = setTimeout(() => {
          labelEl.textContent = "COPY";
        }, 2000);
      }
    });
  });

  /* ------------------------------------------------------------------------
     Day / Night Arcade Palette Switcher (defaults to prefers-color-scheme)
     ------------------------------------------------------------------------ */
  const themeBtn = document.getElementById("theme-toggle");
  const themeText = document.getElementById("theme-toggle-text");

  function currentTheme() {
    const explicit = document.documentElement.getAttribute("data-theme");
    if (explicit === "light" || explicit === "dark") return explicit;
    return prefersLight.matches ? "light" : "dark";
  }

  function syncThemeLabel() {
    if (!themeBtn || !themeText) return;
    const mode = currentTheme();
    themeText.textContent = mode === "light" ? "DAY" : "CRT";
    themeBtn.setAttribute(
      "aria-label",
      mode === "light"
        ? "Switch to dark CRT arcade ocean palette"
        : "Switch to bright daytime ocean palette"
    );
  }

  syncThemeLabel();

  if (typeof prefersLight.addEventListener === "function") {
    prefersLight.addEventListener("change", syncThemeLabel);
  }

  if (themeBtn) {
    themeBtn.addEventListener("click", () => {
      const next = currentTheme() === "light" ? "dark" : "light";
      document.documentElement.setAttribute("data-theme", next);
      syncThemeLabel();
    });
  }
})();
