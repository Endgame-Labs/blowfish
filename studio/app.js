(function () {
  "use strict";

  /* ------------------------------------------------------------------------
     1. Quickstart Copy-to-Clipboard Buttons
     ------------------------------------------------------------------------ */
  var copyButtons = document.querySelectorAll("[data-copy-target]");

  function fallbackCopy(text) {
    var area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.style.position = "fixed";
    area.style.opacity = "0";
    area.style.pointerEvents = "none";
    document.body.appendChild(area);
    area.select();
    var ok = false;
    try {
      ok = document.execCommand("copy");
    } catch (_) {
      ok = false;
    }
    document.body.removeChild(area);
    return ok;
  }

  function showCopiedState(btn) {
    var label = btn.querySelector(".copy-label");
    var iconCopy = btn.querySelector(".icon-copy");
    var iconCheck = btn.querySelector(".icon-check");
    if (!label) return;

    btn.classList.add("copied");
    label.textContent = "Copied!";
    if (iconCopy) iconCopy.hidden = true;
    if (iconCheck) iconCheck.hidden = false;

    window.clearTimeout(btn._resetTimer);
    btn._resetTimer = window.setTimeout(function () {
      btn.classList.remove("copied");
      label.textContent = "Copy";
      if (iconCopy) iconCopy.hidden = false;
      if (iconCheck) iconCheck.hidden = true;
    }, 2000);
  }

  copyButtons.forEach(function (btn) {
    btn.addEventListener("click", function () {
      var targetId = btn.getAttribute("data-copy-target");
      var codeEl = targetId ? document.getElementById(targetId) : null;
      if (!codeEl) return;

      var text = codeEl.textContent.replace(/\n$/, "");

      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(
          function () {
            showCopiedState(btn);
          },
          function () {
            if (fallbackCopy(text)) showCopiedState(btn);
          }
        );
      } else if (fallbackCopy(text)) {
        showCopiedState(btn);
      }
    });
  });

  /* ------------------------------------------------------------------------
     2. Hero Base-URL Switcher Preview (blowfish <-> turbopuffer)
     ------------------------------------------------------------------------ */
  var modeButtons = document.querySelectorAll("[data-endpoint-mode]");
  var previewCode = document.getElementById("endpoint-preview-code");
  var previewCaption = document.getElementById("endpoint-preview-caption");

  var snippets = {
    blowfish: {
      html:
        '<span class="tok-k">import</span> turbopuffer\n' +
        'tpuf <span class="tok-p">=</span> turbopuffer.<span class="tok-f">Turbopuffer</span>(\n' +
        '  api_key<span class="tok-p">=</span><span class="tok-s">"dev"</span>,\n' +
        '  base_url<span class="tok-p">=</span><span class="tok-s">"http://127.0.0.1:8000"</span>,\n' +
        ")",
      caption: "Local dev, CI, on-prem, or your own S3 bucket — same official SDK."
    },
    turbopuffer: {
      html:
        '<span class="tok-k">import</span> turbopuffer\n' +
        'tpuf <span class="tok-p">=</span> turbopuffer.<span class="tok-f">Turbopuffer</span>(\n' +
        '  api_key<span class="tok-p">=</span>os.environ[<span class="tok-s">"TURBOPUFFER_API_KEY"</span>],\n' +
        '  <span class="tok-c"># default turbopuffer cloud endpoint</span>\n' +
        ")",
      caption: "Ready for production? Drop base_url and swim straight onto turbopuffer."
    }
  };

  modeButtons.forEach(function (btn) {
    btn.addEventListener("click", function () {
      var mode = btn.getAttribute("data-endpoint-mode");
      var entry = snippets[mode];
      if (!entry || !previewCode || !previewCaption) return;

      modeButtons.forEach(function (other) {
        other.setAttribute("aria-pressed", other === btn ? "true" : "false");
      });
      previewCode.innerHTML = entry.html;
      previewCaption.textContent = entry.caption;
    });
  });

  /* ------------------------------------------------------------------------
     3. Optional Theme Toggle (respects prefers-color-scheme by default)
     ------------------------------------------------------------------------ */
  var themeToggle = document.getElementById("theme-toggle");
  var root = document.documentElement;
  var darkQuery = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;

  function effectiveTheme() {
    var explicit = root.getAttribute("data-theme");
    if (explicit === "light" || explicit === "dark") return explicit;
    return darkQuery && !darkQuery.matches ? "light" : "dark";
  }

  function syncThemeButton() {
    if (!themeToggle) return;
    var isLight = effectiveTheme() === "light";
    var sun = themeToggle.querySelector(".icon-sun");
    var moon = themeToggle.querySelector(".icon-moon");
    if (sun) sun.hidden = !isLight;
    if (moon) moon.hidden = isLight;
    themeToggle.setAttribute(
      "aria-label",
      isLight ? "Switch to dark ocean theme" : "Switch to sunlit light theme"
    );
    themeToggle.setAttribute(
      "title",
      isLight ? "Switch to dark ocean theme" : "Switch to sunlit light theme"
    );
  }

  if (themeToggle) {
    syncThemeButton();
    themeToggle.addEventListener("click", function () {
      var next = effectiveTheme() === "light" ? "dark" : "light";
      root.setAttribute("data-theme", next);
      syncThemeButton();
    });

    if (darkQuery && typeof darkQuery.addEventListener === "function") {
      darkQuery.addEventListener("change", function () {
        if (!root.hasAttribute("data-theme")) {
          syncThemeButton();
        }
      });
    }
  }
})();
