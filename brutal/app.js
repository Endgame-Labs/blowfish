(() => {
  "use strict";

  const reducedMotionQuery = window.matchMedia("(prefers-reduced-motion: reduce)");

  /* ------------------------------------------------------------------------
     1. Blinking-cursor Terminal Hero (types the quickstart)
     ------------------------------------------------------------------------ */
  const terminalOutput = document.getElementById("hero-terminal-output");
  const replayBtn = document.getElementById("replay-terminal");

  // Scripted lines matching the quickstart verbatim
  const scriptSteps = [
    {
      type: "type",
      prompt: "$ ",
      text: "git clone https://github.com/endgame-labs/blowfish.git && cd blowfish",
      charDelay: 20,
      pauseAfter: 260,
    },
    {
      type: "type",
      prompt: "$ ",
      text: "docker compose up --build --wait blowfish",
      charDelay: 22,
      pauseAfter: 420,
    },
    {
      type: "instant",
      tokens: [
        ["t-ok", "✔ "],
        ["t-aqua", "blowfish v0.1.13 "],
        ["t-dim", "serving on "],
        ["t-yellow", "http://127.0.0.1:8000 "],
        ["t-dim", "(auth off)"],
      ],
      pauseAfter: 380,
    },
    {
      type: "type",
      prompt: "$ ",
      text: "python3 hello.py",
      charDelay: 28,
      pauseAfter: 260,
    },
    {
      type: "instant",
      tokens: [["t-dim", "# point the official turbopuffer SDK at blowfish"]],
      pauseAfter: 160,
    },
    {
      type: "instant",
      tokens: [
        ["t-kw", "tpuf"],
        ["", " = turbopuffer.Turbopuffer(api_key="],
        ["t-str", '"dev"'],
        ["", ", base_url="],
        ["t-yellow", '"http://127.0.0.1:8000"'],
        ["", ")"],
      ],
      pauseAfter: 180,
    },
    {
      type: "instant",
      tokens: [
        ["t-kw", "ns"],
        ["", " = tpuf.namespace("],
        ["t-str", '"hello"'],
        ["", ")"],
      ],
      pauseAfter: 180,
    },
    {
      type: "instant",
      tokens: [
        ["t-kw", "ns"],
        ["", ".write(upsert_rows=[{"],
        ["t-str", '"id"'],
        ["", ": 1, "],
        ["t-str", '"vector"'],
        ["", ": [0.1, 0.2], "],
        ["t-str", '"text"'],
        ["", ": "],
        ["t-str", '"hello, blowfish"'],
        ["", "}], ...)"],
      ],
      pauseAfter: 320,
    },
    {
      type: "instant",
      tokens: [["t-coral", "→ [Row(id=1, $dist=0.0, text='hello, blowfish')]"]],
      pauseAfter: 220,
    },
  ];

  let runId = 0;

  function makeSpan(cls, text) {
    if (!cls) return document.createTextNode(text);
    const span = document.createElement("span");
    span.className = cls;
    span.textContent = text;
    return span;
  }

  function makeCursor() {
    const c = document.createElement("span");
    c.className = "cursor";
    c.setAttribute("aria-hidden", "true");
    c.textContent = "█";
    return c;
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function renderFinalTerminal() {
    if (!terminalOutput) return;
    terminalOutput.textContent = "";
    for (const step of scriptSteps) {
      if (step.type === "type") {
        terminalOutput.appendChild(makeSpan("t-prompt", step.prompt));
        terminalOutput.appendChild(makeSpan("t-cmd", step.text));
      } else {
        for (const [cls, text] of step.tokens) {
          terminalOutput.appendChild(makeSpan(cls, text));
        }
      }
      terminalOutput.appendChild(document.createTextNode("\n"));
    }
    terminalOutput.appendChild(makeSpan("t-prompt", "$ "));
    terminalOutput.appendChild(makeCursor());
  }

  async function playTerminalAnimation() {
    if (!terminalOutput) return;
    const myRun = ++runId;

    if (reducedMotionQuery.matches) {
      renderFinalTerminal();
      return;
    }

    terminalOutput.textContent = "";
    const cursor = makeCursor();

    for (const step of scriptSteps) {
      if (myRun !== runId) return;

      if (step.type === "type") {
        const promptEl = makeSpan("t-prompt", step.prompt);
        const cmdEl = makeSpan("t-cmd", "");
        terminalOutput.appendChild(promptEl);
        terminalOutput.appendChild(cmdEl);
        terminalOutput.appendChild(cursor);

        for (let i = 0; i < step.text.length; i++) {
          if (myRun !== runId) return;
          cmdEl.textContent += step.text[i];
          await sleep(step.charDelay);
        }

        if (myRun !== runId) return;
        cursor.remove();
        terminalOutput.appendChild(document.createTextNode("\n"));
        await sleep(step.pauseAfter);
      } else {
        for (const [cls, text] of step.tokens) {
          terminalOutput.appendChild(makeSpan(cls, text));
        }
        terminalOutput.appendChild(document.createTextNode("\n"));
        terminalOutput.appendChild(cursor);
        await sleep(step.pauseAfter);
        if (myRun !== runId) return;
        cursor.remove();
      }
    }

    if (myRun !== runId) return;
    terminalOutput.appendChild(makeSpan("t-prompt", "$ "));
    terminalOutput.appendChild(cursor);
  }

  if (terminalOutput) {
    if (reducedMotionQuery.matches) {
      renderFinalTerminal();
    } else {
      playTerminalAnimation();
    }

    if (replayBtn) {
      replayBtn.addEventListener("click", () => {
        playTerminalAnimation();
      });
    }

    if (typeof reducedMotionQuery.addEventListener === "function") {
      reducedMotionQuery.addEventListener("change", (event) => {
        if (event.matches) {
          runId++;
          renderFinalTerminal();
        }
      });
    }
  }

  /* ------------------------------------------------------------------------
     2. Copy Buttons for Quickstart Snippets
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
    } catch {
      ok = false;
    }
    area.remove();
    return ok;
  }

  document.querySelectorAll(".copy-btn").forEach((btn) => {
    const label = btn.querySelector(".copy-label");
    let resetTimer = null;

    btn.addEventListener("click", async () => {
      const targetId = btn.getAttribute("data-copy-target");
      const codeEl = targetId ? document.getElementById(targetId) : null;
      if (!codeEl) return;

      const text = codeEl.textContent || "";
      let copied = false;

      if (navigator.clipboard && window.isSecureContext) {
        try {
          await navigator.clipboard.writeText(text);
          copied = true;
        } catch {
          copied = fallbackCopy(text);
        }
      } else {
        copied = fallbackCopy(text);
      }

      if (label) {
        label.textContent = copied ? "Copied! ✔" : "Copy failed";
      }
      btn.classList.toggle("is-copied", copied);

      clearTimeout(resetTimer);
      resetTimer = setTimeout(() => {
        if (label) label.textContent = "Copy";
        btn.classList.remove("is-copied");
      }, 2000);
    });
  });

  /* ------------------------------------------------------------------------
     3. Interactive Pufferfish Mascot
     ------------------------------------------------------------------------ */
  const mascotBtn = document.getElementById("mascot-btn");
  const mascotBubble = document.getElementById("mascot-bubble");

  const quips = [
    "Point base_url here and splash in!",
    "Your bucket is the database. Bloop!",
    "I love turbopuffer—just swap base_url!",
    "10M docs on a 4 GB machine. Cozy reef!",
    "Single Rust binary. Zero cluster drama!",
    "Pre-1.0 puffer: still growing new spikes!",
  ];

  let quipIndex = 0;
  let puffTimer = null;

  if (mascotBtn && mascotBubble) {
    mascotBtn.addEventListener("click", () => {
      quipIndex = (quipIndex + 1) % quips.length;
      mascotBubble.textContent = quips[quipIndex];

      if (!reducedMotionQuery.matches) {
        mascotBtn.classList.add("is-puffed");
        clearTimeout(puffTimer);
        puffTimer = setTimeout(() => {
          mascotBtn.classList.remove("is-puffed");
        }, 320);
      }
    });
  }
})();
