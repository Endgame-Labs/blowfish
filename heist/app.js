(() => {
  const prefersReduced =
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  /* 1. Copy-to-clipboard for quickstart code panels */
  const liveRegion = document.getElementById("copy-status");
  const copyButtons = document.querySelectorAll("[data-copy-target]");

  async function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.top = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
  }

  copyButtons.forEach((btn) => {
    const labelEl = btn.querySelector(".copy-label");
    let resetTimer = null;

    btn.addEventListener("click", async () => {
      const targetId = btn.getAttribute("data-copy-target");
      const codeEl = targetId ? document.getElementById(targetId) : null;
      if (!codeEl) return;

      const text = codeEl.textContent || "";
      try {
        await copyText(text);
        btn.classList.add("is-copied");
        if (labelEl) labelEl.textContent = "Copied!";
        if (liveRegion) liveRegion.textContent = "Code snippet copied to clipboard.";
        clearTimeout(resetTimer);
        resetTimer = setTimeout(() => {
          btn.classList.remove("is-copied");
          if (labelEl) labelEl.textContent = "Copy";
        }, 1800);
      } catch {
        if (labelEl) labelEl.textContent = "Select & copy";
        if (liveRegion) liveRegion.textContent = "Could not copy automatically. Please select and copy the code.";
      }
    });
  });

  /* 2. Playful blowfish puff on click */
  const mascotBtn = document.getElementById("mascot-puff");
  if (mascotBtn && !prefersReduced) {
    mascotBtn.addEventListener("click", () => {
      mascotBtn.classList.remove("is-puffed");
      void mascotBtn.offsetWidth;
      mascotBtn.classList.add("is-puffed");
    });
    mascotBtn.addEventListener("animationend", () => {
      mascotBtn.classList.remove("is-puffed");
    });
  }

  /* 3. Progressive angular scroll-reveal */
  if (!prefersReduced && "IntersectionObserver" in window) {
    const items = document.querySelectorAll("[data-reveal]");
    if (items.length) {
      document.documentElement.classList.add("js-anim");

      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              entry.target.classList.add("is-visible");
              observer.unobserve(entry.target);
            }
          });
        },
        { threshold: 0.12, rootMargin: "0px 0px -30px 0px" }
      );

      items.forEach((el) => observer.observe(el));
    }
  }
})();
