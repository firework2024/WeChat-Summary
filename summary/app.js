const slides = Array.from(document.querySelectorAll(".slide"));
const dotsHost = document.querySelector("#dots");
const prevBtn = document.querySelector("#prevBtn");
const nextBtn = document.querySelector("#nextBtn");
const progressBar = document.querySelector("#progressBar");

let current = 0;

function enhanceMetrics() {
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  const textNodes = [];
  while (walker.nextNode()) {
    const node = walker.currentNode;
    if (node.nodeValue.includes("【")) textNodes.push(node);
  }

  textNodes.forEach((node) => {
    const frag = document.createDocumentFragment();
    const parts = node.nodeValue.split(/(【[^】]+】)/g);
    parts.forEach((part) => {
      if (!part) return;
      if (/^【[^】]+】$/.test(part)) {
        const span = document.createElement("span");
        span.className = "metric";
        span.textContent = part;
        frag.appendChild(span);
      } else {
        frag.appendChild(document.createTextNode(part));
      }
    });
    node.parentNode.replaceChild(frag, node);
  });
}

function buildDots() {
  slides.forEach((slide, index) => {
    const dot = document.createElement("button");
    dot.className = "dot";
    dot.type = "button";
    dot.setAttribute("aria-label", `第 ${index + 1} 页`);
    dot.addEventListener("click", () => goTo(index));
    dotsHost.appendChild(dot);
  });
}

function goTo(index) {
  current = Math.max(0, Math.min(slides.length - 1, index));
  slides.forEach((slide, i) => {
    slide.classList.toggle("is-active", i === current);
    slide.classList.toggle("is-before", i < current);
  });

  Array.from(dotsHost.children).forEach((dot, i) => {
    dot.classList.toggle("is-active", i === current);
  });

  prevBtn.disabled = current === 0;
  nextBtn.disabled = current === slides.length - 1;
  progressBar.style.width = `${((current + 1) / slides.length) * 100}%`;
}

function next() {
  goTo(current + 1);
}

function prev() {
  goTo(current - 1);
}

prevBtn.addEventListener("click", prev);
nextBtn.addEventListener("click", next);

window.addEventListener("keydown", (event) => {
  if (event.key === "ArrowRight" || event.key === "PageDown" || event.key === " ") {
    event.preventDefault();
    next();
  }
  if (event.key === "ArrowLeft" || event.key === "PageUp") {
    event.preventDefault();
    prev();
  }
  if (event.key === "Home") goTo(0);
  if (event.key === "End") goTo(slides.length - 1);
});

let touchStartX = 0;
let touchStartY = 0;

window.addEventListener("touchstart", (event) => {
  const touch = event.changedTouches[0];
  touchStartX = touch.clientX;
  touchStartY = touch.clientY;
}, { passive: true });

window.addEventListener("touchend", (event) => {
  const touch = event.changedTouches[0];
  const dx = touch.clientX - touchStartX;
  const dy = touch.clientY - touchStartY;
  if (Math.abs(dx) > 48 && Math.abs(dx) > Math.abs(dy) * 1.4) {
    if (dx < 0) next();
    else prev();
  }
}, { passive: true });

enhanceMetrics();
buildDots();
goTo(0);
