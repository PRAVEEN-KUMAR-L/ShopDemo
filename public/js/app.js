// ─── Category filter ──────────────────────────────────────────────────────────
document.querySelectorAll(".cat-btn").forEach(function (btn) {
  btn.addEventListener("click", function () {
    document.querySelectorAll(".cat-btn").forEach(function (b) { b.classList.remove("active"); });
    btn.classList.add("active");
    currentCategory = btn.dataset.cat;
    renderProducts(currentCategory);
  });
});

// ─── Init ─────────────────────────────────────────────────────────────────────
(function init() {
  var stored = JSON.parse(localStorage.getItem("loggedInUser") || "null");
  if (stored) document.getElementById("auth-label").textContent = stored.name;
  renderProducts("All");
  // Show consent banner if user hasn't made a choice yet
  showConsentBanner();
  setTimeout(initTracker, 150);
})();