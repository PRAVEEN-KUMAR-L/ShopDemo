// ─── Login ────────────────────────────────────────────────────────────────────
function openLoginModal() {
  var user = JSON.parse(localStorage.getItem("loggedInUser") || "null");
  if (user) { doLogout(); return; }
  document.getElementById("login-modal").classList.add("active");
  document.getElementById("overlay").classList.add("visible");
  document.getElementById("login-email").focus();
}

function closeLoginModal() {
  document.getElementById("login-modal").classList.remove("active");
  document.getElementById("overlay").classList.remove("visible");
  document.getElementById("login-error").classList.add("hidden");
}

async function doLogin() {
  var email = document.getElementById("login-email").value.trim();
  var errEl = document.getElementById("login-error");
  errEl.classList.add("hidden");
  if (!email) { errEl.textContent = "Please enter your email."; errEl.classList.remove("hidden"); return; }

  function getRawCookie(name) {
    var entry = document.cookie.split(";").map(function (c) { return c.trim(); })
      .find(function (c) { return c.startsWith(name + "="); });
    return entry ? decodeURIComponent(entry.slice(name.length + 1)) : null;
  }

  var loginBtn = document.getElementById("login-btn");
  var origText = loginBtn ? loginBtn.textContent : "Sign In";
  var anonymousProfileId = getRawCookie("context-profile-id");

  // Poll up to 4.2 s for the cookie
  if (!anonymousProfileId) {
    if (loginBtn) loginBtn.textContent = "Connecting to tracker…";
    await new Promise(function (resolve) {
      var attempts = 0;
      var iv = setInterval(function () {
        anonymousProfileId = getRawCookie("context-profile-id");
        attempts++;
        if (anonymousProfileId || attempts >= 14) { clearInterval(iv); resolve(); }
      }, 300);
    });
    if (loginBtn) loginBtn.textContent = origText;
  }

  console.log("[Login] anonymousProfileId =", anonymousProfileId || "null after polling");

  try {
    var res = await fetch("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ email: email, password: document.getElementById("login-password").value, anonymousProfileId: anonymousProfileId })
    });
    var data = await res.json();
    if (!data.success) { errEl.textContent = data.error || "Login failed."; errEl.classList.remove("hidden"); return; }
    localStorage.setItem("loggedInUser", JSON.stringify(data.user));
    document.getElementById("auth-label").textContent = data.user.name;

    // If server merged us into an existing master profile, update the browser
    // cookie NOW before loadContext() fires, so the tracker sends the master
    // profile UUID and gets back the full history, segments and scores.
    if (data.masterProfileId) {
      var exp = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toUTCString();
      document.cookie = "context-profile-id=" + data.masterProfileId + "; path=/; expires=" + exp;
      // Also clear the session cookie so the next context.json creates a
      // fresh session tied to the master profile instead of Unomi overwriting
      // our cookie back to the old anonymous profile's session.
      document.cookie = "ecommerce-demo-session-id=; path=/; expires=Thu, 01 Jan 1970 00:00:00 UTC";
      console.log("[Login] Cookie switched to master profile:", data.masterProfileId);
    }

    closeLoginModal();
    window.location.reload();
  } catch (e) {
    errEl.textContent = "Network error. Is the server running?";
    errEl.classList.remove("hidden");
    console.error("[Login] fetch failed:", e);
  }
}

function doLogout() {
  localStorage.removeItem("loggedInUser");
  document.getElementById("auth-label").textContent = "Login";
  var exp = "expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
  document.cookie = "context-profile-id=; " + exp;
  document.cookie = "ecommerce-demo-session-id=; " + exp;
  window.location.reload();
}
