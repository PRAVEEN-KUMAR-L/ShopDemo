// ─── Consent Management ───────────────────────────────────────────────────────
// Unomi stores consents on the profile via the built-in modifyConsent event.
// Two consent types:
//   personalization-tracking  — page views, product views, cart behaviour
//   order-history-tracking    — purchase history
//
// Consent status is cached in localStorage for immediate UI response before
// the Unomi context loads. The source of truth is always Unomi's profile.

var CONSENT_PERSONALIZATION = "personalization-tracking";
var CONSENT_ORDER_HISTORY = "order-history-tracking";

// ─── Consent state helpers ────────────────────────────────────────────────────

function isTrackingAllowed() {
    return localStorage.getItem(CONSENT_PERSONALIZATION) === "GRANTED";
}

function isOrderTrackingAllowed() {
    return localStorage.getItem(CONSENT_ORDER_HISTORY) === "GRANTED";
}

function getConsentStatus(type) {
    return localStorage.getItem(type) || "PENDING";
}

// ─── Send consent event to Unomi ──────────────────────────────────────────────
// Uses the built-in modifyConsent event type which Unomi handles natively.
// The built-in modifyAnyConsent rule fires automatically -- no custom rule needed.

function sendConsentEvent(type, status) {
    if (typeof unomiWebTracker === "undefined") return;
    try {
        unomiWebTracker.collectEvent({
            eventType: "modifyConsent",
            scope: "fooddemo",
            source: { itemId: "shop-main", itemType: "page", scope: "fooddemo" },
            properties: {
                consent: {
                    typeIdentifier: type,
                    scope: "fooddemo",
                    status: status,
                    statusDate: new Date().toISOString(),
                }
            }
        });
        console.log("[Consent] sent:", type, "->", status);
    } catch (e) { console.error("[Consent] event failed:", e); }
}

// ─── Update a single consent ──────────────────────────────────────────────────

function updateConsent(type, status) {
    localStorage.setItem(type, status);
    sendConsentEvent(type, status);
}

// ─── Grant / deny all ────────────────────────────────────────────────────────

function grantAllConsents() {
    localStorage.setItem(CONSENT_PERSONALIZATION, "GRANTED");
    localStorage.setItem(CONSENT_ORDER_HISTORY, "GRANTED");
    sendConsentEvent(CONSENT_PERSONALIZATION, "GRANTED");
    setTimeout(function () {
        sendConsentEvent(CONSENT_ORDER_HISTORY, "GRANTED");
    }, 300);
    hideConsentBanner();
}

function denyAllConsents() {
    localStorage.setItem(CONSENT_PERSONALIZATION, "DENIED");
    localStorage.setItem(CONSENT_ORDER_HISTORY, "DENIED");
    sendConsentEvent(CONSENT_PERSONALIZATION, "DENIED");
    setTimeout(function () {
        sendConsentEvent(CONSENT_ORDER_HISTORY, "DENIED");
    }, 300);
    hideConsentBanner();
}

// ─── Consent banner ───────────────────────────────────────────────────────────

function hideConsentBanner() {
    var banner = document.getElementById("consent-banner");
    if (banner) banner.classList.add("hidden");
}

function showConsentBanner() {
    // Only show if user has not yet made a choice
    var p = getConsentStatus(CONSENT_PERSONALIZATION);
    var o = getConsentStatus(CONSENT_ORDER_HISTORY);
    if (p !== "PENDING" || o !== "PENDING") return;
    var banner = document.getElementById("consent-banner");
    if (banner) banner.classList.remove("hidden");
}

// ─── Sync consent state from Unomi profile ───────────────────────────────────
// Called from tracker callback after context loads.
// Unomi is the source of truth -- if profile has consents, sync to localStorage.

function syncConsentsFromProfile(ctx) {
    if (!ctx) return;
    var consents = ctx.consents || {};
    [CONSENT_PERSONALIZATION, CONSENT_ORDER_HISTORY].forEach(function (type) {
        var key = "fooddemo/" + type;
        if (consents[key] && consents[key].status) {
            localStorage.setItem(type, consents[key].status);
        }
    });
}

// ─── Consent modal (customize view) ──────────────────────────────────────────

// Tracks pending choices in the modal before user clicks Save
var _pendingConsents = {};

function showConsentDetails() {
    // Initialize pending state from current localStorage values
    _pendingConsents[CONSENT_PERSONALIZATION] = getConsentStatus(CONSENT_PERSONALIZATION);
    _pendingConsents[CONSENT_ORDER_HISTORY] = getConsentStatus(CONSENT_ORDER_HISTORY);

    // Render toggle button states
    renderToggleButton(
        document.getElementById("toggle-personalization"),
        _pendingConsents[CONSENT_PERSONALIZATION]
    );
    renderToggleButton(
        document.getElementById("toggle-order-history"),
        _pendingConsents[CONSENT_ORDER_HISTORY]
    );

    document.getElementById("consent-modal").classList.add("active");
}

function closeConsentModal() {
    document.getElementById("consent-modal").classList.remove("active");
}

function renderToggleButton(btn, status) {
    if (!btn) return;
    if (status === "GRANTED") {
        btn.textContent = "Allowed";
        btn.className = "shrink-0 px-3 py-1.5 text-xs font-semibold rounded-lg transition bg-green-100 text-green-700 border border-green-300";
    } else {
        btn.textContent = "Blocked";
        btn.className = "shrink-0 px-3 py-1.5 text-xs font-semibold rounded-lg transition bg-red-100 text-red-700 border border-red-300";
    }
}

function toggleConsentOption(type, btn) {
    // Toggle between GRANTED and DENIED in the modal (not saved yet)
    var current = _pendingConsents[type] || "DENIED";
    _pendingConsents[type] = current === "GRANTED" ? "DENIED" : "GRANTED";
    renderToggleButton(btn, _pendingConsents[type]);
}

function saveConsentPreferences() {
    var types = Object.keys(_pendingConsents);
    types.forEach(function (type, index) {
        setTimeout(function () {
            updateConsent(type, _pendingConsents[type]);
        }, index * 300);
    });
    setTimeout(function () {
        closeConsentModal();
        hideConsentBanner();
        if (window.unomiContext) applyPersonalization(window.unomiContext);
    }, types.length * 300 + 100);
}