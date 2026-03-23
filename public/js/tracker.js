// ─── Tracker ──────────────────────────────────────────────────────────────────
function handleTrackerLoadFailure() {
  console.warn("[Unomi] Tracker script failed to load — is Unomi running?");
}

var _initialLoadDone = false;

function initTracker() {
  if (typeof unomiWebTracker === "undefined") {
    console.warn("[Unomi] unomiWebTracker not available.");
    return;
  }
  try {
    var SESSION_COOKIE = "fooddemo-session-id";
    if (unomiWebTracker.getCookie(SESSION_COOKIE) == null) {
      unomiWebTracker.setCookie(SESSION_COOKIE, unomiWebTracker.generateGuid(), 1);
    }

    unomiWebTracker.initTracker({
      scope: "fooddemo",
      contextServerUrl: "http://localhost:3000",
      site: { siteInfo: { siteID: "fooddemo" } },
      page: {
        pageInfo: {
          pageID: "shop-" + document.location.pathname.replace(/\//g, "-"),
          pageName: document.title,
          pagePath: document.location.pathname,
          destinationURL: document.location.origin + document.location.pathname,
          language: "en",
          categories: [],
          tags: []
        },
        attributes: {},
        consentTypes: []
      },
      requiredProfileProperties: ["*"],
      requireSegments: true,
      requireScores: true,
      wemInitConfig: {
        contextServerUrl: "http://localhost:3000",
        timeoutInMilliseconds: "1500",
        contextServerCookieName: "context-profile-id",
        activateWem: true,
        trackerSessionIdCookieName: SESSION_COOKIE,
        trackerProfileIdCookieName: "context-profile-id",
        requiredProfileProperties: ["*"],
        requireSegments: true,
        requireScoring: true,
      }
    });

    // Register callback BEFORE loadContext so it fires on the first load.
    unomiWebTracker._registerCallback(function () {
      try {
        var ctx = unomiWebTracker.getLoadedContext();
        window.unomiContext = ctx;
        syncConsentsFromProfile(ctx);
        console.log("[Unomi] Context loaded. profileId:", ctx && ctx.profileId, "segments:", ctx && ctx.profileSegments);
        applyPersonalization(ctx);
        updateDebugPanel(ctx);

        // Send explicit view event on every page load so pageViewCount increments.
        // loadContext() alone does NOT send a view event in Unomi 2.6.
        if (!_initialLoadDone) {
          _initialLoadDone = true;
          trackPageView();
<<<<<<< HEAD
          refreshContext(400);
=======
          refreshContext(1500);
>>>>>>> 359b767976320fae1bd23039c03debd9813f0065
        }

        // Scores are not returned in context.json in Unomi 2.6 -- fetch via profile API
        var profileId = ctx && ctx.profileId;
        if (profileId) {
          fetch("/api/profile/" + profileId)
            .then(function (r) { return r.json(); })
            .then(function (profile) {
              var score = (profile.scores && profile.scores["engagementScore"]) || 0;
              document.getElementById("engagement-badge").textContent = "Score: " + score;
              // Also update debug panel with full profile data from API
              if (profile.properties) {
                document.getElementById("debug-pageviews").textContent = profile.properties.pageViewCount || "0";
                document.getElementById("debug-cartadds").textContent = profile.properties.totalCartAdds || "0";
                document.getElementById("debug-segments").textContent = (profile.segments || []).join(", ") || "none";
                document.getElementById("debug-profileid").textContent = profileId.length > 18 ? profileId.substring(0, 18) + "…" : profileId;
              }
            })
            .catch(function (e) { console.warn("[Score fetch]", e); });
        }
      } catch (e) {
        console.error("[Unomi] Callback error:", e);
      }
    }, "foodDemoCallback");
    console.log("[Unomi] loadContext() called");

    // Explicitly fire loadContext after callback is registered
    setTimeout(function () {
      try {
        var ctx = unomiWebTracker.getLoadedContext();
        if (!ctx || !ctx.profileId) {
          unomiWebTracker.loadContext({
            requireSegments: true,
            requireScoring: true,
            requiredProfileProperties: ["*"]
          });
          console.log("[Unomi] loadContext() called explicitly");
        } else {
          console.log("[Unomi] Context already loaded by initTracker, skipping explicit call");
        }
      } catch (e) {
        console.warn("[Unomi] loadContext check failed:", e);
      }
    }, 100);
  } catch (e) {
    if (e && e.message && e.message.indexOf("already requested") !== -1) {
      console.log("[Unomi] loadContext already in flight from initTracker — callback will fire");
    } else {
      console.error("[Unomi] Tracker init failed:", e);
    }
  }
}

function refreshContext(delayMs) {
  if (typeof unomiWebTracker === "undefined") return;
  function getRawCookie(name) {
    var entry = document.cookie.split(";").map(function (c) { return c.trim(); })
      .find(function (c) { return c.startsWith(name + "="); });
    return entry ? decodeURIComponent(entry.slice(name.length + 1)) : null;
  }
  setTimeout(function () {
    var profileId = getRawCookie("context-profile-id");
    if (!profileId) return;
    fetch("/api/profile/" + profileId)
      .then(function (r) { return r.json(); })
      .then(function (profile) {
        var ctx = {
          profileId: profile.itemId,
          profileProperties: profile.properties || {},
          profileSegments: profile.segments || [],
          scores: profile.scores || {}
        };
        window.unomiContext = ctx;
        applyPersonalization(ctx);
        updateDebugPanel(ctx);
        var score = (profile.scores && profile.scores["engagementScore"]) || 0;
        document.getElementById("engagement-badge").textContent = "Score: " + score;
        if (profile.properties) {
          document.getElementById("debug-pageviews").textContent = profile.properties.pageViewCount || "0";
          document.getElementById("debug-cartadds").textContent = profile.properties.totalCartAdds || "0";
          document.getElementById("debug-segments").textContent = (profile.segments || []).join(", ") || "none";
          document.getElementById("debug-profileid").textContent = profileId.length > 18 ? profileId.substring(0, 18) + "…" : profileId;
        }
      })
      .catch(function (e) { console.warn("[Unomi] Refresh failed:", e); });
  }, delayMs || 900);
}