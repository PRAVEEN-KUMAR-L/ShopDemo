"use strict";

const express = require("express");
const fetch = require("node-fetch");
const { UNOMI_BASE } = require("./config");
const { cleanCookies } = require("./unomiHelper");

const router = express.Router();

// ─── Tracker Proxy ────────────────────────────────────────────────────────────
// All browser->Unomi traffic is proxied through Express so the browser only
// ever talks to localhost:3000 (same-origin -- no CORS issues).
//
// Fixes applied here vs naive proxy:
//   * r.text() + res.send()  -- avoids double JSON parse that drops fields
//   * req.url not hardcoded  -- preserves ?sessionId=&profileId= query params
//   * GET + POST for context.json -- tracker version determines which it uses
//   * express.text() before express.json() -- preserves raw body for forwarding
//   * cleanCookies() -- strips Secure/HttpOnly/SameSite/Domain so cookie
//     lands on localhost HTTP and is readable by document.cookie

// Tracker JS bundle
router.get("/tracker/*", async (req, res) => {
  try {
    const r = await fetch(`${UNOMI_BASE}${req.path}`);
    const text = await r.text();
    console.log("[PROXY] tracker script status:", r.status, "bytes:", text.length);
    res.set("Content-Type", "application/javascript; charset=utf-8");
    res.status(r.status).send(text);
  } catch (e) {
    console.warn("[PROXY] tracker script unavailable:", e.message);
    res.status(502).send("// Unomi tracker unavailable");
  }
});

// context.json -- handles both GET and POST (tracker version-dependent)
async function proxyContextJson(req, res) {
  try {
    const isGet = req.method === "GET";

    // Clean passthrough -- built-in referrer rules disabled in Karaf via rule-toggle
    const bodyToSend = typeof req.body === "string" ? req.body : JSON.stringify(req.body || {});

    console.log("[PROXY] context.json", req.method, req.url, "body length:", isGet ? 0 : bodyToSend.length);

    const r = await fetch(`${UNOMI_BASE}${req.url}`, {
      method: req.method,
      headers: {
        "Content-Type": "application/json",
        ...(req.headers.cookie ? { Cookie: req.headers.cookie } : {}),
      },
      ...(isGet ? {} : { body: bodyToSend }),
    });

    const raw = r.headers.raw();
    console.log("[PROXY] context.json Unomi status:", r.status, "set-cookie:", JSON.stringify(raw["set-cookie"]));

    if (raw["set-cookie"]) {
      cleanCookies(raw["set-cookie"]).forEach(c => res.append("Set-Cookie", c));
    }

    const text = await r.text();
    res.set("Content-Type", "application/json");
    res.status(r.status).send(text);
  } catch (e) {
    console.error("[PROXY] context.json failed:", e.message);
    res.status(502).json({ error: "Unomi unavailable" });
  }
}
router.get("/context.json", proxyContextJson);
router.post("/context.json", proxyContextJson);

// eventcollector -- async event batching
router.post("/eventcollector", async (req, res) => {
  try {
    const rawBody = typeof req.body === "string" ? req.body : JSON.stringify(req.body || {});
    console.log("[PROXY] eventcollector", req.url, "body length:", rawBody.length);

    const r = await fetch(`${UNOMI_BASE}${req.url}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(req.headers.cookie ? { Cookie: req.headers.cookie } : {}),
      },
      body: rawBody,
    });

    const raw = r.headers.raw();
    if (raw["set-cookie"]) {
      cleanCookies(raw["set-cookie"]).forEach(c => res.append("Set-Cookie", c));
    }

    const text = await r.text();
    res.set("Content-Type", "application/json");
    res.status(r.status).send(text);
  } catch (e) {
    console.error("[PROXY] eventcollector failed:", e.message);
    res.status(502).json({ error: "Unomi unavailable" });
  }
});

module.exports = router;
