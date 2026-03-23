"use strict";

const fetch = require("node-fetch");
const { UNOMI_BASE, UNOMI_AUTH } = require("./config");

// ─── Unomi admin helper ───────────────────────────────────────────────────────
async function unomiRequest(method, urlPath, body) {
  const opts = {
    method,
    headers: {
      Authorization: UNOMI_AUTH,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
  };
  if (body !== undefined && body !== null) opts.body = JSON.stringify(body);
  const res = await fetch(`${UNOMI_BASE}${urlPath}`, opts);
  let responseBody;
  try { responseBody = await res.json(); }
  catch { responseBody = {}; }
  return { ok: res.ok, status: res.status, body: responseBody };
}

// ─── Cookie cleaner ───────────────────────────────────────────────────────────
// Unomi sets context-profile-id with Secure + HttpOnly + SameSite=None +
// Domain=localhost.  On plain HTTP localhost the browser silently drops Secure
// cookies; HttpOnly blocks JS from reading them; Domain=localhost is rejected
// by Chrome/Firefox.  Strip all four so the cookie lands and is JS-readable.
function cleanCookies(rawArray) {
  return rawArray.map(c =>
    c.replace(/;\s*Secure/gi, "")
      .replace(/;\s*HttpOnly/gi, "")
      .replace(/;\s*SameSite=\w+/gi, "")
      .replace(/;\s*Domain=[^;,]+/gi, "")
  );
}

module.exports = { unomiRequest, cleanCookies };
