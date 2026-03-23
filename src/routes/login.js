"use strict";

const express = require("express");
const { unomiRequest } = require("../unomiHelper");
const { DEMO_USERS } = require("../config");

const router = express.Router();

// ─── POST /api/login ──────────────────────────────────────────────────────────
router.post("/", async (req, res) => {
  const { email } = req.body;

  // Prefer the profile ID the browser JS passed explicitly in the POST body.
  // Fall back to the cookie Express parsed from the request headers.
  const anonymousProfileId =
    (req.body.anonymousProfileId && String(req.body.anonymousProfileId).trim()) ||
    (req.cookies && req.cookies["context-profile-id"]) ||
    null;

  console.log(`[LOGIN] email=${email}  anonymousProfileId=${anonymousProfileId || "NULL"}`);

  const user = DEMO_USERS.find(u => u.email === email.toLowerCase().trim());
  if (!user) {
    return res.status(401).json({ success: false, error: "User not found. Try alice@demo.com, bob@demo.com, or carol@demo.com." });
  }

  if (!anonymousProfileId) {
    console.warn("[LOGIN] No profile ID -- login without Unomi profile link.");
    return res.json({ success: true, user: { name: user.name, email: user.email }, masterProfileId: null });
  }

  // Give ES time to index the profile before we read/write it
  await new Promise(r => setTimeout(r, 600));

  // ── Merge-first login strategy ────────────────────────────────────────────────
  //
  // Problem with simple re-aliasing:
  //   Every logout expires the cookie -> next visit creates a new anonymous UUID ->
  //   login re-aliases alice to the new empty profile -> all previous history lost.
  //
  // Fix: on login, check if the user already has an existing profile via alias.
  //   YES -> merge the new anonymous profile (slave) INTO the existing one (master)
  //          so all new session activity is folded into historical profile.
  //          Return masterProfileId to browser so it updates its cookie immediately.
  //   NO  -> this is the user's first ever login. Create the alias pointing to the
  //          current anonymous profile, then stamp identity properties onto it.

  let masterProfileId = null;

  // Step 1: Check if user already has a profile via existing alias
  try {
    const existingRes = await unomiRequest("GET", `/cxs/profiles/${encodeURIComponent(user.email)}`);
    if (existingRes.ok && existingRes.body && existingRes.body.itemId) {
      masterProfileId = existingRes.body.itemId;
      console.log(`[LOGIN] Existing profile found via alias  master=${masterProfileId}`);
    }
  } catch (_) { }

  if (masterProfileId && masterProfileId !== anonymousProfileId) {
    // Step 2a: Returning user -- manually merge anonymous session data into master.
    // Unomi 2.6 has no /merge/ REST endpoint -- we do it ourselves:
    // read both profiles, combine numeric counters + boolean interests, write master, delete slave.
    try {
      const [masterRes, slaveRes] = await Promise.all([
        unomiRequest("GET", `/cxs/profiles/${masterProfileId}`),
        unomiRequest("GET", `/cxs/profiles/${anonymousProfileId}`),
      ]);

      if (masterRes.ok && slaveRes.ok) {
        const master = masterRes.body;
        const slave  = slaveRes.body;
        const mp = master.properties || {};
        const sp = slave.properties  || {};

        const merged = {
          pageViewCount:           (parseInt(mp.pageViewCount)  || 0) + (parseInt(sp.pageViewCount)  || 0),
          totalCartAdds:           (parseInt(mp.totalCartAdds)  || 0) + (parseInt(sp.totalCartAdds)  || 0),
          totalPurchases:          (parseInt(mp.totalPurchases) || 0) + (parseInt(sp.totalPurchases) || 0),
          interestedInElectronics: (mp.interestedInElectronics === "true" || sp.interestedInElectronics === "true") ? "true" : undefined,
          interestedInFashion:     (mp.interestedInFashion     === "true" || sp.interestedInFashion     === "true") ? "true" : undefined,
          firstVisit: mp.firstVisit && sp.firstVisit
            ? (new Date(mp.firstVisit) < new Date(sp.firstVisit) ? mp.firstVisit : sp.firstVisit)
            : (mp.firstVisit || sp.firstVisit),
          email:      user.email,
          loggedInAs: user.name,
          userId:     user.userId,
        };
        // Remove undefined keys
        Object.keys(merged).forEach(k => merged[k] === undefined && delete merged[k]);

        delete master.version;
        const writeRes = await unomiRequest("POST", "/cxs/profiles", {
          ...master,
          properties: { ...mp, ...merged },
        });

        if (writeRes.ok) {
          console.log(`[LOGIN] Manual merge OK  slave=${anonymousProfileId} -> master=${masterProfileId}`);
          await unomiRequest("DELETE", `/cxs/profiles/${anonymousProfileId}`);
          console.log(`[LOGIN] Deleted anonymous profile  id=${anonymousProfileId}`);
        } else {
          console.warn(`[LOGIN] Merge write warning  status=${writeRes.status}`, JSON.stringify(writeRes.body));
        }
      }
    } catch (e) { console.error("[LOGIN] Manual merge threw:", e.message); }

  } else {
    // Step 2b: First login ever -- create alias and stamp identity
    masterProfileId = anonymousProfileId;

    try {
      const aliasRes = await unomiRequest(
        "POST",
        `/cxs/profiles/${anonymousProfileId}/aliases/${encodeURIComponent(user.email)}`
      );
      if (aliasRes.ok || aliasRes.status === 409) {
        console.log(`[LOGIN] Alias created  ${user.email} -> ${anonymousProfileId}`);
      } else {
        console.warn(`[LOGIN] Alias warning  status=${aliasRes.status}`, JSON.stringify(aliasRes.body));
      }
    } catch (e) { console.error("[LOGIN] Alias threw:", e.message); }

    try {
      const profileRes = await unomiRequest("GET", `/cxs/profiles/${anonymousProfileId}`);
      if (!profileRes.ok) {
        console.warn(`[LOGIN] Profile GET failed  status=${profileRes.status}`);
      } else {
        const existing = profileRes.body;
        console.log(`[LOGIN] Profile GET OK  version=${existing.version}  props:`, JSON.stringify(existing.properties));

        // Must delete version before writing back.
        // ES optimistic concurrency control silently drops the write if we POST
        // back the same version number -- leaving email/name never written.
        delete existing.version;

        const writeRes = await unomiRequest("POST", "/cxs/profiles", {
          ...existing,
          properties: {
            ...(existing.properties || {}),
            email:      user.email,
            loggedInAs: user.name,
            userId:     user.userId,
          },
        });

        if (writeRes.ok) {
          console.log(`[LOGIN] Profile stamped  email=${user.email}  id=${anonymousProfileId}`);
        } else {
          console.warn(`[LOGIN] Profile POST warning  status=${writeRes.status}`, JSON.stringify(writeRes.body));
        }
      }
    } catch (e) { console.error("[LOGIN] Profile stamp threw:", e.message); }
  }

  // masterProfileId is returned so the browser can update its cookie immediately,
  // ensuring the next loadContext() uses the right (master) profile UUID.
  return res.json({ success: true, user: { name: user.name, email: user.email }, masterProfileId });
});

module.exports = router;
