"use strict";

const express = require("express");
const { unomiRequest } = require("../unomiHelper");

const router = express.Router();

// ─── GET /api/profile/:profileId ─────────────────────────────────────────────
router.get("/:profileId", async (req, res) => {
  try {
    const r = await unomiRequest("GET", `/cxs/profiles/${req.params.profileId}`);
    if (r.ok) {
      const p = r.body, props = p.properties || {};
      return res.json({
        _identity: { profileId: p.itemId, email: props.email || null, name: props.loggedInAs || null, userId: props.userId || null, segments: p.segments || [], scores: p.scores || {} },
        ...p,
      });
    }
    return res.status(r.status).json({ error: "Profile fetch failed", detail: r.body });
  } catch (e) {
    return res.status(502).json({ error: "Upstream error", detail: e.message });
  }
});

module.exports = router;
