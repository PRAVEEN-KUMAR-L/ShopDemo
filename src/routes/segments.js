"use strict";

const express = require("express");
const { unomiRequest } = require("../unomiHelper");

const router = express.Router();

// ─── GET /api/segments ────────────────────────────────────────────────────────
router.get("/", async (req, res) => {
  try {
    const r = await unomiRequest("GET", "/cxs/segments");
    if (r.ok) return res.json(r.body);
    return res.status(r.status).json({ error: "Segments fetch failed" });
  } catch (e) {
    return res.status(502).json({ error: "Upstream error", detail: e.message });
  }
});

module.exports = router;
