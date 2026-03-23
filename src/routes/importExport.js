"use strict";

/**
 * importExport.js
 * ──────────────────────────────────────────────────────────────────────────────
 * Handles all profile import & export routes for ShopDemo x Apache Unomi.
 *
 * Key design decisions driven by the existing project:
 *
 *  1. EXPORT EXISTING PROFILES
 *     Uses POST /cxs/profiles/search (same pattern used in login.js alias lookup)
 *     to page through ALL profiles currently in Elasticsearch, then serialises
 *     them to CSV so nothing already stored is lost.
 *
 *  2. IMPORT + MERGE WITH EXISTING
 *     On CSV import every row is matched against an existing profile by the
 *     mergingProperty (default: email, same merge key used in login.js).
 *     - If a match is found  → properties are MERGED (counters summed,
 *       booleans OR-ed, identity fields kept from incoming row).
 *     - If no match          → profile is created fresh via the standard
 *       Unomi importConfiguration oneshot flow.
 *     This mirrors the manual merge logic already in login.js.
 *
 *  3. UNOMI IMPORT CONFIGS
 *     Full CRUD for oneshot and recurrent importConfiguration objects
 *     (backed by Apache Camel under the hood in Unomi).
 *
 *  4. UNOMI EXPORT CONFIGS
 *     Full CRUD for oneshot and recurrent exportConfiguration objects.
 *
 * Routes registered in server.js:
 *   app.use("/api/import",  importExportRouter);
 *   app.use("/api/export",  importExportRouter);
 */

const express = require("express");
const multer  = require("multer");
const path    = require("path");
const fs      = require("fs");
const os      = require("os");

const { unomiRequest }        = require("../unomiHelper");
const { SCOPE }               = require("../config");
const { parseCSV, toCSV }     = require("../csvHelper");          // new helper (see csvHelper.js)
const { mergeProfileProps }   = require("../profileMergeHelper"); // new helper (see profileMergeHelper.js)

const router = express.Router();

// ── multer: store CSV uploads in OS temp dir ──────────────────────────────────
const upload = multer({
  dest: path.join(os.tmpdir(), "shopdemo_csv_uploads"),
  fileFilter: (_req, file, cb) => {
    if (file.mimetype === "text/csv" || file.originalname.endsWith(".csv")) {
      cb(null, true);
    } else {
      cb(new Error("Only CSV files are accepted"));
    }
  },
  limits: { fileSize: 10 * 1024 * 1024 }, // 10 MB
});

// ═════════════════════════════════════════════════════════════════════════════
//  EXPORT — read ALL profiles from Elasticsearch and stream back as CSV
// ═════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/export/profiles/csv?segment=<segmentId>&fields=email,firstName,...
 *
 * Fetches every profile from Unomi (paged 200 at a time), optionally filtered
 * by a segment that already exists in the project (e.g. "high-value-customer").
 * Returns a downloadable CSV.
 *
 * Query params:
 *   segment  (optional) – one of the segment IDs defined in bootstrap.js
 *   fields   (optional) – comma-separated list of property keys to include
 *                         defaults to the full ShopDemo property set
 */
router.get("/profiles/csv", async (req, res) => {
  const segmentFilter = req.query.segment || null;
  const requestedFields = req.query.fields
    ? req.query.fields.split(",").map(f => f.trim()).filter(Boolean)
    : ["email", "firstName", "userId", "loggedInAs",
       "interestedInNorthIndian", "interestedInSouthIndian",
       "interestedInChinese",     "interestedInItalian",
       "pageViewCount", "totalCartAdds", "totalPurchases"];

  try {
    const profiles = await fetchAllProfiles(segmentFilter);

    if (profiles.length === 0) {
      return res.status(200)
        .set("Content-Type", "text/csv")
        .set("Content-Disposition", `attachment; filename="shopdemo-profiles-empty.csv"`)
        .send(requestedFields.join(",") + "\n");
    }

    const csvString = toCSV(profiles, requestedFields);
    const filename  = segmentFilter
      ? `shopdemo-${segmentFilter}-${Date.now()}.csv`
      : `shopdemo-all-profiles-${Date.now()}.csv`;

    res.set("Content-Type", "text/csv");
    res.set("Content-Disposition", `attachment; filename="${filename}"`);
    return res.send(csvString);

  } catch (e) {
    console.error("[EXPORT] CSV export failed:", e.message);
    return res.status(502).json({ error: "Export failed", detail: e.message });
  }
});

/**
 * GET /api/export/profiles/json?segment=<segmentId>
 *
 * Same as above but returns raw JSON array — useful for debugging or
 * feeding into another system without CSV round-tripping.
 */
router.get("/profiles/json", async (req, res) => {
  const segmentFilter = req.query.segment || null;
  try {
    const profiles = await fetchAllProfiles(segmentFilter);
    return res.json({ count: profiles.length, profiles });
  } catch (e) {
    return res.status(502).json({ error: "Export failed", detail: e.message });
  }
});

// ═════════════════════════════════════════════════════════════════════════════
//  IMPORT — read CSV and merge each row into existing / create new profiles
// ═════════════════════════════════════════════════════════════════════════════

/**
 * POST /api/import/profiles/csv
 * Content-Type: multipart/form-data
 * Field: csvFile  – the CSV file
 * Field: mergingProperty – which CSV column/profile property is the unique key
 *                          (default: "email", matching login.js strategy)
 * Field: overwrite – "true" | "false"  (default "true")
 *                    if true, incoming values win for scalar fields;
 *                    numeric counters are always SUMMED regardless.
 *
 * Returns a JSON summary: { imported, merged, skipped, errors[] }
 */
router.post("/profiles/csv", upload.single("csvFile"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No CSV file uploaded" });
  }

  const mergingProperty = req.body.mergingProperty || "email";
  const overwrite       = req.body.overwrite !== "false"; // default true

  let rows;
  try {
    const raw = fs.readFileSync(req.file.path, "utf8");
    rows = parseCSV(raw);
    fs.unlinkSync(req.file.path); // clean up temp file immediately
  } catch (e) {
    return res.status(400).json({ error: "CSV parse error", detail: e.message });
  }

  if (rows.length === 0) {
    return res.json({ imported: 0, merged: 0, skipped: 0, errors: [] });
  }

  const summary = { imported: 0, merged: 0, skipped: 0, errors: [] };

  for (const row of rows) {
    const mergeKey = row[mergingProperty];
    if (!mergeKey) {
      summary.skipped++;
      continue;
    }

    try {
      // ── Step 1: check if a profile already exists with this merge key ──────
      const existingRes = await unomiRequest("GET", `/cxs/profiles/${encodeURIComponent(mergeKey)}`);

      if (existingRes.ok && existingRes.body && existingRes.body.itemId) {
        // ── Existing profile found → MERGE ────────────────────────────────────
        const existing   = existingRes.body;
        const mergedProps = mergeProfileProps(existing.properties || {}, row, overwrite);

        // Must delete .version before POSTing back (same pattern as login.js)
        const toWrite = { ...existing, properties: mergedProps };
        delete toWrite.version;

        const writeRes = await unomiRequest("POST", "/cxs/profiles", toWrite);
        if (writeRes.ok) {
          summary.merged++;
          console.log(`[IMPORT] Merged into existing profile key=${mergeKey} id=${existing.itemId}`);
        } else {
          summary.errors.push({ key: mergeKey, error: `Merge write failed: ${writeRes.status}` });
        }

      } else {
        // ── No existing profile → CREATE via Unomi profile POST ───────────────
        const newProfile = {
          itemType:   "profile",
          scope:      SCOPE,
          properties: { ...row },
          segments:   [],
          scores:     {},
          consents:   {},
        };
        const createRes = await unomiRequest("POST", "/cxs/profiles", newProfile);
        if (createRes.ok) {
          // Create alias so the profile can be found by the merge key next time
          const profileId = createRes.body.itemId;
          if (profileId && mergingProperty === "email") {
            await unomiRequest(
              "POST",
              `/cxs/profiles/${profileId}/aliases/${encodeURIComponent(mergeKey)}`
            ).catch(() => {}); // alias failure is non-fatal
          }
          summary.imported++;
          console.log(`[IMPORT] Created new profile key=${mergeKey}`);
        } else {
          summary.errors.push({ key: mergeKey, error: `Create failed: ${createRes.status}` });
        }
      }

    } catch (e) {
      summary.errors.push({ key: mergeKey, error: e.message });
    }
  }

  console.log("[IMPORT] Complete:", summary);
  return res.json(summary);
});

// ═════════════════════════════════════════════════════════════════════════════
//  UNOMI IMPORT CONFIGURATION CRUD  (Camel-backed, for recurrent file polling)
// ═════════════════════════════════════════════════════════════════════════════

// GET  /api/import/configs          — list all import configs
router.get("/configs", async (req, res) => {
  const r = await unomiRequest("GET", "/cxs/importConfiguration");
  return res.status(r.ok ? 200 : r.status).json(r.body);
});

// GET  /api/import/configs/:id      — get one import config
router.get("/configs/:configId", async (req, res) => {
  const r = await unomiRequest("GET", `/cxs/importConfiguration/${req.params.configId}`);
  return res.status(r.ok ? 200 : r.status).json(r.body);
});

// POST /api/import/configs          — create/update an import config
router.post("/configs", async (req, res) => {
  const r = await unomiRequest("POST", "/cxs/importConfiguration", req.body);
  return res.status(r.ok ? 200 : r.status).json(r.body);
});

// DELETE /api/import/configs/:id    — delete an import config
router.delete("/configs/:configId", async (req, res) => {
  const r = await unomiRequest("DELETE", `/cxs/importConfiguration/${req.params.configId}`);
  return res.status(r.ok ? 200 : r.status).json(r.body);
});

// ═════════════════════════════════════════════════════════════════════════════
//  UNOMI EXPORT CONFIGURATION CRUD  (Camel-backed, for recurrent file writing)
// ═════════════════════════════════════════════════════════════════════════════

// GET  /api/export/configs          — list all export configs
router.get("/configs", async (req, res) => {
  const r = await unomiRequest("GET", "/cxs/exportConfiguration");
  return res.status(r.ok ? 200 : r.status).json(r.body);
});

// GET  /api/export/configs/:id      — get one export config
router.get("/configs/:configId", async (req, res) => {
  const r = await unomiRequest("GET", `/cxs/exportConfiguration/${req.params.configId}`);
  return res.status(r.ok ? 200 : r.status).json(r.body);
});

// POST /api/export/configs          — create/update an export config
router.post("/configs", async (req, res) => {
  const r = await unomiRequest("POST", "/cxs/exportConfiguration", req.body);
  return res.status(r.ok ? 200 : r.status).json(r.body);
});

// DELETE /api/export/configs/:id    — delete an export config
router.delete("/configs/:configId", async (req, res) => {
  const r = await unomiRequest("DELETE", `/cxs/exportConfiguration/${req.params.configId}`);
  return res.status(r.ok ? 200 : r.status).json(r.body);
});

// ═════════════════════════════════════════════════════════════════════════════
//  INTERNAL HELPERS
// ═════════════════════════════════════════════════════════════════════════════

/**
 * fetchAllProfiles(segmentFilter?)
 *
 * Pages through /cxs/profiles/search 200 at a time until all profiles
 * currently in Elasticsearch are collected.  Optionally filters by segment.
 *
 * Uses the same POST /cxs/profiles/search pattern Unomi uses internally —
 * no special admin endpoint needed, just the standard REST API already
 * authenticated via unomiRequest.
 */
async function fetchAllProfiles(segmentFilter = null) {
  const PAGE_SIZE = 200;
  let offset      = 0;
  let total       = null;
  const all       = [];

  // Build search condition
  const condition = segmentFilter
    ? {
        type: "profileSegmentCondition",
        parameterValues: { segments: [segmentFilter], matchType: "in" },
      }
    : { type: "matchAllCondition" };

  while (total === null || offset < total) {
    const body = {
      offset,
      limit:     PAGE_SIZE,
      sortby:    "properties.email:asc",
      condition,
    };

    const r = await unomiRequest("POST", "/cxs/profiles/search", body);
    if (!r.ok) {
      throw new Error(`Profile search failed with status ${r.status}`);
    }

    const data = r.body;
    total  = data.totalSize || 0;
    const list = data.list  || [];
    all.push(...list);
    offset += list.length;

    // Safety: avoid infinite loop if Unomi returns 0 items unexpectedly
    if (list.length === 0) break;
  }

  console.log(`[EXPORT] Fetched ${all.length} profiles (segment=${segmentFilter || "all"})`);
  return all;
}

module.exports = router;