"use strict";

/**
 * scripts/nightlySync.js
 *
 * Pulls all current profiles from Elasticsearch via the existing
 * /api/export/profiles/csv endpoint, then drops the CSV into the
 * folder that Unomi's recurrent import config is already watching.
 *
 * Unomi's Camel router picks it up within 30 seconds and merges
 * every row back into Elasticsearch using the merge rules in
 * profileMergeHelper.js (counters summed, interests OR-ed).
 *
 * No external database required — Elasticsearch IS the database.
 *
 * Schedule via cron:
 *   0 2 * * * node /path/to/ShopDemo/scripts/nightlySync.js
 *
 * Or add to package.json scripts and run manually:
 *   npm run sync
 */

const fs    = require("fs");
const path  = require("path");
const os    = require("os");
const fetch = require("node-fetch");

const SHOPDEMO_BASE = process.env.SHOPDEMO_BASE || "http://localhost:3000";

// os.tmpdir() returns the correct temp folder for the current OS:
// Windows → C:\Users\<user>\AppData\Local\Temp
// Linux   → /tmp
// Mac     → /var/folders/...
const EXPORT_DIR = process.env.SYNC_DIR || path.join(os.tmpdir(), "shopdemo_import");

async function run() {
  console.log("[NIGHTLY SYNC] Starting at", new Date().toISOString());
  console.log("[NIGHTLY SYNC] Platform:", process.platform);
  console.log("[NIGHTLY SYNC] Export dir:", EXPORT_DIR);

  // ── Step 1: Export all current profiles from Elasticsearch ───────────────
  const res = await fetch(`${SHOPDEMO_BASE}/api/export/profiles/csv`);

  if (!res.ok) {
    throw new Error(`Export endpoint returned ${res.status}: ${res.statusText}`);
  }

  const csv = await res.text();

  if (!csv || csv.trim().split("\n").length <= 1) {
    console.log("[NIGHTLY SYNC] No profiles to sync. Exiting.");
    return;
  }

  const rowCount = csv.trim().split("\n").length - 1; // subtract header
  console.log(`[NIGHTLY SYNC] Fetched ${rowCount} profiles from Elasticsearch`);

  // ── Step 2: Write to the folder Unomi's Camel watcher is polling ─────────
  fs.mkdirSync(EXPORT_DIR, { recursive: true });

  const filename = path.join(EXPORT_DIR, `sync-${Date.now()}.csv`);
  fs.writeFileSync(filename, csv, "utf8");

  console.log(`[NIGHTLY SYNC] Wrote to ${filename}`);
  console.log("[NIGHTLY SYNC] Unomi will pick this up and merge within 30 seconds");
  console.log("[NIGHTLY SYNC] Processed files will be moved to", path.join(EXPORT_DIR, ".done"));
}

run().catch(e => {
  console.error("[NIGHTLY SYNC] Failed:", e.message);
  process.exit(1);
});