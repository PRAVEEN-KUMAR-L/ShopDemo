"use strict";

/**
 * scripts/registerSyncConfig.js
 *
 * Run ONCE after deployment to register the recurrent import config
 * in Unomi. After this, Unomi's Camel router watches /tmp/shopdemo_import/
 * for CSV files automatically — no further setup needed.
 *
 * Run with:
 *   npm run setup:sync
 */

const os   = require("os");
const path = require("path");
const { unomiRequest } = require("../src/unomiHelper");

// Must match the EXPORT_DIR in nightlySync.js exactly.
// os.tmpdir() resolves to the correct temp folder per OS:
//   Windows → C:\Users\<user>\AppData\Local\Temp
//   Linux   → /tmp
//   Mac     → /var/folders/...
const SYNC_DIR = process.env.SYNC_DIR || path.join(os.tmpdir(), "shopdemo_import");

// Camel requires forward slashes even on Windows, and needs a trailing slash.
const CAMEL_SOURCE_PATH = SYNC_DIR.replace(/\\/g, "/").replace(/\/?$/, "/");

async function register() {
  console.log("[SETUP] Registering Unomi recurrent import config...");
  console.log("[SETUP] Camel will watch:", CAMEL_SOURCE_PATH);

  const r = await unomiRequest("POST", "/cxs/importConfiguration", {
    itemId:                    "shopNightlySync",
    itemType:                  "importConfig",
    name:                      "ShopDemo Nightly Sync",
    configType:                "recurrent",
    columnSeparator:           ",",
    lineSeparator:             "\\n",
    hasHeader:                 true,
    hasDeleteColumn:           false,
    mergingProperty:           "email",
    overwriteExistingProfiles: true,
    active:                    true,
    properties: {
      // Watches this folder every 30 seconds for new .csv files.
      // Processed files are moved to .done/ automatically by Camel.
      source: `file:///${CAMEL_SOURCE_PATH}?include=.*.csv&move=.done&consumer.delay=30s`,
      mapping: {
        "email":                   0,
        "firstName":               1,
        "userId":                  2,
        "interestedInNorthIndian": 3,
        "interestedInSouthIndian": 4,
        "interestedInChinese":     5,
        "interestedInItalian":     6,
        "pageViewCount":           7,
        "totalCartAdds":           8,
        "totalPurchases":          9
      }
    }
  });

  if (r.ok || r.status === 409) {
    console.log("[SETUP] Recurrent import config registered. Unomi is now watching /tmp/shopdemo_import/");
  } else {
    console.error("[SETUP] Failed:", r.status, JSON.stringify(r.body));
    process.exit(1);
  }
}

register().catch(e => {
  console.error("[SETUP] Error:", e.message);
  process.exit(1);
});