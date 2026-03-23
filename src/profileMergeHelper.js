"use strict";

/**
 * profileMergeHelper.js
 * ──────────────────────────────────────────────────────────────────────────────
 * Merges an incoming CSV row (plain key/value object) into an existing Unomi
 * profile's properties object.
 *
 * Merge rules (mirror the manual merge in login.js):
 *
 *  NUMERIC COUNTERS  — always SUMMED so history is never lost.
 *    pageViewCount, totalCartAdds, totalPurchases
 *
 *  BOOLEAN INTERESTS — OR-ed (once true, stays true).
 *    interestedInNorthIndian, interestedInSouthIndian,
 *    interestedInChinese,     interestedInItalian
 *
 *  IDENTITY FIELDS   — incoming row wins when overwrite=true (default),
 *    email, firstName, loggedInAs, userId    otherwise existing value kept.
 *
 *  FIRST-SEEN DATE   — keeps the EARLIER of the two values.
 *    firstVisit
 *
 *  ALL OTHER FIELDS  — overwrite=true  → incoming row wins
 *                      overwrite=false → existing value kept
 */

// Properties whose values should be SUMMED across old + new
const COUNTER_FIELDS = new Set([
  "pageViewCount",
  "totalCartAdds",
  "totalPurchases",
]);

// Properties that are "true"/"false" strings and should be OR-ed
const BOOLEAN_INTEREST_FIELDS = new Set([
  "interestedInNorthIndian",
  "interestedInSouthIndian",
  "interestedInChinese",
  "interestedInItalian",
]);

/**
 * mergeProfileProps(existingProps, incomingRow, overwrite = true)
 *
 * @param {Object}  existingProps  - profile.properties from Unomi (already stored in ES)
 * @param {Object}  incomingRow    - one parsed CSV row from the import file
 * @param {boolean} overwrite      - if true, scalar identity fields from CSV win;
 *                                   if false, existing values are preserved
 * @returns {Object}               - merged properties object ready to POST back to Unomi
 */
function mergeProfileProps(existingProps, incomingRow, overwrite = true) {
  // Start from a copy of existing so we don't mutate the original
  const merged = { ...existingProps };

  for (const [key, incomingVal] of Object.entries(incomingRow)) {
    // Skip undefined / empty incoming values — don't overwrite something with nothing
    if (incomingVal === undefined || incomingVal === null || incomingVal === "") continue;

    if (COUNTER_FIELDS.has(key)) {
      // ── NUMERIC: sum old + new ─────────────────────────────────────────────
      const oldNum = parseInt(existingProps[key], 10) || 0;
      const newNum = parseInt(incomingVal, 10)        || 0;
      merged[key]  = oldNum + newNum;

    } else if (BOOLEAN_INTEREST_FIELDS.has(key)) {
      // ── BOOLEAN INTEREST: OR — once true, stays true ──────────────────────
      const oldTrue = existingProps[key] === "true" || existingProps[key] === true;
      const newTrue = incomingVal === "true" || incomingVal === true;
      merged[key]   = (oldTrue || newTrue) ? "true" : "false";

    } else if (key === "firstVisit") {
      // ── DATE: keep the EARLIER of the two ─────────────────────────────────
      const existingDate = existingProps.firstVisit ? new Date(existingProps.firstVisit) : null;
      const incomingDate = new Date(incomingVal);
      if (!existingDate || incomingDate < existingDate) {
        merged.firstVisit = incomingVal;
      }
      // else: existing date is earlier — keep it (no-op since we started from existingProps)

    } else {
      // ── ALL OTHER FIELDS: respect overwrite flag ───────────────────────────
      if (overwrite) {
        merged[key] = incomingVal;
      } else {
        // Keep existing value if present; only fill in blanks from CSV
        if (existingProps[key] === undefined || existingProps[key] === null || existingProps[key] === "") {
          merged[key] = incomingVal;
        }
      }
    }
  }

  return merged;
}

/**
 * buildMergeReport(existingProps, incomingRow, merged)
 *
 * Returns a human-readable diff for logging / API response.
 * Useful during debugging to understand what actually changed.
 */
function buildMergeReport(existingProps, incomingRow, merged) {
  const changes = [];
  for (const key of new Set([...Object.keys(incomingRow), ...Object.keys(existingProps)])) {
    const before = existingProps[key];
    const after  = merged[key];
    if (String(before) !== String(after)) {
      changes.push({ field: key, before: before ?? "(none)", after: after ?? "(none)" });
    }
  }
  return changes;
}

module.exports = { mergeProfileProps, buildMergeReport };