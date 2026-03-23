"use strict";

/**
 * csvHelper.js
 * ──────────────────────────────────────────────────────────────────────────────
 * Lightweight CSV parser and serialiser used by importExport.js.
 * No third-party deps — only core Node.js.
 *
 * Handles:
 *  - Quoted fields (including fields with commas inside quotes)
 *  - \r\n and \n line endings
 *  - Empty / blank rows (skipped)
 *  - Boolean coercion: "true"/"false" strings stay as strings so they match
 *    what Apache Unomi stores (profile properties are string-typed).
 */

/**
 * parseCSV(raw: string) → Array<Object>
 *
 * Parses a CSV string with a header row.
 * Returns an array of plain objects keyed by header names.
 *
 * Example:
 *   Input:  "email,firstName\nalice@demo.com,Alice\n"
 *   Output: [{ email: "alice@demo.com", firstName: "Alice" }]
 */
function parseCSV(raw) {
  // Normalise line endings
  const lines = raw.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");

  // Find first non-empty line as header
  const headerLine = lines.find(l => l.trim().length > 0);
  if (!headerLine) return [];

  const headers   = splitCSVRow(headerLine);
  const dataLines = lines.slice(lines.indexOf(headerLine) + 1);

  const rows = [];
  for (const line of dataLines) {
    if (!line.trim()) continue; // skip blank lines
    const values = splitCSVRow(line);
    if (values.length === 0) continue;

    const obj = {};
    headers.forEach((h, i) => {
      const raw = (values[i] !== undefined ? values[i] : "").trim();
      // Coerce numeric strings to numbers for counter fields
      if (raw !== "" && /^\d+$/.test(raw)) {
        obj[h] = parseInt(raw, 10);
      } else {
        obj[h] = raw;
      }
    });
    rows.push(obj);
  }

  return rows;
}

/**
 * splitCSVRow(line: string) → string[]
 *
 * Splits a single CSV line respecting quoted fields.
 */
function splitCSVRow(line) {
  const result = [];
  let current  = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch   = line[i];
    const next = line[i + 1];

    if (inQuotes) {
      if (ch === '"' && next === '"') {
        // Escaped quote inside quoted field
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        result.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
  }
  result.push(current); // push the last field
  return result;
}

/**
 * toCSV(profiles: Array<Object>, fields: string[]) → string
 *
 * Serialises an array of Unomi profile objects to a CSV string.
 * Each profile's .properties object is flattened; only `fields` are included.
 *
 * Also includes a leading `profileId` column so exported profiles can be
 * identified even when re-imported into another system.
 */
function toCSV(profiles, fields) {
  const allColumns = ["profileId", ...fields];
  const header     = allColumns.map(escapeCSVField).join(",");

  const dataRows = profiles.map(profile => {
    const props = profile.properties || {};
    return allColumns.map(col => {
      if (col === "profileId") return escapeCSVField(profile.itemId || "");
      const val = props[col];
      if (val === null || val === undefined) return "";
      return escapeCSVField(String(val));
    }).join(",");
  });

  return [header, ...dataRows].join("\n");
}

/**
 * escapeCSVField(value: string) → string
 *
 * Wraps a field in double-quotes if it contains commas, quotes, or newlines.
 */
function escapeCSVField(value) {
  if (/[",\n\r]/.test(value)) {
    return '"' + value.replace(/"/g, '""') + '"';
  }
  return value;
}

module.exports = { parseCSV, toCSV, splitCSVRow };