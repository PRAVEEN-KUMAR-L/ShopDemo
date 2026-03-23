"use strict";

const express      = require("express");
const cookieParser = require("cookie-parser");
const cors         = require("cors");
const path         = require("path");

const { PORT }           = require("./src/config");
const { bootstrapUnomi } = require("./src/bootstrap");
const proxyRouter        = require("./src/proxy");
const loginRouter        = require("./src/routes/login");
const profileRouter      = require("./src/routes/profile");
const segmentsRouter     = require("./src/routes/segments");
<<<<<<< HEAD
const importExportRouter = require("./src/routes/importExport");
=======
>>>>>>> 359b767976320fae1bd23039c03debd9813f0065

const app = express();

// ─── Middleware ───────────────────────────────────────────────────────────────
// express.text MUST be registered before express.json for the proxy paths.
// The Unomi tracker sends its body as plain text; if express.json runs first
// it silently discards the body, the proxy forwards nothing, and Unomi 400s.
app.use("/context.json",   express.text({ type: "*/*" }));
app.use("/eventcollector", express.text({ type: "*/*" }));

app.use(cors());
app.use(express.json());
app.use(cookieParser());
app.use(express.static(path.join(__dirname, "public")));

// ─── Request logger (debug) ───────────────────────────────────────────────────
app.use(function (req, _res, next) {
  console.log("[REQUEST]", req.method, req.url);
  next();
});

// ─── Routes ───────────────────────────────────────────────────────────────────
app.use("/",            proxyRouter);
app.use("/api/login",   loginRouter);
app.use("/api/profile", profileRouter);
app.use("/api/segments", segmentsRouter);
<<<<<<< HEAD
app.use("/api/import",   importExportRouter);
app.use("/api/export",   importExportRouter);

=======
>>>>>>> 359b767976320fae1bd23039c03debd9813f0065

// SPA fallback -- never serve index.html for asset requests
app.get("*", (req, res) => {
  if (/\.\w+$/.test(req.path)) return res.status(404).send("Not found");
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ─── Startup ──────────────────────────────────────────────────────────────────
(async () => {
  try { await bootstrapUnomi(); }
  catch (e) { console.error("[BOOTSTRAP] Unexpected error:", e.message); }
  app.listen(PORT, () => console.log(`\nShopDemo running at http://localhost:${PORT}\n`));
})();
