const express    = require("express");
const cors       = require("cors");
const fetch      = require("node-fetch");
const fs         = require("fs");

const app        = express();
const PORT       = process.env.PORT || 10000;
const AT_TOKEN   = process.env.AT_TOKEN;
const AT_BASE    = process.env.AT_BASE    || "appdD2UGbFIfzkj7q";
const AT_TABLE   = process.env.AT_TABLE   || "tbltj1I38yoAh2HOF";
const SLACK_TOKEN= process.env.SLACK_TOKEN;

app.use(cors());
app.use(express.json());

// ── Helpers ──────────────────────────────────────────────────────
const PERF_LOG = "./performance-log.json";
const SNAPSHOT = "./status-snapshot.json";

function readJSON(path, fallback) {
  try { return JSON.parse(fs.readFileSync(path, "utf8")); }
  catch { return fallback; }
}
function writeJSON(path, data) {
  fs.writeFileSync(path, JSON.stringify(data, null, 2));
}

const PREV_STATUS_ROLES = {
  "Ready for Copy":                      { role:"copywriter", urgencyField:"copyDeadline" },
  "Copywriting in Progress":             { role:"copywriter", urgencyField:"copyDeadline" },
  "Copywriting Complete - Ready for QA": { role:"copywriter", urgencyField:"copyDeadline" },
  "Ready For Design":                    { role:"designer",   urgencyField:"designDeadline" },
  "Design in Progress":                  { role:"designer",   urgencyField:"designDeadline" },
  "Design Complete - Ready for QA":      { role:"designer",   urgencyField:"designDeadline" },
  "Client Review":                       { role:"manager",    urgencyField:"dueDate" },
  "Upload":                              { role:"uploader",   urgencyField:"uploadDeadline" },
  "Schedule":                            { role:"manager",    urgencyField:"sendDate" },
};

function updatePerformanceLog(deliverables) {
  const snapshot = readJSON(SNAPSHOT, {});
  const perfLog  = readJSON(PERF_LOG, []);
  const newSnap  = {};

  deliverables.forEach(d => {
    const prev = snapshot[d.id];
    newSnap[d.id] = {
      status: d.status, statusUpdated: d.statusUpdated,
      name: d.name, client: d.client,
      copyDeadline: d.copyDeadline, designDeadline: d.designDeadline,
      uploadDeadline: d.uploadDeadline, dueDate: d.dueDate, sendDate: d.sendDate,
      manager: d.manager, copywriter: d.copywriter, designer: d.designer, uploader: d.uploader,
    };

    if (!prev || prev.status === d.status || !d.statusUpdated) return;

    const prevSc = PREV_STATUS_ROLES[prev.status];
    if (!prevSc) return;

    const deadline = prev[prevSc.urgencyField] || d[prevSc.urgencyField];
    if (!deadline) return;

    const completedAt  = new Date(d.statusUpdated);
    const deadlineDate = new Date(deadline);
    deadlineDate.setHours(23, 59, 59, 999);
    const onTime = completedAt <= deadlineDate;

    let people = [];
    if (prevSc.role === "copywriter") people = prev.copywriter || d.copywriter || [];
    if (prevSc.role === "designer")   people = prev.designer   || d.designer   || [];
    if (prevSc.role === "manager")    people = prev.manager    || d.manager    || [];
    if (prevSc.role === "uploader")   people = [prev.uploader  || d.uploader].filter(Boolean);

    people.forEach(p => {
      perfLog.push({
        personId: p.id, personName: p.name, role: prevSc.role,
        taskId: d.id, taskName: d.name, client: d.client,
        fromStatus: prev.status, toStatus: d.status,
        deadline, completedAt: d.statusUpdated, onTime,
        month: completedAt.toISOString().slice(0, 7),
      });
    });
  });

  writeJSON(SNAPSHOT, newSnap);
  writeJSON(PERF_LOG, perfLog);
  console.log(`Performance log updated — ${perfLog.length} entries`);
}

// ── GET /health ───────────────────────────────────────────────────
app.get("/health", (req, res) => res.json({ ok: true, version: "2.0.0" }));

// ── GET /deliverables ─────────────────────────────────────────────
app.get("/deliverables", async (req, res) => {
  try {
    const getDate = v => v ? (Array.isArray(v) ? v[0] : v) : null;
    const getPpl  = v => (v || []).map(p => ({ id: p.id, name: p.name }));
    let allRecords = [], offset = null;
    do {
      const url = new URL(`https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`);
      url.searchParams.set("pageSize", "100");
      url.searchParams.set("filterByFormula", `NOT({Status} = "Complete")`);
      if (offset) url.searchParams.set("offset", offset);
      const atRes = await fetch(url.toString(), { headers: { Authorization: `Bearer ${AT_TOKEN}` } });
      if (!atRes.ok) { const e = await atRes.json().catch(()=>({})); return res.status(atRes.status).json({ error: e?.error?.message || `AT ${atRes.status}` }); }
      const data = await atRes.json();
      (data.records || []).forEach(r => {
        const f = r.fields;
        const upArr = getPpl(f["Uploader"]);
        const client = Array.isArray(f["Client Name"]) ? f["Client Name"][0] : f["Client Name"] || "";
        if (!client) return;
        allRecords.push({
          id:             r.id,
          name:           f["Deliverable Name"] || "",
          createdTime:    r.createdTime || null,
          statusUpdated:  f["Status Updated"] || null,
          status:         f["Status"] || "",
          client,
          sendDate:       getDate(f["Send Date/Activation Date"]),
          copyDeadline:   getDate(f["\uD83D\uDCC5Copy Deadline"] || f["Copy Deadline"]),
          designDeadline: getDate(f["\uD83C\uDFA8Design Deadline"] || f["Design Deadline"]),
          uploadDeadline: getDate(f["\uD83D\uDCE4Klaviyo Upload Deadline"] || f["Klaviyo Upload Deadline"]),
          dueDate:        getDate(f["Due Date"]),
          manager:        getPpl(f["Manager"]),
          copywriter:     getPpl(f["Copywriter"]),
          designer:       getPpl(f["Designer"]),
          uploader:       upArr.length ? upArr[0] : null,
        });
      });
      offset = data.offset || null;
    } while (offset);
    res.json({ records: allRecords, count: allRecords.length });
  } catch (err) {
    console.error("/deliverables error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /send-dm ─────────────────────────────────────────────────
app.post("/send-dm", async (req, res) => {
  try {
    const { slackUserId, text, blocks, attachments } = req.body;
    const body = { channel: slackUserId, text: text || " " };
    if (blocks?.length)      body.blocks      = blocks;
    if (attachments?.length) body.attachments = attachments;
    const slackRes = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify(body),
    });
    const data = await slackRes.json();
    if (!data.ok) return res.status(400).json({ error: data.error });
    res.json({ ok: true, ts: data.ts });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /send-channel ────────────────────────────────────────────
app.post("/send-channel", async (req, res) => {
  try {
    const { channelId, text, blocks, attachments } = req.body;
    const body = { channel: channelId, text: text || " " };
    if (blocks?.length)      body.blocks      = blocks;
    if (attachments?.length) body.attachments = attachments;
    const slackRes = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify(body),
    });
    const data = await slackRes.json();
    if (!data.ok) return res.status(400).json({ error: data.error });
    res.json({ ok: true, ts: data.ts });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /comments/:recordId ───────────────────────────────────────
app.get("/comments/:recordId", async (req, res) => {
  try {
    const atRes = await fetch(
      `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}/${req.params.recordId}/comments`,
      { headers: { Authorization: `Bearer ${AT_TOKEN}` } }
    );
    if (!atRes.ok) { const e = await atRes.json().catch(()=>({})); return res.status(atRes.status).json({ error: e?.error?.message }); }
    const data = await atRes.json();
    res.json({ comments: data.comments || [] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /performance ──────────────────────────────────────────────
app.get("/performance", (req, res) => {
  res.json({ entries: readJSON(PERF_LOG, []) });
});

// ── POST /snapshot ────────────────────────────────────────────────
app.post("/snapshot", async (req, res) => {
  try {
    const getDate = v => v ? (Array.isArray(v) ? v[0] : v) : null;
    const getPpl  = v => (v || []).map(p => ({ id: p.id, name: p.name }));
    let allRecords = [], offset = null;
    do {
      const url = new URL(`https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`);
      url.searchParams.set("pageSize", "100");
      url.searchParams.set("filterByFormula", `NOT({Status} = "Complete")`);
      if (offset) url.searchParams.set("offset", offset);
      const atRes = await fetch(url.toString(), { headers: { Authorization: `Bearer ${AT_TOKEN}` } });
      const data = await atRes.json();
      (data.records || []).forEach(r => {
        const f = r.fields;
        const upArr = getPpl(f["Uploader"]);
        allRecords.push({
          id: r.id, name: f["Deliverable Name"] || "", status: f["Status"] || "",
          statusUpdated:  f["Status Updated"] || null,
          copyDeadline:   getDate(f["Copy Deadline"]),
          designDeadline: getDate(f["Design Deadline"]),
          uploadDeadline: getDate(f["Klaviyo Upload Deadline"]),
          dueDate:        getDate(f["Due Date"]),
          sendDate:       getDate(f["Send Date/Activation Date"]),
          manager:        getPpl(f["Manager"]),
          copywriter:     getPpl(f["Copywriter"]),
          designer:       getPpl(f["Designer"]),
          uploader:       upArr.length ? upArr[0] : null,
          client:         Array.isArray(f["Client Name"]) ? f["Client Name"][0] : f["Client Name"] || "",
        });
      });
      offset = data.offset || null;
    } while (offset);
    updatePerformanceLog(allRecords);
    res.json({ ok: true, records: allRecords.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => console.log(`Insendra server running on port ${PORT}`));
