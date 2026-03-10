const express    = require("express");
const path       = require("path");
const cors       = require("cors");
const fs         = require("fs");

const app        = express();
const PORT       = process.env.PORT || 10000;
const AT_TOKEN   = process.env.AT_TOKEN;
const AT_BASE    = process.env.AT_BASE    || "appdD2UGbFIfzkj7q";
const AT_TABLE   = process.env.AT_TABLE   || "tbltj1I38yoAh2HOF";
const SLACK_TOKEN     = process.env.SLACK_TOKEN;
const ANTHROPIC_KEY   = process.env.ANTHROPIC_KEY;
const AT_TEAM_TABLE   = "tblVV58Gw8yq70NF0";
const AT_CLIENT_TABLE = "tblCp6L6Ccpg9icak";

app.use(cors());
app.use(express.json());

// ── Helpers ──────────────────────────────────────────────────────
const PERF_LOG = "./performance-log.json";
const SNAPSHOT = "./status-snapshot.json";

// ── Dynamic config (refreshed every 30 mins) ─────────────────────
let dynamicTeam    = null; // { [atUserId]: { name, slackId, role } }
let dynamicClients = null; // { [clientName]: { slackChannelId, prefix } }
let configLoadedAt = 0;
const CONFIG_TTL   = 30 * 60 * 1000;

async function loadDynamicConfig() {
  try {
    // ── Fetch team ──────────────────────────────────────────────
    const teamRes  = await fetch(
      `https://api.airtable.com/v0/${AT_BASE}/${AT_TEAM_TABLE}?pageSize=100`,
      { headers: { Authorization: `Bearer ${AT_TOKEN}` } }
    );
    const teamData = await teamRes.json();
    const team = {};
    (teamData.records || []).forEach(r => {
      const f      = r.fields;
      const atId   = f["Airtable User ID"];
      const slackId = f["Slack ID"];
      const role   = (f["Role"] || "").toLowerCase();
      const name   = f["Name"] || "";
      if (atId && slackId) team[atId] = { name, slackId, role };
    });

    // ── Fetch active clients ────────────────────────────────────
    const clientRes  = await fetch(
      `https://api.airtable.com/v0/${AT_BASE}/${AT_CLIENT_TABLE}?pageSize=100`,
      { headers: { Authorization: `Bearer ${AT_TOKEN}` } }
    );
    const clientData = await clientRes.json();
    const clients = {};
    (clientData.records || []).forEach(r => {
      const f      = r.fields;
      const name   = f["Client Name"];
      const status = Array.isArray(f["Relationship Status"])
        ? f["Relationship Status"].map(s => typeof s === "object" ? s.name : s)
        : [];
      if (!name || !status.includes("Active")) return;
      const slackChannelId = f["Slack Channel ID"] || null;
      const prefix         = (f["Task Prefix"] || "").toUpperCase();
      clients[name] = { slackChannelId, prefix };
    });

    dynamicTeam    = team;
    dynamicClients = clients;
    configLoadedAt = Date.now();
    console.log(`[config] Loaded ${Object.keys(team).length} team members, ${Object.keys(clients).length} active clients`);
  } catch (err) {
    console.error("[config] Failed to load dynamic config:", err.message);
  }
}

async function getTeam() {
  if (!dynamicTeam || Date.now() - configLoadedAt > CONFIG_TTL) await loadDynamicConfig();
  return dynamicTeam || {};
}

async function getClients() {
  if (!dynamicClients || Date.now() - configLoadedAt > CONFIG_TTL) await loadDynamicConfig();
  return dynamicClients || {};
}

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
    const getPpl  = v => { if(!v)return []; const arr=Array.isArray(v)?v:[v]; return arr.map(p=>typeof p==='object'?{id:p.id||'',name:p.name||''}:{id:p,name:p}); };
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
        // Client Name is a linked record — try lookup fields for the display name
        const clientRaw = f["Client"] || f["Client Name (from Clients)"] || f["Client Name (from Client)"] || f["Account Name"] || f["Client Name"] || "";
        const client = Array.isArray(clientRaw) ? clientRaw[0] : clientRaw || "";
        if (!client) return;
        allRecords.push({
          id:             r.id,
          name:           f["Deliverable Name"] || "",
          createdTime:    r.createdTime || null,
          statusUpdated:  f["Status Updated"] || null,
          status:         f["Status"] || "",
          client,
          sendDate:       getDate(f["Send Date/Activation Date"]),
          copyDeadline:   getDate(Object.entries(f).find(([k])=>k.includes("Copy Deadline"))?.[1]),
          designDeadline: getDate(Object.entries(f).find(([k])=>k.includes("Design Deadline"))?.[1]),
          uploadDeadline: getDate(Object.entries(f).find(([k])=>k.includes("Upload Deadline")||k.includes("Klaviyo Upload"))?.[1]),
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
    console.error("/deliverables error:", err.message, err.stack);
    res.status(500).json({ error: err.message, stack: err.stack });
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
    const getPpl  = v => { if(!v)return []; const arr=Array.isArray(v)?v:[v]; return arr.map(p=>typeof p==='object'?{id:p.id||'',name:p.name||''}:{id:p,name:p}); };
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

// ── POST /backfill ────────────────────────────────────────────────
app.post("/backfill", async (req, res) => {
  try {
    const { since = "2026-01-01" } = req.body;
    const sinceDate = new Date(since);
    const getDate = v => v ? (Array.isArray(v) ? v[0] : v) : null;
    const getPpl  = v => { if(!v)return []; const arr=Array.isArray(v)?v:[v]; return arr.map(p=>typeof p==="object"?{id:p.id||"",name:p.name||""}:{id:p,name:p}); };

    let allRecords = [], offset = null;
    do {
      const url = new URL(`https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`);
      url.searchParams.set("pageSize", "100");
      url.searchParams.set("filterByFormula",
        `AND({Status} = "Complete", IS_AFTER({Send Date/Activation Date}, "${since}"))`
      );
      if (offset) url.searchParams.set("offset", offset);
      const atRes = await fetch(url.toString(), { headers: { Authorization: `Bearer ${AT_TOKEN}` } });
      const data = await atRes.json();
      if (data.error) return res.status(400).json({ error: data.error });
      (data.records || []).forEach(r => {
        const f = r.fields;
        const upArr = getPpl(f["Uploader"]);
        allRecords.push({
          id:             r.id,
          name:           f["Deliverable Name"] || "",
          status:         "Complete",
          statusUpdated:  f["Status Updated"] || r.createdTime || null,
          createdTime:    r.createdTime,
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

    const filtered = allRecords.filter(d => d.sendDate && new Date(d.sendDate) >= sinceDate);
    const perfLog = readJSON(PERF_LOG, []);
    const existingIds = new Set(perfLog.map(e => e.taskId + e.role));
    let added = 0;

    filtered.forEach(d => {
      const roles = [
        { role:"copywriter", people: d.copywriter,                    deadline: d.copyDeadline   },
        { role:"designer",   people: d.designer,                      deadline: d.designDeadline },
        { role:"uploader",   people: d.uploader ? [d.uploader] : [],  deadline: d.uploadDeadline },
        { role:"manager",    people: d.manager,                       deadline: d.dueDate        },
      ];

      roles.forEach(({ role, people, deadline }) => {
        if (!people?.length || !deadline) return;
        if (!d.statusUpdated) return;
        const completedAt = new Date(d.statusUpdated);
        const deadlineDate = new Date(deadline);
        deadlineDate.setHours(23, 59, 59, 999);
        const onTime = completedAt <= deadlineDate;
        const month = completedAt.toISOString().slice(0, 7);
        if (completedAt < sinceDate) return;

        people.forEach(p => {
          const key = d.id + role;
          if (existingIds.has(key)) return;
          existingIds.add(key);
          perfLog.push({
            personId:    p.id,
            personName:  p.name,
            role,
            taskId:      d.id,
            taskName:    d.name,
            client:      d.client,
            fromStatus:  "backfill",
            toStatus:    "Complete",
            deadline,
            completedAt: completedAt.toISOString(),
            onTime,
            month,
            backfilled:  true,
          });
          added++;
        });
      });
    });

    writeJSON(PERF_LOG, perfLog);
    console.log(`Backfill complete — added ${added} entries from ${filtered.length} records`);
    res.json({ ok: true, recordsFound: filtered.length, entriesAdded: added });
  } catch (err) {
    console.error("/backfill error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── DM On/Off Switch ─────────────────────────────────────────────
let dmsEnabled = true;

app.post("/dms-toggle", (req, res) => {
  dmsEnabled = !dmsEnabled;
  console.log(`[scheduler] DMs ${dmsEnabled ? "ENABLED" : "DISABLED"}`);
  res.json({ ok: true, dmsEnabled });
});

app.get("/dms-status", (req, res) => {
  res.json({ dmsEnabled });
});

// ── Status Change Polling ─────────────────────────────────────────
const TEAM = {
  "usrDi7oYvN51c0Z4H": { name: "Mariana Lara",       slackId: "U0AGYC9PNUR",  role: "manager"     },
  "usrov3FwJAjCJQSKY": { name: "Laryssa Wirstiuk",   slackId: "U0A9ZBD7K9B",  role: "manager"     },
  "usrP1mWmgGcgnCNTL": { name: "Jeremy Fleming",      slackId: "U070WRQ611D",  role: "manager"     },
  "usrvcB8uzTmePkrwR": { name: "Rebecca O'Sullivan",  slackId: "U09863FSY72",  role: "copywriter"  },
  "usreeW420YviThfXJ": { name: "Carly Reynolds",      slackId: "U09791NT6HZ",  role: "copywriter"  },
  "usrEDbIfe8QXuzpfZ": { name: "Enrique",             slackId: "U09S1DRMWP5",  role: "designer"    },
  "usr5ml09SqVwZl6A6": { name: "Kelvin Molina",       slackId: "U09LEGUPF2S",  role: "uploader"    },
};

const STATUS_ACTIONS = {
  "Ready for Copy":                      { role: "copywriter", action: "Write copy",       urgencyField: "copyDeadline"   },
  "Copywriting in Progress":             { role: "copywriter", action: "Finish copy",      urgencyField: "copyDeadline"   },
  "Copywriting Complete - Ready for QA": { role: "manager",    action: "QA copy",          urgencyField: "copyDeadline"   },
  "Ready For Design":                    { role: "designer",   action: "Start design",     urgencyField: "designDeadline" },
  "Design in Progress":                  { role: "designer",   action: "Finish design",    urgencyField: "designDeadline" },
  "Design Complete - Ready for QA":      { role: "manager",    action: "QA design",        urgencyField: "designDeadline" },
  "Client Review":                       { role: "manager",    action: "Follow up",        urgencyField: "dueDate"        },
  "Upload":                              { role: "uploader",   action: "Upload",           urgencyField: "uploadDeadline" },
  "Schedule":                            { role: "uploader",   action: "Schedule send",    urgencyField: "uploadDeadline" },
  "Revisions":                           { role: "designer",   action: "Make revisions",   urgencyField: "designDeadline" },
};

const AT_BASE_URL = `https://airtable.com/${AT_BASE}/tbltj1I38yoAh2HOF`;
const PRIORITY_COLORS = {
  overdue: "#E53E3E",
  soon:    "#F59E0B",
  later:   "#6D4DF4",
};

function daysUntilEST(str) {
  if (!str) return null;
  const s = String(str).slice(0, 10);
  const parts = s.split("-").map(Number);
  if (parts.length !== 3) return null;
  const d = new Date(parts[0], parts[1] - 1, parts[2], 12, 0, 0);
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  return Math.round((d - today) / 86400000);
}

function deadlineLabelSrv(days, dateStr) {
  if (days === null) return null;
  const d = new Date(dateStr + "T12:00:00");
  const fmt = d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  if (days < 0)  return `${fmt} (${Math.abs(days)}d overdue)`;
  if (days === 0) return `${fmt} (today)`;
  return `${fmt} (${days}d left)`;
}

function buildStatusChangeDM(record, newStatus, person) {
  const sc = STATUS_ACTIONS[newStatus];
  if (!sc) return null;

  const urgencyDate = record[sc.urgencyField] || record.sendDate || null;
  const days = daysUntilEST(urgencyDate);
  const dl = urgencyDate ? deadlineLabelSrv(days, urgencyDate) : null;

  const fieldLabel = {
    copyDeadline: "Copy deadline", designDeadline: "Design deadline",
    uploadDeadline: "Upload deadline", sendDate: "Send date",
  }[sc.urgencyField] || "Due";

  const urgencyColor = days === null ? PRIORITY_COLORS.later
    : days < 0  ? PRIORITY_COLORS.overdue
    : days <= 3 ? PRIORITY_COLORS.soon
    : PRIORITY_COLORS.later;

  const lines = [];
  lines.push(`*${record.name}*`);
  lines.push(`*${newStatus}*`);
  lines.push(`Next: ${sc.action}`);
  if (dl) lines.push(`${fieldLabel}: ${dl}`);

  return {
    text: record.name,
    blocks: [],
    attachments: [
      {
        color: urgencyColor,
        blocks: [
          {
            type: "section",
            text: { type: "mrkdwn", text: lines.join("\n") },
            accessory: {
              type: "button",
              text: { type: "plain_text", text: "Open Task", emoji: false },
              url: `${AT_BASE_URL}/${record.id}`,
              action_id: `open_${record.id}`,
            },
          },
        ],
      },
    ],
  };
}

async function fetchAllDeliverables() {
  const getDate = v => v ? (Array.isArray(v) ? v[0] : v) : null;
  const getPpl  = v => {
    if (!v) return [];
    const arr = Array.isArray(v) ? v : [v];
    return arr.map(p => typeof p === "object" ? { id: p.id || "", name: p.name || "" } : { id: p, name: p });
  };
  let allRecords = [], offset = null;
  do {
    const url = new URL(`https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const atRes = await fetch(url.toString(), { headers: { Authorization: `Bearer ${AT_TOKEN}` } });
    if (!atRes.ok) throw new Error(`Airtable error: ${atRes.status}`);
    const data = await atRes.json();
    const clientRaw = f => f["Client"] || f["Client Name (from Clients)"] || f["Client Name (from Client)"] || f["Client Name"] || "";
    data.records.forEach(r => {
      const f = r.fields;
      allRecords.push({
        id: r.id,
        name: f["Deliverable Name"] || "",
        status: f["Status"] || "",
        client: Array.isArray(clientRaw(f)) ? clientRaw(f)[0] : clientRaw(f),
        sendDate:       getDate(f["Send Date/Activation Date"]),
        copyDeadline:   getDate(Object.entries(f).find(([k]) => k.includes("Copy Deadline"))?.[1]),
        designDeadline: getDate(Object.entries(f).find(([k]) => k.includes("Design Deadline"))?.[1]),
        uploadDeadline: getDate(Object.entries(f).find(([k]) => k.includes("Upload Deadline"))?.[1]),
        manager:    getPpl(f["Manager"]),
        copywriter: getPpl(f["Copywriter"]),
        designer:   getPpl(f["Designer"]),
        uploader:   getPpl(f["Uploader"])[0] || null,
      });
    });
    offset = data.offset || null;
  } while (offset);
  return allRecords;
}

async function sendDirectMessage(slackId, text, blocks, attachments) {
  const body = { channel: slackId, text: text || " " };
  if (blocks?.length)      body.blocks      = blocks;
  if (attachments?.length) body.attachments = attachments;
  await fetch("https://slack.com/api/chat.postMessage", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
    body: JSON.stringify(body),
  });
}

async function pollForStatusChanges() {
  try {
    console.log("[poll] Checking for status changes...");
    const snapshot = readJSON(SNAPSHOT, {});
    const records  = await fetchAllDeliverables();
    const newSnap  = { ...snapshot };
    let changeCount = 0;

    const team = await getTeam();

    for (const record of records) {
      const prev = snapshot[record.id];
      newSnap[record.id] = {
        ...snapshot[record.id],
        status: record.status,
        name: record.name,
      };

      if (!prev || prev.status === record.status) continue;

      const sc = STATUS_ACTIONS[record.status];
      if (!sc) continue;

      let owners = [];
      if (sc.role === "manager")    owners = record.manager    || [];
      if (sc.role === "copywriter") owners = record.copywriter || [];
      if (sc.role === "designer")   owners = record.designer   || [];
      if (sc.role === "uploader")   owners = record.uploader ? [record.uploader] : [];

      for (const owner of owners) {
        const person = team[owner.id];
        if (!person) continue;

        if (sc.role === "copywriter" && /resend/i.test(record.name)) continue;

        const dm = buildStatusChangeDM(record, record.status, person);
        if (!dm) continue;

        await sendDirectMessage(person.slackId, dm.text, dm.blocks, dm.attachments);
        console.log(`[poll] Notified ${person.name} — "${record.name}" → ${record.status}`);
        changeCount++;
      }
    }

    writeJSON(SNAPSHOT, newSnap);
    if (changeCount === 0) console.log("[poll] No changes detected.");
    else console.log(`[poll] ${changeCount} notification(s) sent.`);
  } catch (err) {
    console.error("[poll] Error:", err.message);
  }
}

const POLL_INTERVAL_MS = 5 * 60 * 1000;

// ── Slack Event Subscriptions ─────────────────────────────────────
const processedEvents = new Set();

let deliverableCache = null;
let deliverableCacheTime = 0;
const CACHE_TTL_MS = 5 * 60 * 1000;

async function getCachedDeliverables() {
  const now = Date.now();
  if (deliverableCache && (now - deliverableCacheTime) < CACHE_TTL_MS) {
    return deliverableCache;
  }
  const records = await fetchAllDeliverables();
  deliverableCache = records;
  deliverableCacheTime = now;
  return records;
}

app.post("/slack/events", async (req, res) => {
  const body = req.body;

  if (body.type === "url_verification") {
    return res.json({ challenge: body.challenge });
  }

  res.sendStatus(200);

  const event = body.event;
  if (!event) return;
  if (event.type !== "message") return;
  if (event.subtype) return;
  if (event.bot_id) return;

  if (processedEvents.has(event.client_msg_id)) return;
  if (event.client_msg_id) processedEvents.add(event.client_msg_id);
  if (processedEvents.size > 500) processedEvents.clear();

  const userSlackId = event.user;
  const channelId   = event.channel;
  const userMessage = (event.text || "").trim();
  if (!userMessage) return;

  const team = await getTeam();
  const person = Object.entries(team).find(([, m]) => m.slackId === userSlackId)?.[1];
  const personName = person?.name || "a team member";

  try {
    const records = await getCachedDeliverables();
    const taskContext = await buildTaskContext(records, userSlackId);

    const systemPrompt = `You are Insendra, an assistant for a marketing agency's task management system.
You help team members understand their tasks, deadlines, and workflow.
You are speaking with ${personName}.

Here is the current live task data from Airtable:

${taskContext}

Guidelines:
- Be concise and direct. No fluff.
- Use plain text — no markdown headers, no bullet symbols, minimal bold.
- When listing tasks keep it short: name, status, deadline.
- Dates are in EST.
- Workflow order: Ready for Copy → Copywriting in Progress → Copywriting Complete - Ready for QA → Ready For Design → Design in Progress → Design Complete - Ready for QA → Client Review → Upload → Schedule → Complete
- Client Review means the client is reviewing and a manager needs to follow up.`;

    const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 1024,
        system: systemPrompt,
        messages: [{ role: "user", content: userMessage }],
      }),
    });

    const claudeData = await claudeRes.json();
    if (!claudeRes.ok || claudeData.error || !claudeData.content?.[0]?.text) {
      console.error("[chat] Claude API error:", JSON.stringify(claudeData));
    }
    const reply = claudeData.content?.[0]?.text || "Sorry, I couldn\'t process that. Try again.";

    await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify({ channel: channelId, text: reply }),
    });

    console.log(`[chat] ${personName}: "${userMessage.slice(0, 60)}" → replied`);
  } catch (err) {
    console.error("[chat] Error:", err.message);
    await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify({ channel: channelId, text: "Something went wrong. Try again in a moment." }),
    });
  }
});

async function buildTaskContext(records, userSlackId) {
  const team = await getTeam();
  const lines = [];
  const person = Object.entries(team).find(([, m]) => m.slackId === userSlackId)?.[1];

  const getOwners = r => {
    const sc = STATUS_ACTIONS[r.status];
    if (!sc) return [];
    if (sc.role === "manager")    return r.manager    || [];
    if (sc.role === "copywriter") return r.copywriter || [];
    if (sc.role === "designer")   return r.designer   || [];
    if (sc.role === "uploader")   return r.uploader ? [r.uploader] : [];
    return [];
  };

  if (person) {
    const myTasks = records.filter(r => getOwners(r).some(o => o.id && team[o.id]?.slackId === userSlackId));
    if (myTasks.length) {
      lines.push(`${person.name}'s tasks:`);
      myTasks.forEach(r => {
        const sc = STATUS_ACTIONS[r.status];
        const urgDate = r[sc?.urgencyField] || r.sendDate;
        const days = daysUntilEST(urgDate);
        const daysStr = days === null ? "" : days < 0 ? ` (${Math.abs(days)}d OVERDUE)` : days === 0 ? " (TODAY)" : ` (${days}d)`;
        lines.push(`  ${r.name} | ${r.status}${daysStr}`);
      });
      lines.push("");
    }
  }

  lines.push("All deliverables:");
  records.slice(0, 80).forEach(r => {
    const days = daysUntilEST(r.sendDate);
    const daysStr = days === null ? "" : ` send:${r.sendDate?.slice(0,10)}(${days < 0 ? days+"d" : "+"+days+"d"})`;
    const owners = getOwners(r).map(o => o.name?.split(" ")[0]).filter(Boolean).join(",");
    lines.push(`  ${r.name} | ${r.status}${daysStr} | ${owners}`);
  });

  lines.push("\nTeam:");
  Object.entries(team).forEach(([id, member]) => {
    const count = records.filter(r => getOwners(r).some(o => o.id === id)).length;
    lines.push(`  ${member.name}: ${count} tasks`);
  });

  return lines.join("\n");
}

// ── GET /config — dynamic team + clients ─────────────────────────
app.get("/config", async (req, res) => {
  try {
    const team    = await getTeam();
    const clients = await getClients();
    res.json({ team, clients });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Server-side Daily DM Scheduler ───────────────────────────────
const SERVER_URL = "https://insendra-server.onrender.com";

function estPartsSrv() {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false
  });
  const parts = Object.fromEntries(fmt.formatToParts(new Date()).map(p => [p.type, p.value]));
  return new Date(
    parseInt(parts.year), parseInt(parts.month) - 1, parseInt(parts.day),
    parseInt(parts.hour), parseInt(parts.minute), parseInt(parts.second)
  );
}

function estTodaySrv() {
  const est = estPartsSrv();
  return new Date(est.getFullYear(), est.getMonth(), est.getDate());
}

const isMondaySrv = () => estPartsSrv().getDay() === 1;

function deadlineLabelSched(days, dateStr){
  if(days===null||!dateStr)return null;
  const d=new Date(dateStr);
  const dateFormatted=d.toLocaleDateString("en-US",{month:"short",day:"numeric"});
  if(days<0)  return`Was due ${dateFormatted} (${Math.abs(days)}d ago)`;
  if(days===0)return`Due today`;
  return`Due ${dateFormatted} (${days}d left)`;
}

function isActionable(text){
  const t=text.toLowerCase();
  return ["?","please","can you","could you","need","waiting","follow up",
          "action","update","fix","change","revise","approve","review",
          "feedback","asap","urgent","done?","ready?","status"].some(k=>t.includes(k));
}

function assignTasks(deliverables){
  const map={};
  deliverables.forEach(d=>{
    const sc=STATUS_ACTIONS[d.status];if(!sc)return;
    const{role,action,urgencyField}=sc;
    let who=[];
    if(role==="manager"   &&d.manager?.length)   who=d.manager;
    if(role==="copywriter"&&d.copywriter?.length) who=d.copywriter;
    if(role==="designer"  &&d.designer?.length)   who=d.designer;
    if(role==="uploader"  &&d.uploader)           who=[d.uploader];
    const urgencyDate=d[urgencyField] || d.dueDate || d.sendDate || null;
    const resolvedUrgencyField=d[urgencyField] ? urgencyField
      : d.dueDate ? "dueDate"
      : d.sendDate ? "sendDate"
      : urgencyField;
    const days=daysUntilEST(urgencyDate);
    if(role==="copywriter" && /resend/i.test(d.name)) return;
    who.forEach(p=>{
      map[p.id]=map[p.id]||[];
      map[p.id].push({id:d.id,name:d.name,client:d.client,action,days,sendDate:d.sendDate,urgencyField:resolvedUrgencyField,urgencyDate,status:d.status,createdTime:d.createdTime||null,statusUpdated:d.statusUpdated||null,manager:d.manager,designer:d.designer,copywriter:d.copywriter,uploader:d.uploader});
    });
  });
  Object.values(map).forEach(arr=>arr.sort((a,b)=>(a.days??999)-(b.days??999)));
  return map;
}

async function fetchRecordComments(recordId) {
  try {
    const url = `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}/${recordId}/comments?pageSize=10`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${AT_TOKEN}` } });
    if (!res.ok) return [];
    const data = await res.json();
    return (data.comments || []).map(c => ({ text: c.text, author: c.author?.name || "" }));
  } catch { return []; }
}

async function triggerSnapshotSrv() {
  try { await fetch(`${SERVER_URL}/snapshot`, { method: "POST" }); } catch {}
}

function buildDMMessageSrv(person,tasks,weekly){
  const date=estTodaySrv().toLocaleDateString("en-US",{weekday:"long",month:"long",day:"numeric"});
  const grouped={overdue:[],today:[],soon:[],later:[]};

  tasks.forEach(t=>{
    if(t.days===null)return;
    const u=urgencyInfo(t.days);
    grouped[u.section].push({...t,u});
  });

  const total=grouped.overdue.length+grouped.today.length+grouped.soon.length+grouped.later.length;
  const attachments=[];

  const sectionConfig=[
    {key:"overdue",label:"OVERDUE",           color:PRIORITY_COLORS.overdue},
    {key:"today",  label:"DUE SOON",          color:PRIORITY_COLORS.soon},
    {key:"soon",   label:"COMING UP",         color:PRIORITY_COLORS.later},
    {key:"later",  label:"UPCOMING",          color:PRIORITY_COLORS.later},
  ];

  attachments.push({
    color:"#6D4DF4",
    blocks:[{
      type:"section",
      fields:[
        {type:"mrkdwn",text:`*${person.name}*\n${person.role}`},
        {type:"mrkdwn",text:`*${date}*\n${total} task${total!==1?"s":""} today · <${SERVER_URL}/dashboard#user=${person.id}|📊 My Dashboard>`},
      ]
    }]
  });

  const insights=[];

  [...grouped.overdue,...grouped.today,...grouped.soon].forEach(t=>{
    const statusDate=t.statusUpdated||t.createdTime;
    if(!statusDate)return;
    const daysStuck=Math.round((Date.now()-new Date(statusDate))/(86400000));
    if(daysStuck>=5){
      insights.push(`*${t.name}* has been in "${t.status}" for ${daysStuck} day${daysStuck!==1?"s":""} with no status change.`);
    }
  });

  tasks.forEach(t=>{
    const ns=STATUS_ACTIONS?.[t.status];
    if(!ns)return;
    if(ns.role===person.role.toLowerCase())return;
    let blockedName="";
    if(ns.role==="designer"  &&t.designer?.length)  blockedName=t.designer[0].name.split(" ")[0];
    if(ns.role==="copywriter"&&t.copywriter?.length) blockedName=t.copywriter[0].name.split(" ")[0];
    if(ns.role==="uploader"  &&t.uploader)           blockedName=t.uploader.name?.split(" ")[0]||"";
    if(ns.role==="manager"   &&t.manager?.length)    blockedName=t.manager[0].name.split(" ")[0];
    if(blockedName){
      insights.push(`*${t.name}* — ${blockedName} is waiting to ${ns.action} once this is done.`);
    }
  });

  tasks.filter(t=>t.comments&&t.comments.length>0).forEach(t=>{
    const latest=t.comments[0];
    const author=latest.author?.name?.split(" ")[0]||"Someone";
    const msg=(latest.text||"").slice(0,100);
    insights.push(`*${t.name}* — ${author} commented: _"${msg}"_`);
  });

  let closingLine="";
  if(grouped.overdue.length===0&&grouped.today.length===0){
    closingLine="No urgent deadlines today.";
  } else if(grouped.overdue.length>0){
    closingLine=`${grouped.overdue.length} item${grouped.overdue.length>1?"s":""} overdue.`;
  } else {
    closingLine=`${grouped.today.length} item${grouped.today.length>1?"s":""} due within 3 days.`;
  }

  const summaryText=insights.length>0
    ? insights.join("\n")+`\n\n${closingLine}`
    : closingLine;

  attachments.push({
    color:"#6D4DF4",
    blocks:[{type:"section",text:{type:"mrkdwn",text:summaryText}}]
  });

  if(total===0){
    attachments.push({color:"#6D4DF4",blocks:[
      {type:"context",elements:[{type:"mrkdwn",text:"*insendra*"}]}
    ]});
    return {blocks:[],attachments,text:`Task update for ${person.name}`};
  }

  sectionConfig.forEach(({key,label,color})=>{
    const items=grouped[key];
    if(!items.length)return;

    const sectionBlocks=[];
    sectionBlocks.push({type:"section",text:{type:"mrkdwn",text:`*${label}*`}});
    sectionBlocks.push({type:"divider"});

    items.forEach((t,i)=>{
      const dl=t.days!==null && t.urgencyDate
        ? deadlineLabelSched(t.days, t.urgencyDate)
        : null;

      const dlFieldName={
        copyDeadline:"Copy deadline",
        designDeadline:"Design deadline",
        uploadDeadline:"Upload deadline",
        sendDate:"Send date",
        dueDate:"",
      }[t.urgencyField];

      const dlFormatted=dl?(dlFieldName?`${dlFieldName}: ${dl}`:dl):null;
      const secondLine=[t.action, dlFormatted].filter(Boolean).join("   ·   ");

      let nextStr="";
      const ns=STATUS_ACTIONS?.[t.status];
      if(ns){
        let nextPerson="";
        if(ns.role==="designer"  &&t.designer?.length)  nextPerson=t.designer[0].name.split(" ")[0];
        if(ns.role==="uploader"  &&t.uploader)          nextPerson=t.uploader.name.split(" ")[0];
        if(ns.role==="manager"   &&t.manager?.length)   nextPerson=t.manager[0].name.split(" ")[0];
        if(ns.role==="copywriter"&&t.copywriter?.length) nextPerson=t.copywriter[0].name.split(" ")[0];
        nextStr=nextPerson ? `_Next: ${nextPerson} to ${ns.action}_` : `_Next: ${ns.action}_`;
      }

      let commentStr="";
      if(t.comments&&t.comments.length){
        const lines=t.comments.map(c=>{
          const author=c.author?.name?.split(" ")[0]||"Someone";
          const msg=(c.text||"").slice(0,120);
          return `_${author}: "${msg}"_`;
        });
        commentStr=lines.join("\n");
      }

      const fullText=[
        `*${t.name}*`,
        secondLine,
        nextStr||"",
        commentStr ? `*Comments:*\n${commentStr}` : ""
      ].filter(Boolean).join("\n");

      sectionBlocks.push({
        type:"section",
        text:{type:"mrkdwn",text:fullText},
        accessory:{
          type:"button",
          text:{type:"plain_text",text:"Open Task",emoji:false},
          url:`${AT_BASE_URL}/${t.id}`,
          action_id:`open_${t.id}`,
        }
      });
      if(i<items.length-1) sectionBlocks.push({type:"divider"});
    });

    attachments.push({color, blocks:sectionBlocks});
    attachments.push({color:"#E2E2E2",blocks:[{type:"section",text:{type:"mrkdwn",text:" "}}]});
  });

  attachments.push({
    color:"#6D4DF4",
    blocks:[{type:"context",elements:[{type:"mrkdwn",text:`*insendra*   ·   Reply in thread to flag an update`}]}]
  });

  return {blocks:[],attachments,text:`Task update for ${person.name}`};
}

function urgencyInfo(days){
  if(days===null)return{section:"later"};
  if(days<0)     return{section:"overdue"};
  if(days===0)   return{section:"today"};
  if(days<=3)    return{section:"today"};
  if(days<=14)   return{section:"soon"};
  return              {section:"later"};
}

function buildSynopsis(client, grouped){
  const lines=[];
  const allItems=[
    ...(grouped.overdue||[]),
    ...(grouped.today||[]),
    ...(grouped.soon||[]),
    ...(grouped.later||[])
  ];

  (grouped.overdue||[]).forEach(t=>{
    const d=Math.abs(t.days);
    const flag=d>=7?` This has been overdue for over a week.`:"";
    lines.push(`*${t.name}* is ${d}d overdue — ${t.status} needs to be resolved immediately.${flag}`);
  });

  [...(grouped.today||[]),...(grouped.soon||[])].forEach(t=>{
    const statusDate=t.statusUpdated||t.createdTime;
    if(!statusDate)return;
    const daysInStatus=Math.round((Date.now()-new Date(statusDate))/(86400000));
    if(daysInStatus>=5){
      lines.push(`*${t.name}* has been sitting in "${t.status}" for ${daysInStatus} days — this needs to progress.`);
    }
  });

  allItems.filter(t=>t.status==="Client Review").forEach(t=>{
    const statusDate=t.statusUpdated||t.createdTime;
    if(!statusDate)return;
    const waiting=Math.round((Date.now()-new Date(statusDate))/(86400000));
    if(waiting>=3){
      lines.push(`*${t.name}* has been with the client for ~${waiting} days — a follow-up may be needed.`);
    }
  });

  const qaItems=allItems.filter(t=>
    t.status==="Copywriting Complete - Ready for QA"||
    t.status==="Design Complete - Ready for QA"
  );
  if(qaItems.length){
    const names=qaItems.map(t=>t.name).join(", ");
    lines.push(`*QA needed:* ${names}`);
  }

  allItems.filter(t=>t.comments&&t.comments.length>0).forEach(t=>{
    const latest=t.comments[0];
    const author=latest.author?.name?.split(" ")[0]||"Someone";
    const msg=(latest.text||"").slice(0,100);
    lines.push(`*${t.name}* — ${author} commented: _"${msg}"_`);
  });

  const steps=[];
  if((grouped.overdue||[]).length)
    steps.push("Resolve overdue items first — these are blocking the pipeline");
  if(allItems.some(t=>t.status==="Client Review"))
    steps.push("Follow up on client approvals — check how long each has been waiting");
  if(qaItems.length)
    steps.push("Clear the QA queue before it becomes a bottleneck");
  if(allItems.some(t=>t.status==="Upload"||t.status==="Schedule"))
    steps.push("Confirm all uploads and sends scheduled for this week are locked in");

  if(steps.length){
    lines.push("");
    lines.push("*This week:*");
    steps.forEach((s,i)=>lines.push(`${i+1}. ${s}`));
  }

  return lines.join("\n");
}

function buildDigestMessageSrv(client,items){
  const date=estTodaySrv().toLocaleDateString("en-US",{weekday:"long",month:"long",day:"numeric"});
  const grouped={overdue:[],today:[],soon:[],later:[]};

  items.forEach(t=>{
    if(t.days===null)return;
    const u=urgencyInfo(t.days);
    grouped[u.section].push({...t,u});
  });

  const total=grouped.overdue.length+grouped.today.length+grouped.soon.length+grouped.later.length;
  const attachments=[];

  attachments.push({
    color:"#6D4DF4",
    blocks:[
      {type:"section",fields:[
        {type:"mrkdwn",text:`*${client}*\nWeekly Digest`},
        {type:"mrkdwn",text:`*${date}*\n${total} active deliverable${total!==1?"s":""}`},
      ]},
      {type:"context",elements:[{
        type:"mrkdwn",
        text:`<${SERVER_URL}/dashboard#client=${encodeURIComponent(client)}|📋 View Client Dashboard>`
      }]}
    ]
  });

  const synopsis=buildSynopsis(client,grouped);
  if(synopsis){
    attachments.push({
      color:"#6D4DF4",
      blocks:[{type:"section",text:{type:"mrkdwn",text:synopsis}}]
    });
  }

  if(total===0){
    attachments.push({color:"#6D4DF4",blocks:[
      {type:"section",text:{type:"mrkdwn",text:"Nothing active right now."}},
      {type:"context",elements:[{type:"mrkdwn",text:"*insendra*"}]}
    ]});
    return {blocks:[],attachments,text:`${client} — Weekly Digest`};
  }

  const sectionConfig=[
    {key:"overdue",label:"OVERDUE",           color:PRIORITY_COLORS.overdue},
    {key:"today",  label:"DUE SOON",          color:PRIORITY_COLORS.soon},
    {key:"soon",   label:"COMING UP",         color:PRIORITY_COLORS.later},
    {key:"later",  label:"UPCOMING",          color:PRIORITY_COLORS.later},
  ];

  sectionConfig.forEach(({key,label,color})=>{
    const items=grouped[key];
    if(!items.length)return;

    const sectionBlocks=[];
    sectionBlocks.push({type:"section",text:{type:"mrkdwn",text:`*${label}*`}});
    sectionBlocks.push({type:"divider"});

    items.forEach((t,i)=>{
      const dl=t.days!==null && t.urgencyDate
        ? deadlineLabelSched(t.days, t.urgencyDate)
        : null;

      let nextStr="";
      const ns=STATUS_ACTIONS?.[t.status];
      if(ns){
        let nextPerson="";
        if(ns.role==="designer"  &&t.designer?.length)  nextPerson=t.designer[0].name.split(" ")[0];
        if(ns.role==="uploader"  &&t.uploader)          nextPerson=t.uploader.name.split(" ")[0];
        if(ns.role==="manager"   &&t.manager?.length)   nextPerson=t.manager[0].name.split(" ")[0];
        if(ns.role==="copywriter"&&t.copywriter?.length) nextPerson=t.copywriter[0].name.split(" ")[0];
        nextStr=nextPerson?`_Next: ${nextPerson} to ${ns.action}_`:`_Next: ${ns.action}_`;
      }

      const sc=STATUS_ACTIONS[t.status];
      const action=sc?sc.action:"";
      const secondLine=[action,dl].filter(Boolean).join("   ·   ");
      const fullText=nextStr?`*${t.name}*\n${secondLine}\n${nextStr}`:`*${t.name}*\n${secondLine}`;

      sectionBlocks.push({
        type:"section",
        text:{type:"mrkdwn",text:fullText},
        accessory:{
          type:"button",
          text:{type:"plain_text",text:"Open Task",emoji:false},
          url:`${AT_BASE_URL}/${t.id}`,
          action_id:`open_${t.id}`,
        }
      });
      if(i<items.length-1)sectionBlocks.push({type:"divider"});
    });

    attachments.push({color,blocks:sectionBlocks});
    attachments.push({color:"#E2E2E2",blocks:[{type:"section",text:{type:"mrkdwn",text:" "}}]});
  });

  attachments.push({color:"#6D4DF4",blocks:[{type:"context",elements:[{type:"mrkdwn",text:`*insendra*   ·   Reply in thread to flag an update`}]}]});

  return {blocks:[],attachments,text:`${client} — Weekly Digest`};
}

async function runDailyDMs() {
  if (!dmsEnabled) {
    console.log("[scheduler] DMs are disabled — skipping run");
    return;
  }
  const weekly = isMondaySrv();
  console.log(`[scheduler] Starting ${weekly ? "weekly" : "daily"} DM run...`);

  try {
    await triggerSnapshotSrv();

    const deliverables = await fetchAllDeliverables();
    console.log(`[scheduler] Loaded ${deliverables.length} deliverables`);

    const tasks = assignTasks(deliverables);
    console.log(`[scheduler] Assigned tasks to ${Object.keys(tasks).length} team members`);

    const allTaskIds = [...new Set(Object.values(tasks).flat().map(t => t.id))];
    const commentsMap = {};
    await Promise.all(allTaskIds.map(async id => {
      const comments = await fetchRecordComments(id);
      const actionable = comments.filter(c => isActionable(c.text || ""));
      if (actionable.length) commentsMap[id] = actionable;
    }));

    const team = await getTeam();

    let sent = 0;
    for (const [id, list] of Object.entries(tasks)) {
      const member = team[id];
      if (!member) continue;
      const person = { ...member, id };
      try {
        const listWithComments = list.map(t => ({ ...t, comments: commentsMap[t.id] || [] }));
        const { blocks, attachments, text } = buildDMMessageSrv(person, listWithComments, weekly);
        await sendDirectMessage(member.slackId, text, blocks, attachments);
        console.log(`[scheduler] Sent to ${member.name} — ${list.length} task(s)`);
        sent++;
      } catch (err) {
        console.error(`[scheduler] Failed to send to ${member?.name}:`, err.message);
      }
    }

    if (weekly) {
      const clients = await getClients();
      console.log(`[scheduler] Sending weekly digests to ${Object.keys(clients).length} clients...`);
      for (const [clientName, clientConfig] of Object.entries(clients)) {
        if (!clientConfig.slackChannelId) continue;
        try {
          const prefix = (clientConfig.prefix || "").toUpperCase();
          const items = deliverables.filter(d => {
            if (!d.name) return false;
            const m = d.name.match(/^\[([A-Z0-9]+)\]/);
            return m && m[1].toUpperCase() === prefix;
          });
          if (!items.length) continue;
          const { blocks, attachments, text } = buildDigestMessageSrv(clientName, items);
          await fetch("https://slack.com/api/chat.postMessage", {
            method: "POST",
            headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
            body: JSON.stringify({ channel: clientConfig.slackChannelId, text, blocks, attachments }),
          });
          console.log(`[scheduler] Sent weekly digest for ${clientName}`);
        } catch (err) {
          console.error(`[scheduler] Digest failed for ${clientName}:`, err.message);
        }
      }
    }

    console.log(`[scheduler] Done — ${sent} DMs sent`);
  } catch (err) {
    console.error("[scheduler] Run failed:", err.message);
  }
}

let lastRunDate = null;

function startDailyScheduler() {
  console.log("[scheduler] Cron-style scheduler started — checking every minute for 9AM EST Mon-Fri");

  setInterval(async () => {
    const now = estPartsSrv();
    const hour = now.getHours();
    const minute = now.getMinutes();
    const day = now.getDay();
    const dateKey = `${now.getFullYear()}-${now.getMonth()}-${now.getDate()}`;

    if (hour === 9 && minute === 0 && day >= 1 && day <= 5 && lastRunDate !== dateKey) {
      lastRunDate = dateKey;
      console.log(`[scheduler] 9AM EST trigger fired (${dateKey})`);
      await runDailyDMs();
    }
  }, 60 * 1000);

  const now = estPartsSrv();
  const next = new Date(now);
  next.setHours(9, 0, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  while (next.getDay() === 0 || next.getDay() === 6) next.setDate(next.getDate() + 1);
  const nextLabel = `${next.getMonth()+1}/${next.getDate()}/${next.getFullYear()}, 9:00:00 AM`;
  const hUntil = Math.round((next - now) / 3600000 * 10) / 10;
  console.log(`[scheduler] Next DM run in ~${hUntil}h (${nextLabel} EST)`);
}

function scheduleNextDailyRun() {
  startDailyScheduler();
}

// ── POST /run-dms-now — manual trigger from admin dashboard ───────
app.post("/run-dms-now", async (req, res) => {
  res.json({ ok: true, message: "DM run triggered — check Render logs" });
  await runDailyDMs();
});

// ── Klaviyo proxy helpers ─────────────────────────────────────────
const KV_BASE = "https://a.klaviyo.com/api";
const KV_REV  = "2024-10-15";

function kvHeaders(key) {
  return { Authorization: `Klaviyo-API-Key ${key}`, revision: KV_REV, Accept: "application/json" };
}
async function kvGet(key, path) {
  const r = await fetch(`${KV_BASE}${path}`, { headers: kvHeaders(key) });
  const j = await r.json();
  if (!r.ok) console.error(`[kv GET ${path}]`, JSON.stringify(j).slice(0, 300));
  return j;
}
async function kvPost(key, path, body) {
  const r = await fetch(`${KV_BASE}${path}`, {
    method: "POST",
    headers: { ...kvHeaders(key), "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const j = await r.json();
  if (!r.ok) console.error(`[kv POST ${path}]`, JSON.stringify(j).slice(0, 300));
  return j;
}

// ── POST /klaviyo-data — main proxy ──────────────────────────────
app.post("/klaviyo-data", async (req, res) => {
  const { klaviyoKey } = req.body;
  if (!klaviyoKey) return res.status(400).json({ error: "Missing klaviyoKey" });

  try {
    // ── Step 1: Account + lists/segments (fast, required) ──────────
    const [acctRes, listRes, segRes] = await Promise.all([
      kvGet(klaviyoKey, "/accounts/"),
      kvGet(klaviyoKey, "/lists/?page[size]=1").catch(() => ({})),
      kvGet(klaviyoKey, "/segments/?page[size]=1").catch(() => ({})),
    ]);
    const acct = acctRes.data?.[0]?.attributes ?? {};

    // ── Step 2: Get conversion metric ID ───────────────────────────
    const metricsRes = await kvGet(klaviyoKey, "/metrics/?page[size]=200&fields[metric]=name").catch(() => ({}));
    const metrics    = metricsRes.data || [];
    const convMetric = metrics.find(m => m.attributes?.name === "Placed Order")
                    || metrics.find(m => /order|purchase|revenue/i.test(m.attributes?.name || ""))
                    || null;
    const convId = convMetric?.id || null;
    console.log(`[kv] convId=${convId} (${convMetric?.attributes?.name}), metrics found=${metrics.length}`);

    // ── Step 3: Fetch all campaigns + flows ────────────────────────
    // Note: No status filter in URL — filter in code so we see the raw statuses
    const [campRes, flowRes] = await Promise.all([
      kvGet(klaviyoKey, "/campaigns/?sort=-send_time&page[size]=50&fields[campaign]=name,status,send_time,audiences"),
      kvGet(klaviyoKey, "/flows/?page[size]=50&sort=-updated&fields[flow]=name,status,trigger_type"),
    ]);

    const allCamps = campRes.data ?? [];
    const allFlows = flowRes.data ?? [];

    // Log the actual statuses we see (helps debug)
    const statuses = [...new Set(allCamps.map(c => c.attributes?.status))];
    console.log(`[kv] All campaign statuses: ${statuses.join(", ")} (total ${allCamps.length})`);

    // Accept all "done sending" statuses
    const sentCamps = allCamps.filter(c => {
      const s = (c.attributes?.status || "").toLowerCase();
      return ["sent", "complete", "sending", "variations sent"].includes(s);
    });
    console.log(`[kv] Sent campaigns: ${sentCamps.length}, All flows: ${allFlows.length}`);

    // ── Step 4: Campaign stats ─────────────────────────────────────
    let campStats = {};
    if (sentCamps.length > 0) {
      const baseStats = ["recipients", "open_rate", "click_rate", "unsubscribe_rate", "bounce_rate"];
      const convStats = convId ? [...baseStats, "conversions"] : baseStats;
      const body = {
        data: {
          type: "campaign-values-report",
          attributes: {
            timeframe: { key: "last_12_months" },
            statistics: convStats,
            ...(convId ? {
              conversion_metric_id: convId,
              value_statistics: ["conversion_value", "revenue_per_recipient", "average_order_value"],
            } : {}),
          },
        },
      };
      campStats = await kvPost(klaviyoKey, "/campaign-values-reports/", body).catch(e => {
        console.error("[kv] camp-stats failed:", e.message); return {};
      });
      console.log(`[kv] campStats results: ${campStats.data?.attributes?.results?.length ?? "err"}`);
    }

    // ── Step 5: Flow stats ─────────────────────────────────────────
    let flowStats = {};
    if (allFlows.length > 0) {
      const baseStats = ["recipients", "open_rate", "click_rate"];
      const convStats = convId ? [...baseStats, "conversions"] : baseStats;
      const body = {
        data: {
          type: "flow-values-report",
          attributes: {
            timeframe: { key: "last_12_months" },
            statistics: convStats,
            ...(convId ? {
              conversion_metric_id: convId,
              value_statistics: ["conversion_value", "revenue_per_recipient"],
            } : {}),
          },
        },
      };
      flowStats = await kvPost(klaviyoKey, "/flow-values-reports/", body).catch(e => {
        console.error("[kv] flow-stats failed:", e.message); return {};
      });
      console.log(`[kv] flowStats results: ${flowStats.data?.attributes?.results?.length ?? "err"}`);
    }

    // ── Step 6: Monthly revenue chart (only if convId) ─────────────
    let monthly = [];
    if (convId) {
      const mRes = await kvPost(klaviyoKey, "/metric-aggregates/", {
        data: {
          type: "metric-aggregate",
          attributes: {
            metric_id: convId,
            measurements: ["sum_value"],
            interval: "month",
            timeframe: { key: "last_12_months" },
          },
        },
      }).catch(() => ({}));
      const dates  = mRes.data?.attributes?.dates ?? [];
      const points = mRes.data?.attributes?.data  ?? [];
      monthly = dates.map((d, i) => ({
        month:   d,
        revenue: points.reduce((s, p) => s + (p.measurements?.sum_value?.[i] ?? 0), 0),
      }));
    }

    // ── Step 7: Match stats to campaigns/flows ─────────────────────
    const cs = campStats.data?.attributes?.results ?? [];
    const fs = flowStats.data?.attributes?.results ?? [];

    const campaigns = sentCamps.slice(0, 30).map(c => {
      const result = cs.find(r => r.groupings?.campaign_id === c.id);
      const s  = result?.statistics      ?? {};
      const vs = result?.value_statistics ?? {};
      return {
        id:               c.id,
        name:             c.attributes.name,
        send_time:        c.attributes.send_time,
        recipients:       s.recipients       ?? null,
        open_rate:        s.open_rate        ?? null,
        click_rate:       s.click_rate       ?? null,
        unsubscribe_rate: s.unsubscribe_rate ?? null,
        conversions:      s.conversions      ?? null,
        revenue:          vs.conversion_value         ?? null,
        rpr:              vs.revenue_per_recipient    ?? null,
        aov:              vs.average_order_value      ?? null,
      };
    });

    const flows = allFlows.map(f => {
      const result = fs.find(r => r.groupings?.flow_id === f.id);
      const s  = result?.statistics      ?? {};
      const vs = result?.value_statistics ?? {};
      return {
        id:           f.id,
        name:         f.attributes.name,
        status:       f.attributes.status,
        trigger_type: f.attributes.trigger_type,
        recipients:   s.recipients  ?? null,
        open_rate:    s.open_rate   ?? null,
        click_rate:   s.click_rate  ?? null,
        conversions:  s.conversions ?? null,
        revenue:      vs.conversion_value      ?? null,
        rpr:          vs.revenue_per_recipient ?? null,
      };
    });

    res.json({
      account: {
        name:     acct.contact_information?.organization_name ?? "Account",
        timezone: acct.timezone ?? "",
        currency: acct.preferred_currency ?? "",
      },
      has_revenue:   !!convId,
      conv_metric:   convMetric?.attributes?.name ?? null,
      campaigns,
      flows,
      monthly,
      list_count:    listRes.meta?.total_count  ?? (listRes.data?.length  ?? 0),
      segment_count: segRes.meta?.total_count   ?? (segRes.data?.length   ?? 0),
      _debug: {
        all_campaign_count:  allCamps.length,
        sent_campaign_count: sentCamps.length,
        flow_count:          allFlows.length,
        campaign_statuses:   statuses,
        conv_metric_id:      convId,
        conv_metric_name:    convMetric?.attributes?.name,
        camp_stats_rows:     cs.length,
        flow_stats_rows:     fs.length,
      },
    });
  } catch (err) {
    console.error("[klaviyo-proxy] fatal:", err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /klaviyo-debug — returns raw API snapshot for debugging ───
app.get("/klaviyo-debug", async (req, res) => {
  const key = req.query.key;
  if (!key) return res.status(400).json({ error: "Pass ?key=YOUR_PRIVATE_KEY" });
  try {
    const [camps, flows, metrics] = await Promise.all([
      kvGet(key, "/campaigns/?sort=-send_time&page[size]=5&fields[campaign]=name,status,send_time"),
      kvGet(key, "/flows/?page[size]=5&fields[flow]=name,status,trigger_type"),
      kvGet(key, "/metrics/?page[size]=20&fields[metric]=name"),
    ]);
    res.json({
      campaigns_sample: camps.data?.slice(0,5).map(c=>({id:c.id,...c.attributes})),
      flows_sample:     flows.data?.slice(0,5).map(f=>({id:f.id,...f.attributes})),
      metrics:          metrics.data?.map(m=>({id:m.id,name:m.attributes?.name})),
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── GET /dashboard ────────────────────────────────────────────────
app.get("/dashboard", (req, res) => {
  res.sendFile(path.join(__dirname, "dashboard.html"));
});

// ── NEW: GET /client-dashboard ────────────────────────────────────
app.get("/client-dashboard", (req, res) => {
  res.sendFile(path.join(__dirname, "client-dashboard.html"));
});

app.listen(PORT, () => {
  console.log(`Insendra server running on port ${PORT}`);
  loadDynamicConfig().then(() => {
    console.log(`[poll] Status change polling started — every ${POLL_INTERVAL_MS / 60000} minutes`);
    setTimeout(() => {
      pollForStatusChanges();
      setInterval(pollForStatusChanges, POLL_INTERVAL_MS);
    }, 30000);
    setInterval(loadDynamicConfig, CONFIG_TTL);
    scheduleNextDailyRun();
  });
});
