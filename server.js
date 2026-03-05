const express    = require("express");
const path       = require("path");
const cors       = require("cors");
const fs         = require("fs");

const app        = express();
const PORT       = process.env.PORT || 10000;
const AT_TOKEN   = process.env.AT_TOKEN;
const AT_BASE    = process.env.AT_BASE    || "appdD2UGbFIfzkj7q";
const AT_TABLE   = process.env.AT_TABLE   || "tbltj1I38yoAh2HOF";
const SLACK_TOKEN   = process.env.SLACK_TOKEN;
const ANTHROPIC_KEY = process.env.ANTHROPIC_KEY;

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
// Fetches completed records since a start date and infers on-time
// status using sendDate as the completion deadline proxy.
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
      // Fetch completed records with a send date after our since date
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

    // Filter to records after sinceDate
    const filtered = allRecords.filter(d => d.sendDate && new Date(d.sendDate) >= sinceDate);

    const perfLog = readJSON(PERF_LOG, []);
    const existingIds = new Set(perfLog.map(e => e.taskId + e.role));
    let added = 0;

    filtered.forEach(d => {
      // For each role, check if they had a deadline and infer on-time
      const roles = [
        { role:"copywriter", people: d.copywriter,                    deadline: d.copyDeadline   },
        { role:"designer",   people: d.designer,                      deadline: d.designDeadline },
        { role:"uploader",   people: d.uploader ? [d.uploader] : [],  deadline: d.uploadDeadline },
        { role:"manager",    people: d.manager,                       deadline: d.dueDate        },
      ];

      roles.forEach(({ role, people, deadline }) => {
        if (!people?.length || !deadline) return;
        // Only use statusUpdated — skip if not available (no proxy fallback)
        if (!d.statusUpdated) return;
        const completedAt = new Date(d.statusUpdated);
        const deadlineDate = new Date(deadline);
        deadlineDate.setHours(23, 59, 59, 999);
        const onTime = completedAt <= deadlineDate;
        const month = completedAt.toISOString().slice(0, 7);

        // Skip if before sinceDate
        if (completedAt < sinceDate) return;

        people.forEach(p => {
          const key = d.id + role;
          if (existingIds.has(key)) return; // don't duplicate
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


// ── Status Change Polling ─────────────────────────────────────────
// Runs every 5 minutes, compares current Airtable statuses to snapshot,
// sends a rich DM to the new task owner on any status change.

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
  "Client Review":                       { role: "manager",    action: "Follow up",        urgencyField: "sendDate"       },
  "Upload":                              { role: "uploader",   action: "Upload",           urgencyField: "uploadDeadline" },
  "Schedule":                            { role: "uploader",   action: "Schedule send",    urgencyField: "uploadDeadline" },
  "Revisions":                           { role: "designer",   action: "Make revisions",   urgencyField: "designDeadline" },
};

const AT_BASE_URL = "https://airtable.com/appdD2UGbFIfzkj7q/tbltj1I38yoAh2HOF";
const PRIORITY_COLORS = {
  overdue: "#E53E3E",
  soon:    "#F59E0B",
  later:   "#6D4DF4",
};

function daysUntilEST(str) {
  if (!str) return null;
  const parts = str.slice(0, 10).split("-").map(Number);
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
  }[sc.urgencyField] || "";

  const dlStr = dl ? (fieldLabel ? `${fieldLabel}: ${dl}` : dl) : null;
  const secondLine = [sc.action, dlStr].filter(Boolean).join("   ·   ");

  const urgencyColor = days === null ? PRIORITY_COLORS.later
    : days < 0  ? PRIORITY_COLORS.overdue
    : days <= 3 ? PRIORITY_COLORS.soon
    : PRIORITY_COLORS.later;

  const firstName = person.name.split(" ")[0];
  const headerText = `*${record.name}* has been assigned to you.`;
  const subText = `Status changed to *${newStatus}*`;

  const attachments = [
    {
      color: urgencyColor,
      blocks: [
        {
          type: "section",
          text: { type: "mrkdwn", text: `${headerText}\n${subText}\n${secondLine}` },
          accessory: {
            type: "button",
            text: { type: "plain_text", text: "Open Task", emoji: false },
            url: `${AT_BASE_URL}/${record.id}`,
            action_id: `open_${record.id}`,
          },
        },
      ],
    },
  ];

  return {
    text: `Status update — ${newStatus}`,
    blocks: [],
    attachments: [attachments[0]], // only the main card, no footer
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
        uploader:   getPpl(f["Uploader"]) [0] || null,
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

    for (const record of records) {
      const prev = snapshot[record.id];
      newSnap[record.id] = {
        ...snapshot[record.id],
        status: record.status,
        name: record.name,
      };

      // Skip if no previous snapshot or status hasn't changed
      if (!prev || prev.status === record.status) continue;

      const sc = STATUS_ACTIONS[record.status];
      if (!sc) continue;

      // Find who owns this task now
      let owners = [];
      if (sc.role === "manager")    owners = record.manager    || [];
      if (sc.role === "copywriter") owners = record.copywriter || [];
      if (sc.role === "designer")   owners = record.designer   || [];
      if (sc.role === "uploader")   owners = record.uploader ? [record.uploader] : [];

      for (const owner of owners) {
        const person = TEAM[owner.id];
        if (!person) continue;

        // Skip resend tasks for copywriters
        if (sc.role === "copywriter" && /resend/i.test(record.name)) continue;

        const dm = buildStatusChangeDM(record, record.status, person);
        if (!dm) continue;

        await sendDirectMessage(person.slackId, dm.text, dm.blocks, dm.attachments);
        console.log(`[poll] Notified ${person.name} — "${record.name}" → ${record.status}`);
        changeCount++;
      }
    }

    // Save updated snapshot
    writeJSON(SNAPSHOT, newSnap);
    if (changeCount === 0) console.log("[poll] No changes detected.");
    else console.log(`[poll] ${changeCount} notification(s) sent.`);
  } catch (err) {
    console.error("[poll] Error:", err.message);
  }
}

// Start polling every 5 minutes
const POLL_INTERVAL_MS = 5 * 60 * 1000;
setTimeout(() => {
  pollForStatusChanges(); // initial run after 30s startup delay
  setInterval(pollForStatusChanges, POLL_INTERVAL_MS);
}, 30000);

console.log(`[poll] Status change polling started — every ${POLL_INTERVAL_MS / 60000} minutes`);


// ── Slack Event Subscriptions ─────────────────────────────────────
const processedEvents = new Set();

app.post("/slack/events", async (req, res) => {
  const body = req.body;

  // URL verification challenge (Slack setup)
  if (body.type === "url_verification") {
    return res.json({ challenge: body.challenge });
  }

  // Acknowledge immediately so Slack doesn't retry
  res.sendStatus(200);

  const event = body.event;
  if (!event) return;
  if (event.type !== "message") return;
  if (event.subtype) return;
  if (event.bot_id) return;

  // Dedup
  if (processedEvents.has(event.client_msg_id)) return;
  if (event.client_msg_id) processedEvents.add(event.client_msg_id);
  if (processedEvents.size > 500) processedEvents.clear();

  const userSlackId = event.user;
  const channelId   = event.channel;
  const userMessage = (event.text || "").trim();
  if (!userMessage) return;

  const person = Object.entries(TEAM).find(([, m]) => m.slackId === userSlackId)?.[1];
  const personName = person?.name || "a team member";

  try {
    const records = await fetchAllDeliverables();
    const taskContext = buildTaskContext(records, userSlackId);

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
    const reply = claudeData.content?.[0]?.text || "Sorry, I couldn't process that. Try again.";

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

function buildTaskContext(records, userSlackId) {
  const lines = [];
  const person = Object.entries(TEAM).find(([, m]) => m.slackId === userSlackId)?.[1];

  // Person's own tasks first
  if (person) {
    const myTasks = records.filter(r => {
      const sc = STATUS_ACTIONS[r.status];
      if (!sc) return false;
      let owners = [];
      if (sc.role === "manager")    owners = r.manager    || [];
      if (sc.role === "copywriter") owners = r.copywriter || [];
      if (sc.role === "designer")   owners = r.designer   || [];
      if (sc.role === "uploader")   owners = r.uploader ? [r.uploader] : [];
      return owners.some(o => o.id && TEAM[o.id]?.slackId === userSlackId);
    });
    if (myTasks.length) {
      lines.push(`=== ${person.name}'s Current Tasks ===`);
      myTasks.forEach(r => {
        const sc = STATUS_ACTIONS[r.status];
        const urgDate = r[sc?.urgencyField] || r.sendDate;
        const days = daysUntilEST(urgDate);
        const daysStr = days === null ? "" : days < 0 ? ` (${Math.abs(days)}d OVERDUE)` : days === 0 ? " (due TODAY)" : ` (${days}d left)`;
        lines.push(`- ${r.name} | ${r.status} | ${sc?.action || ""}${daysStr}`);
      });
      lines.push("");
    }
  }

  // All active deliverables by client
  lines.push("=== All Active Deliverables ===");
  const byClient = {};
  records.forEach(r => { const c = r.client || "Unknown"; byClient[c] = byClient[c] || []; byClient[c].push(r); });
  Object.entries(byClient).forEach(([client, tasks]) => {
    lines.push(`
[${client}]`);
    tasks.forEach(r => {
      const days = daysUntilEST(r.sendDate);
      const daysStr = days === null ? "" : ` | Send: ${r.sendDate?.slice(0,10)}${days < 0 ? ` (${Math.abs(days)}d overdue)` : ` (${days}d)`}`;
      const people = [r.manager?.[0]?.name, r.copywriter?.[0]?.name, r.designer?.[0]?.name].filter(Boolean).join(", ");
      lines.push(`- ${r.name} | ${r.status}${daysStr} | Team: ${people}`);
    });
  });

  // Team workload summary
  lines.push("
=== Team Workload ===");
  Object.entries(TEAM).forEach(([id, member]) => {
    const owned = records.filter(r => {
      const sc = STATUS_ACTIONS[r.status];
      if (!sc) return false;
      let owners = [];
      if (sc.role === "manager")    owners = r.manager    || [];
      if (sc.role === "copywriter") owners = r.copywriter || [];
      if (sc.role === "designer")   owners = r.designer   || [];
      if (sc.role === "uploader")   owners = r.uploader ? [r.uploader] : [];
      return owners.some(o => o.id === id);
    });
    const overdue = owned.filter(r => {
      const sc = STATUS_ACTIONS[r.status];
      const d = daysUntilEST(r[sc?.urgencyField] || r.sendDate);
      return d !== null && d < 0;
    }).length;
    lines.push(`- ${member.name} (${member.role}): ${owned.length} active tasks${overdue > 0 ? `, ${overdue} overdue` : ""}`);
  });

  return lines.join("\n");
}

// ── GET /dashboard ───────────────────────────────────────────────
app.get("/dashboard", (req, res) => {
  res.sendFile(path.join(__dirname, "dashboard.html"));
});

app.listen(PORT, () => console.log(`Insendra server running on port ${PORT}`));
