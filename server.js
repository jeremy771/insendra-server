// ── Performance logging ──────────────────────────────────────────
const fs = require("fs");
const PERF_LOG = "./performance-log.json";
const SNAPSHOT  = "./status-snapshot.json";

function readJSON(path, fallback) {
  try { return JSON.parse(fs.readFileSync(path, "utf8")); }
  catch { return fallback; }
}
function writeJSON(path, data) {
  fs.writeFileSync(path, JSON.stringify(data, null, 2));
}

const PREV_STATUS_ROLES = {
  "Ready for Copy":                     { role:"copywriter", urgencyField:"copyDeadline" },
  "Copywriting in Progress":            { role:"copywriter", urgencyField:"copyDeadline" },
  "Copywriting Complete - Ready for QA":{ role:"copywriter", urgencyField:"copyDeadline" },
  "Ready For Design":                   { role:"designer",   urgencyField:"designDeadline" },
  "Design in Progress":                 { role:"designer",   urgencyField:"designDeadline" },
  "Design Complete - Ready for QA":     { role:"designer",   urgencyField:"designDeadline" },
  "Client Review":                      { role:"manager",    urgencyField:"dueDate" },
  "Upload":                             { role:"uploader",   urgencyField:"uploadDeadline" },
  "Schedule":                           { role:"manager",    urgencyField:"sendDate" },
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

// ── GET /performance ─────────────────────────────────────────────
app.get("/performance", (req, res) => {
  res.json({ entries: readJSON(PERF_LOG, []) });
});

// ── POST /snapshot ───────────────────────────────────────────────
app.post("/snapshot", async (req, res) => {
  try {
    const atRes = await fetch(
      `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}?pageSize=100`,
      { headers: { Authorization: `Bearer ${AT_TOKEN}` } }
    );
    const data = await atRes.json();
    const getDate = v => v ? (Array.isArray(v) ? v[0] : v) : null;
    const getPpl  = v => (v||[]).map(p => ({ id: p.id, name: p.name }));
    const records = (data.records||[]).map(r => {
      const f = r.fields;
      const upArr = getPpl(f["Uploader"]);
      return {
        id: r.id, name: f["Deliverable Name"]||"", status: f["Status"]||"",
        statusUpdated:  f["Status Updated"]||null,
        copyDeadline:   getDate(f["Copy Deadline"]),
        designDeadline: getDate(f["Design Deadline"]),
        uploadDeadline: getDate(f["Klaviyo Upload Deadline"]),
        dueDate:        getDate(f["Due Date"]),
        sendDate:       getDate(f["Send Date/Activation Date"]),
        manager:        getPpl(f["Manager"]),
        copywriter:     getPpl(f["Copywriter"]),
        designer:       getPpl(f["Designer"]),
        uploader:       upArr.length ? upArr[0] : null,
        client:         Array.isArray(f["Client Name"]) ? f["Client Name"][0] : f["Client Name"]||"",
      };
    });
    updatePerformanceLog(records);
    res.json({ ok: true, records: records.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
