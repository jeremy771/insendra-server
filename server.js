const express = require("express");
const cors    = require("cors");

const app  = express();
app.use(cors());
app.use(express.json());

const AT_TOKEN    = process.env.AT_TOKEN;
const AT_BASE     = process.env.AT_BASE  || "appdD2UGbFIfzkj7q";
const AT_TABLE    = process.env.AT_TABLE || "tbltj1I38yoAh2HOF";
const SLACK_TOKEN = process.env.SLACK_TOKEN;

const ACTIVE_CLIENTS = [
  "CablesAndKits",
  "Chandler 4 Corners",
  "Safety Gear",
  "Fore & Wharf",
  "Eli Health",
];

app.get("/", (req, res) => {
  res.json({ status: "ok", service: "Insendra Proxy", version: "1.2.0" });
});

app.get("/deliverables", async (req, res) => {
  try {
    const allRecords = [];
    let offset = null;
    do {
      const params = new URLSearchParams();
      params.set("pageSize", "100");
      if (offset) params.set("offset", offset);
      const atRes = await fetch(
        `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}?${params}`,
        { headers: { Authorization: `Bearer ${AT_TOKEN}` } }
      );
      if (!atRes.ok) {
        const err = await atRes.json().catch(() => ({}));
        return res.status(atRes.status).json({ error: err?.error?.message || `Airtable HTTP ${atRes.status}` });
      }
      const data = await atRes.json();
      data.records.forEach(r => {
        const f = r.fields;
        const status = f["Status"] || "";
        if (status === "Complete" || status === "") return;
        const clientRaw = f["Client Name (from Client Name)"];
        const client = Array.isArray(clientRaw) ? clientRaw[0] : (clientRaw || "");
        if (!ACTIVE_CLIENTS.includes(client)) return;
        const getPpl  = v => Array.isArray(v) ? v.map(p => ({ id: p.id, name: p.name })) : [];
        const getDate = v => (!v || typeof v === "object") ? null : v;
        const uploader = getPpl(f["Uploader"]);
        allRecords.push({
          id:             r.id,
          name:           f["Deliverable Name"] || "",
          createdTime:    r.createdTime || null,
          statusUpdated:  f["Status Updated"] || null,
          status,
          client,
          sendDate:       getDate(f["Send Date/Activation Date"]),
          copyDeadline:   getDate(f["\uD83D\uDCC5Copy Deadline"] || f["Copy Deadline"]),
          designDeadline: getDate(f["\uD83C\uDFA8Design Deadline"] || f["Design Deadline"]),
          uploadDeadline: getDate(f["\uD83D\uDCE4Klaviyo Upload Deadline"] || f["Klaviyo Upload Deadline"]),
          dueDate:        getDate(f["Due Date"]),
          manager:        getPpl(f["Manager"]),
          copywriter:     getPpl(f["Copywriter"]),
          designer:       getPpl(f["Designer"]),
          uploader:       uploader.length ? uploader[0] : null,
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

// ── POST /send-dm — supports both blocks and attachments ───────
app.post("/send-dm", async (req, res) => {
  try {
    const { slackUserId, blocks, attachments, text = "Insendra task update" } = req.body;
    const openRes = await fetch("https://slack.com/api/conversations.open", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify({ users: slackUserId }),
    });
    const openData = await openRes.json();
    if (!openData.ok) return res.status(400).json({ error: `conversations.open: ${openData.error}` });

    const payload = { channel: openData.channel.id, text };
    if (attachments) payload.attachments = attachments;
    if (blocks)      payload.blocks      = blocks;

    const msgRes = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify(payload),
    });
    const msgData = await msgRes.json();
    if (!msgData.ok) return res.status(400).json({ error: `chat.postMessage: ${msgData.error}` });
    res.json({ ok: true, ts: msgData.ts, channel: openData.channel.id });
  } catch (err) {
    console.error("/send-dm error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.post("/send-channel", async (req, res) => {
  try {
    const { channelId, blocks, attachments, text = "Insendra digest" } = req.body;
    const payload = { channel: channelId, text };
    if (attachments) payload.attachments = attachments;
    if (blocks)      payload.blocks      = blocks;
    const msgRes = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify(payload),
    });
    const msgData = await msgRes.json();
    if (!msgData.ok) return res.status(400).json({ error: `chat.postMessage: ${msgData.error}` });
    res.json({ ok: true, ts: msgData.ts });
  } catch (err) {
    console.error("/send-channel error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.post("/slack/interactions", express.urlencoded({ extended: true }), async (req, res) => {
  try {
    const payload = JSON.parse(req.body.payload);
    const action  = payload.actions?.[0];
    if (!action) return res.status(400).send("No action");
    res.status(200).send();
    console.log(`Slack interaction: ${action.action_id} — ${action.value}`);
  } catch (err) {
    console.error("/slack/interactions error:", err);
    res.status(500).send("Error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Insendra server running on port ${PORT}`));

// ── GET /comments/:recordId ────────────────────────────────────
app.get("/comments/:recordId", async (req, res) => {
  try {
    const { recordId } = req.params;
    const atRes = await fetch(
      `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}/${recordId}/comments`,
      { headers: { Authorization: `Bearer ${AT_TOKEN}` } }
    );
    if (!atRes.ok) {
      const err = await atRes.json().catch(() => ({}));
      return res.status(atRes.status).json({ error: err?.error?.message || `Airtable HTTP ${atRes.status}` });
    }
    const data = await atRes.json();
    res.json({ comments: data.comments || [] });
  } catch (err) {
    console.error("/comments error:", err);
    res.status(500).json({ error: err.message });
  }
});
