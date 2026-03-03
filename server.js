const express = require("express");
const cors    = require("cors");

const app  = express();
app.use(cors());
app.use(express.json());

// ── Config — set these as Environment Variables in Render ──────
const AT_TOKEN    = process.env.AT_TOKEN;
const AT_BASE     = process.env.AT_BASE     || "appdD2UGbFIfzkj7q";
const AT_TABLE    = process.env.AT_TABLE    || "tbltj1I38yoAh2HOF";
const SLACK_TOKEN = process.env.SLACK_TOKEN;

const AT_FIELDS = [
  "fld24h8Kt67qmofVj", // name
  "fldDZicZUOuhLPCci", // status
  "fldAyiQoNDwmH9Drz", // sendDate
  "fldN7RYkLdoRgPZK2", // copyDeadline
  "fldQPUf30fRmZjZ0N", // designDeadline
  "fldQq9vvc36vsC7ch", // uploadDeadline
  "fldrcjs3Htdxh90w9", // dueDate
  "fld08Y0CJeGD2P25d", // manager
  "fldKdh4HFcAxi6AIl", // copywriter
  "fld9nIa0KKkZDtpFy", // designer
  "fldCVsTVTGdmMMtxI", // uploader
  "fldc0vQSOERVGWFuk", // client
];

const ACTIVE_CLIENTS = [
  "CablesAndKits",
  "Chandler 4 Corners",
  "Safety Gear",
  "Fore & Wharf",
  "Eli Health",
];

// ── Health check ───────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({ status: "ok", service: "Insendra Proxy", version: "1.0.0" });
});

// ── GET /deliverables ──────────────────────────────────────────
app.get("/deliverables", async (req, res) => {
  try {
    const allRecords = [];
    let offset = null;

    const since = new Date();
    since.setDate(since.getDate() - 180);
    const sinceISO = since.toISOString().split("T")[0];

    const clientOr = ACTIVE_CLIENTS
      .map(c => `FIND("${c}", ARRAYJOIN({fldc0vQSOERVGWFuk}))`)
      .join(", ");


    do {
      const params = new URLSearchParams();
      AT_FIELDS.forEach(f => params.append("fields[]", f));
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
        const cr = f["fldc0vQSOERVGWFuk"];
        const client = Array.isArray(cr)
          ? (typeof cr[0] === "string" ? cr[0] : cr[0]?.name || "")
          : (cr || "");
        if (!ACTIVE_CLIENTS.includes(client)) return;

        const getDate = v => (!v || typeof v === "object") ? null : v;
        const getPpl  = v => Array.isArray(v) ? v.map(p => ({ id: p.id, name: p.name })) : [];

        allRecords.push({
          id:             r.id,
          name:           f["fld24h8Kt67qmofVj"] || "",
          status:         typeof f["fldDZicZUOuhLPCci"] === "object"
                            ? f["fldDZicZUOuhLPCci"]?.name || ""
                            : f["fldDZicZUOuhLPCci"] || "",
          sendDate:       getDate(f["fldAyiQoNDwmH9Drz"]),
          copyDeadline:   getDate(f["fldN7RYkLdoRgPZK2"]),
          designDeadline: getDate(f["fldQPUf30fRmZjZ0N"]),
          uploadDeadline: getDate(f["fldQq9vvc36vsC7ch"]),
          dueDate:        getDate(f["fldrcjs3Htdxh90w9"]),
          manager:        getPpl(f["fld08Y0CJeGD2P25d"]),
          copywriter:     getPpl(f["fldKdh4HFcAxi6AIl"]),
          designer:       getPpl(f["fld9nIa0KKkZDtpFy"]),
          uploader:       f["fldCVsTVTGdmMMtxI"]
                            ? { id: f["fldCVsTVTGdmMMtxI"].id, name: f["fldCVsTVTGdmMMtxI"].name }
                            : null,
          client,
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

// ── POST /send-dm ──────────────────────────────────────────────
// Body: { slackUserId, blocks, text }
app.post("/send-dm", async (req, res) => {
  try {
    const { slackUserId, blocks, text = "Insendra task update" } = req.body;

    const openRes = await fetch("https://slack.com/api/conversations.open", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify({ users: slackUserId }),
    });
    const openData = await openRes.json();
    if (!openData.ok) return res.status(400).json({ error: `conversations.open: ${openData.error}` });

    const msgRes = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify({ channel: openData.channel.id, text, blocks }),
    });
    const msgData = await msgRes.json();
    if (!msgData.ok) return res.status(400).json({ error: `chat.postMessage: ${msgData.error}` });

    res.json({ ok: true, ts: msgData.ts, channel: openData.channel.id });
  } catch (err) {
    console.error("/send-dm error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /send-channel ─────────────────────────────────────────
// Body: { channelId, blocks, text }
app.post("/send-channel", async (req, res) => {
  try {
    const { channelId, blocks, text = "Insendra digest" } = req.body;

    const msgRes = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SLACK_TOKEN}` },
      body: JSON.stringify({ channel: channelId, text, blocks }),
    });
    const msgData = await msgRes.json();
    if (!msgData.ok) return res.status(400).json({ error: `chat.postMessage: ${msgData.error}` });

    res.json({ ok: true, ts: msgData.ts });
  } catch (err) {
    console.error("/send-channel error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /slack/interactions ───────────────────────────────────
// Layer 3 — handles Slack button clicks (Mark as Done, etc.)
app.post("/slack/interactions", express.urlencoded({ extended: true }), async (req, res) => {
  try {
    const payload = JSON.parse(req.body.payload);
    const action  = payload.actions?.[0];
    if (!action) return res.status(400).send("No action");

    // Acknowledge immediately — Slack requires response within 3s
    res.status(200).send();

    const { action_id, value } = action;
    console.log(`Slack interaction: ${action_id} — ${value}`);

    // Layer 3 handlers go here, e.g.:
    // if (action_id === "mark_done") { await markRecordComplete(value); }
  } catch (err) {
    console.error("/slack/interactions error:", err);
    res.status(500).send("Error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Insendra server running on port ${PORT}`));
