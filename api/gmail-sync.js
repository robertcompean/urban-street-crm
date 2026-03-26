const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;

const BATCH_SIZE = 5;        // contacts processed in parallel
const MAX_THREADS = 10;      // threads fetched per contact
const TIMEOUT_MS = 250_000;  // stop at 250s, leave buffer for Vercel 300s limit

// ─── Auth ────────────────────────────────────────────────────────────────────

const refreshAccessToken = async (refreshToken) => {
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      refresh_token: refreshToken,
      client_id: GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_CLIENT_SECRET,
      grant_type: "refresh_token",
    }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Token refresh failed: ${data.error}`);
  return data.access_token;
};

// ─── Gmail API ───────────────────────────────────────────────────────────────

const fetchGmailThreads = async (accessToken, contactEmail, maxResults = MAX_THREADS) => {
  const query = encodeURIComponent(`from:${contactEmail} OR to:${contactEmail}`);
  const res = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/threads?q=${query}&maxResults=${maxResults}`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  const data = await res.json();
  return data.threads || [];
};

const fetchThread = async (accessToken, threadId) => {
  const res = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/threads/${threadId}?format=full`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  return await res.json();
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

const decodeBody = (part) => {
  if (!part) return "";
  if (part.body?.data) {
    return Buffer.from(part.body.data, "base64url").toString("utf-8");
  }
  if (part.parts) {
    for (const p of part.parts) {
      const text = decodeBody(p);
      if (text) return text;
    }
  }
  return "";
};

const getHeader = (headers, name) =>
  headers?.find(h => h.name.toLowerCase() === name.toLowerCase())?.value || "";

// ─── Supabase writes ──────────────────────────────────────────────────────────

const upsertConversation = async (contactId, thread, userEmail) => {
  const msgs = thread.messages || [];
  if (msgs.length === 0) return;

  const firstMsg = msgs[0];
  const headers = firstMsg.payload?.headers || [];
  const subject = getHeader(headers, "subject") || "(no subject)";
  const lastMsg = msgs[msgs.length - 1];
  const lastHeaders = lastMsg.payload?.headers || [];
  const lastDate = new Date(parseInt(lastMsg.internalDate)).toISOString();

  // Determine direction of most recent message
  const lastFrom = getHeader(lastHeaders, "from");
  const lastDirection = lastFrom.includes(userEmail) ? "outbound" : "inbound";

  // Upsert conversation
  const convRes = await fetch(`${SUPABASE_URL}/rest/v1/conversations`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_SERVICE_KEY,
      "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
      "Prefer": "resolution=merge-duplicates,return=representation",
    },
    body: JSON.stringify({
      contact_id: contactId,
      subject,
      gmail_thread_id: thread.id,
      last_message_at: lastDate,
      last_direction: lastDirection,
      updated_at: new Date().toISOString(),
    }),
  });

  const convData = await convRes.json();
  const conv = Array.isArray(convData) ? convData[0] : convData;
  if (!conv?.id) return;

  // Upsert each message
  for (const msg of msgs) {
    const mHeaders = msg.payload?.headers || [];
    const fromEmail = getHeader(mHeaders, "from");
    const sentAt = new Date(parseInt(msg.internalDate)).toISOString();
    const bodyText = decodeBody(msg.payload).slice(0, 4000);
    const direction = fromEmail.includes(userEmail) ? "outbound" : "inbound";

    await fetch(`${SUPABASE_URL}/rest/v1/messages`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
        "Prefer": "resolution=merge-duplicates",
      },
      body: JSON.stringify({
        conversation_id: conv.id,
        gmail_message_id: msg.id,
        direction,
        from_email: fromEmail,
        subject: getHeader(mHeaders, "subject") || subject,
        body_text: bodyText,
        sent_at: sentAt,
      }),
    });
  }

  // Update contact last_activity_date
  await fetch(`${SUPABASE_URL}/rest/v1/contacts?id=eq.${contactId}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_SERVICE_KEY,
      "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
    },
    body: JSON.stringify({ last_activity_date: lastDate.split("T")[0] }),
  });
};

// After all threads for a contact are processed:
// - query the most recent conversation's last_direction
// - write last_email_direction + gmail_synced_at back to contacts
const updateContactSyncMeta = async (contactId) => {
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/conversations?contact_id=eq.${contactId}&order=last_message_at.desc&limit=1&select=last_direction`,
    {
      headers: {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
      },
    }
  );
  const data = await res.json();
  const lastEmailDirection = data?.[0]?.last_direction ?? null;

  await fetch(`${SUPABASE_URL}/rest/v1/contacts?id=eq.${contactId}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_SERVICE_KEY,
      "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
    },
    body: JSON.stringify({
      gmail_synced_at: new Date().toISOString(),
      last_email_direction: lastEmailDirection,
    }),
  });
};

// ─── Handler ──────────────────────────────────────────────────────────────────

export default async function handler(req, res) {
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const authHeader = req.headers.authorization;
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  try {
    // Get stored refresh token
    const tokenRes = await fetch(`${SUPABASE_URL}/rest/v1/gmail_tokens?select=*&limit=1`, {
      headers: {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
      },
    });
    const tokens = await tokenRes.json();
    if (!tokens || tokens.length === 0) {
      return res.status(400).json({ error: "No Gmail token found. Connect Gmail first." });
    }

    const { refresh_token, user_email } = tokens[0];
    const accessToken = await refreshAccessToken(refresh_token);

    // Fetch sync-eligible contacts via DB function (scored, recently active, or
    // previously emailed) — ordered stalest gmail_synced_at first.
    const eligibleRes = await fetch(`${SUPABASE_URL}/rest/v1/rpc/get_sync_eligible_contacts`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
      },
      body: JSON.stringify({}),
    });
    const allContacts = await eligibleRes.json();
    if (!Array.isArray(allContacts)) {
      return res.status(500).json({ error: "Failed to fetch eligible contacts", detail: allContacts });
    }

    console.log(`Syncing Gmail for ${allContacts.length} eligible contacts (stalest first) as ${user_email}`);

    const startTime = Date.now();
    let synced = 0;
    let errors = 0;
    let processed = 0;

    // Process in parallel batches of BATCH_SIZE
    for (let i = 0; i < allContacts.length; i += BATCH_SIZE) {
      // Hard stop before Vercel kills us
      if (Date.now() - startTime > TIMEOUT_MS) {
        console.log(`Timeout reached after processing ${processed} contacts — stopping cleanly.`);
        break;
      }

      const chunk = allContacts.slice(i, i + BATCH_SIZE);

      await Promise.all(chunk.map(async (contact) => {
        try {
          // Fetch thread list, then all thread details in parallel
          const threads = await fetchGmailThreads(accessToken, contact.email);
          if (threads.length > 0) {
            const fullThreads = await Promise.all(
              threads.map(t => fetchThread(accessToken, t.id))
            );
            for (const full of fullThreads) {
              await upsertConversation(contact.id, full, user_email);
            }
            synced++;
          }
          // Always stamp gmail_synced_at so this contact moves to back of queue
          await updateContactSyncMeta(contact.id);
        } catch (err) {
          console.error(`Error syncing ${contact.email}:`, err.message);
          errors++;
        }
      }));

      processed += chunk.length;

      // Brief pause between batches to respect Gmail rate limits
      await new Promise(r => setTimeout(r, 150));
    }

    const elapsed = Math.round((Date.now() - startTime) / 1000);
    return res.status(200).json({
      ok: true,
      total: allContacts.length,
      processed,
      synced,
      errors,
      elapsed_seconds: elapsed,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Sync error:", err);
    return res.status(500).json({ error: err.message });
  }
}
