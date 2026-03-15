const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;

// Get a fresh access token using the stored refresh token
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

// Fetch Gmail threads for a given email address
const fetchGmailThreads = async (accessToken, contactEmail, maxResults = 20) => {
  const query = encodeURIComponent(`from:${contactEmail} OR to:${contactEmail}`);
  const res = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/threads?q=${query}&maxResults=${maxResults}`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  const data = await res.json();
  return data.threads || [];
};

// Fetch full thread details
const fetchThread = async (accessToken, threadId) => {
  const res = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/threads/${threadId}?format=full`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  return await res.json();
};

// Decode base64 Gmail message body
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

// Upsert conversation and messages into Supabase
const upsertConversation = async (contactId, thread, userEmail) => {
  const msgs = thread.messages || [];
  if (msgs.length === 0) return;

  const firstMsg = msgs[0];
  const headers = firstMsg.payload?.headers || [];
  const subject = getHeader(headers, "subject") || "(no subject)";
  const lastMsg = msgs[msgs.length - 1];
  const lastHeaders = lastMsg.payload?.headers || [];
  const lastDate = new Date(parseInt(lastMsg.internalDate)).toISOString();

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
      updated_at: new Date().toISOString(),
    }),
  });

  let conv;
  const convData = await convRes.json();
  conv = Array.isArray(convData) ? convData[0] : convData;

  // If upsert did not return the row, fetch it by gmail_thread_id and fix contact_id
  if (!conv?.id) {
    const fetchRes = await fetch(
      `${SUPABASE_URL}/rest/v1/conversations?gmail_thread_id=eq.${thread.id}&select=id`,
      { headers: { "apikey": SUPABASE_SERVICE_KEY, "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}` } }
    );
    const rows = await fetchRes.json();
    conv = rows?.[0];
    if (conv?.id && contactId) {
      await fetch(`${SUPABASE_URL}/rest/v1/conversations?id=eq.${conv.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json", "apikey": SUPABASE_SERVICE_KEY, "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`, "Prefer": "" },
        body: JSON.stringify({ contact_id: contactId, last_message_at: lastDate }),
      });
    }
  }

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
      "Prefer": "",
    },
    body: JSON.stringify({ last_activity_date: lastDate.split("T")[0] }),
  });
};

export default async function handler(req, res) {
  // Allow manual trigger via POST or cron via GET
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  // Simple auth check for manual triggers
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

    // Get all contacts with email addresses
    let allContacts = [];
    let offset = 0;
    const PAGE = 1000;
    while (true) {
      const batch = await fetch(
        `${SUPABASE_URL}/rest/v1/contacts?email=not.is.null&select=id,email&limit=${PAGE}&offset=${offset}`,
        {
          headers: {
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
          },
        }
      );
      const data = await batch.json();
      if (!data || data.length === 0) break;
      allContacts = allContacts.concat(data);
      if (data.length < PAGE) break;
      offset += PAGE;
    }

    console.log(`Syncing Gmail for ${allContacts.length} contacts as ${user_email}`);

    let synced = 0;
    let errors = 0;

    for (const contact of allContacts) {
      try {
        const threads = await fetchGmailThreads(accessToken, contact.email);
        for (const thread of threads) {
          const full = await fetchThread(accessToken, thread.id);
          await upsertConversation(contact.id, full, user_email);
        }
        if (threads.length > 0) synced++;
        // Small delay to avoid Gmail rate limits
        await new Promise(r => setTimeout(r, 100));
      } catch (err) {
        console.error(`Error syncing ${contact.email}:`, err.message);
        errors++;
      }
    }

    return res.status(200).json({
      ok: true,
      total: allContacts.length,
      synced,
      errors,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Sync error:", err);
    return res.status(500).json({ error: err.message });
  }
}
