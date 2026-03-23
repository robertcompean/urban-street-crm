const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";
const SUPABASE_SERVICE_KEY = "sb_publishable_9whlg1wqquwmjgivsavs0A_H7HpbgE9";
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;

// How many contacts to process per cron run
const BATCH_SIZE = 150;
// Delay between Gmail API calls (ms)
const DELAY_MS = 300;

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

const fetchGmailThreads = async (accessToken, contactEmail, maxResults = 20) => {
  const query = encodeURIComponent(`from:${contactEmail} OR to:${contactEmail}`);
  const res = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/threads?q=${query}&maxResults=${maxResults}`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  if (res.status === 429) throw new Error("RATE_LIMIT");
  const data = await res.json();
  return data.threads || [];
};

const fetchThread = async (accessToken, threadId) => {
  const res = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/threads/${threadId}?format=full`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  if (res.status === 429) throw new Error("RATE_LIMIT");
  return await res.json();
};

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

const upsertConversation = async (contactId, thread, userEmail) => {
  const msgs = thread.messages || [];
  if (msgs.length === 0) return;

  const firstMsg = msgs[0];
  const headers = firstMsg.payload?.headers || [];
  const subject = getHeader(headers, "subject") || "(no subject)";
  const lastMsg = msgs[msgs.length - 1];
  const lastDate = new Date(parseInt(lastMsg.internalDate)).toISOString();
  const lastHeaders = lastMsg.payload?.headers || [];
  const lastFromEmail = getHeader(lastHeaders, "from");
  const lastDirection = lastFromEmail.toLowerCase().includes(userEmail.split("@")[0].toLowerCase()) ? "outbound" : "inbound";

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

  let conv;
  const convData = await convRes.json();
  conv = Array.isArray(convData) ? convData[0] : convData;

  if (!conv?.id) {
    const fetchRes = await fetch(
      `${SUPABASE_URL}/rest/v1/conversations?contact_id=eq.${contactId}&gmail_thread_id=eq.${thread.id}&select=id`,
      { headers: { "apikey": SUPABASE_SERVICE_KEY, "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}` } }
    );
    const rows = await fetchRes.json();
    conv = rows?.[0];
  }

  if (!conv?.id) return;

  for (const msg of msgs) {
    const mHeaders = msg.payload?.headers || [];
    const fromEmail = getHeader(mHeaders, "from");
    const sentAt = new Date(parseInt(msg.internalDate)).toISOString();
    const bodyText = decodeBody(msg.payload).slice(0, 4000);
    const direction = fromEmail.toLowerCase().includes(userEmail.split("@")[0].toLowerCase()) ? "outbound" : "inbound";

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

const updateCursor = async (tokenId, newOffset) => {
  await fetch(`${SUPABASE_URL}/rest/v1/gmail_tokens?id=eq.${tokenId}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_SERVICE_KEY,
      "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
      "Prefer": "",
    },
    body: JSON.stringify({ sync_offset: newOffset }),
  });
};

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
    // Get token + cursor
    const tokenRes = await fetch(`${SUPABASE_URL}/rest/v1/gmail_tokens?select=*&limit=1`, {
      headers: {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
      },
    });
    const tokens = await tokenRes.json();
    if (!tokens || tokens.length === 0) {
      return res.status(400).json({ error: "No Gmail token found." });
    }

    const { id: tokenId, refresh_token, user_email, sync_offset } = tokens[0];
    const currentOffset = sync_offset || 0;
    const accessToken = await refreshAccessToken(refresh_token);

    // Count total contacts
    const countRes = await fetch(
      `${SUPABASE_URL}/rest/v1/contacts?email=not.is.null&select=id&limit=1`,
      {
        headers: {
          "apikey": SUPABASE_SERVICE_KEY,
          "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
          "Prefer": "count=exact",
          "Range-Unit": "items",
          "Range": "0-0",
        },
      }
    );
    const totalCount = parseInt(countRes.headers.get("content-range")?.split("/")[1] || "0");

    // Fetch this batch
    const batchRes = await fetch(
      `${SUPABASE_URL}/rest/v1/contacts?email=not.is.null&select=id,email&limit=${BATCH_SIZE}&offset=${currentOffset}&order=id.asc`,
      {
        headers: {
          "apikey": SUPABASE_SERVICE_KEY,
          "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
        },
      }
    );
    const batch = await batchRes.json();

    if (!batch || batch.length === 0) {
      // Reached the end — reset cursor to 0
      await updateCursor(tokenId, 0);
      return res.status(200).json({ ok: true, message: "Sync complete, cursor reset", total: totalCount });
    }

    console.log(`Syncing contacts ${currentOffset}–${currentOffset + batch.length} of ${totalCount}`);

    let synced = 0;
    let errors = 0;
    let rateLimited = false;

    for (const contact of batch) {
      if (rateLimited) break;
      try {
        const threads = await fetchGmailThreads(accessToken, contact.email);
        for (const thread of threads) {
          const full = await fetchThread(accessToken, thread.id);
          await upsertConversation(contact.id, full, user_email);
          await new Promise(r => setTimeout(r, DELAY_MS));
        }
        if (threads.length > 0) synced++;
        await new Promise(r => setTimeout(r, DELAY_MS));
      } catch (err) {
        if (err.message === "RATE_LIMIT") {
          console.warn(`Rate limited at contact ${contact.email}, stopping batch`);
          rateLimited = true;
        } else {
          console.error(`Error syncing ${contact.email}:`, err.message);
          errors++;
        }
      }
    }

    // Advance cursor (or reset if end reached)
    const newOffset = currentOffset + batch.length >= totalCount ? 0 : currentOffset + batch.length;
    await updateCursor(tokenId, newOffset);

    return res.status(200).json({
      ok: true,
      batch_start: currentOffset,
      batch_size: batch.length,
      synced,
      errors,
      rate_limited: rateLimited,
      next_offset: newOffset,
      total: totalCount,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Sync error:", err);
    return res.status(500).json({ error: err.message });
  }
}
