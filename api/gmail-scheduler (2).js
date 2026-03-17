/**
 * api/gmail-scheduler.js
 * Vercel cron handler — runs once daily via vercel.json
 * Executes pending scheduled_sends batches within their send window.
 *
 * scheduled_sends row shape:
 *   { id, name, mode, config, recipient_ids, schedule, status,
 *     sends_completed, next_send_at, created_at }
 *
 * schedule object:
 *   { start_date, daily_limit, window_start, window_end, send_days, timezone }
 *
 * config object (varies by mode):
 *   template: { template_id, template_name }
 *   compose:  { subject, body, sig_id }
 *   blast:    { subject, html, from_name }
 */

const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const RESEND_KEY    = process.env.RESEND_API_KEY;
const UNSUB_DOMAIN  = process.env.UNSUB_DOMAIN || "https://urbansv.vercel.app";
const TZ            = process.env.SEND_TIMEZONE || "America/Los_Angeles";
const CRON_SECRET   = process.env.CRON_SECRET;

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

// ── Supabase REST helpers ─────────────────────────────────────────────────────

async function supa(path, opts = {}) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1${path}`, {
    ...opts,
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_KEY,
      "Authorization": `Bearer ${SUPABASE_KEY}`,
      "Prefer": opts.method === "POST" ? "return=representation" : "",
      ...(opts.headers || {}),
    },
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase ${path}: ${res.status} ${err}`);
  }
  const ct = res.headers.get("content-type") || "";
  return ct.includes("application/json") ? res.json() : null;
}

// ── Gmail token refresh ───────────────────────────────────────────────────────

async function getGmailAccessToken() {
  const rows = await supa("/oauth_tokens?provider=eq.gmail&select=refresh_token,access_token,expires_at&limit=1");
  const stored = rows?.[0];
  if (!stored?.refresh_token) throw new Error("No Gmail refresh token stored. Connect Gmail via the CRM first.");

  const expiresAt = stored.expires_at ? new Date(stored.expires_at) : null;
  if (expiresAt && expiresAt.getTime() - Date.now() > 5 * 60 * 1000) {
    return stored.access_token;
  }

  const params = new URLSearchParams({
    client_id: process.env.GOOGLE_CLIENT_ID,
    client_secret: process.env.GOOGLE_CLIENT_SECRET,
    refresh_token: stored.refresh_token,
    grant_type: "refresh_token",
  });
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: params.toString(),
  });
  const data = await res.json();
  if (!data.access_token) throw new Error("Token refresh failed: " + JSON.stringify(data));

  const newExpiry = new Date(Date.now() + data.expires_in * 1000).toISOString();
  await supa("/oauth_tokens?provider=eq.gmail", {
    method: "PATCH",
    body: JSON.stringify({ access_token: data.access_token, expires_at: newExpiry }),
    headers: { Prefer: "" },
  });

  return data.access_token;
}

// ── Merge fields ─────────────────────────────────────────────────────────────

function applyMerge(text, contact) {
  if (!text) return "";
  const cap = s => s ? s.charAt(0).toUpperCase() + s.slice(1) : "";
  return text
    .replace(/{{first_name}}/gi, cap(contact.first_name) || "")
    .replace(/{{last_name}}/gi, cap(contact.last_name) || "")
    .replace(/{{full_name}}/gi, `${cap(contact.first_name) || ""} ${cap(contact.last_name) || ""}`.trim())
    .replace(/{{email}}/gi, contact.email || "")
    .replace(/{{company}}/gi, contact.company || "")
    .replace(/{{city}}/gi, contact.city || "")
    .replace(/{{state}}/gi, contact.state || "")
    .replace(/{{id}}/gi, contact.id || "")
    .replace(/{{unsub_url}}/gi, `${UNSUB_DOMAIN}/api/unsubscribe?id=${contact.id}`);
}

function unsubFooter(contactId) {
  return `<div style="margin-top:32px;padding-top:16px;border-top:1px solid #e2e2e5;font-family:Arial,sans-serif;font-size:11px;color:#999;text-align:center;">
    You received this because you are in Urban Street Ventures' contact list.
    <a href="${UNSUB_DOMAIN}/api/unsubscribe?id=${contactId}" style="color:#999;">Unsubscribe</a>
  </div>`;
}

// ── Gmail send (RFC 2822 via API) ─────────────────────────────────────────────

async function gmailSend({ accessToken, to, subject, bodyText, bodyHtml }) {
  const userRes = await fetch("https://www.googleapis.com/gmail/v1/users/me/profile", {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  const { emailAddress } = await userRes.json();

  const boundary = `usv_${Date.now()}`;
  const mime = [
    `From: Urban Street Ventures <${emailAddress}>`,
    `To: ${to}`,
    `Subject: ${subject}`,
    `MIME-Version: 1.0`,
    `Content-Type: multipart/alternative; boundary="${boundary}"`,
    ``,
    `--${boundary}`,
    `Content-Type: text/plain; charset="UTF-8"`,
    ``,
    bodyText || "",
    ``,
    `--${boundary}`,
    `Content-Type: text/html; charset="UTF-8"`,
    ``,
    bodyHtml || `<pre>${bodyText || ""}</pre>`,
    ``,
    `--${boundary}--`,
  ].join("\r\n");

  const encoded = Buffer.from(mime).toString("base64url");
  const res = await fetch("https://www.googleapis.com/gmail/v1/users/me/messages/send", {
    method: "POST",
    headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ raw: encoded }),
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error("Gmail send failed: " + JSON.stringify(err));
  }
  return res.json();
}

// ── Resend blast send ─────────────────────────────────────────────────────────

async function resendSend({ to, subject, html, fromName }) {
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { Authorization: `Bearer ${RESEND_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      from: `${fromName || "Urban Street Ventures"} <noreply@urbansv.com>`,
      to,
      subject,
      html,
    }),
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error("Resend failed: " + JSON.stringify(err));
  }
  return res.json();
}

// ── Log to conversations + messages ──────────────────────────────────────────

async function logToSupabase({ contactId, subject, bodyText, fromEmail, gmailMsgId, gmailThreadId }) {
  try {
    let convId;
    if (gmailThreadId) {
      const existing = await supa(`/conversations?gmail_thread_id=eq.${gmailThreadId}&select=id`);
      if (existing?.length) {
        convId = existing[0].id;
        await supa(`/conversations?id=eq.${convId}`, {
          method: "PATCH",
          headers: { Prefer: "" },
          body: JSON.stringify({ last_message_at: new Date().toISOString(), last_direction: "outbound", snippet: (bodyText || "").slice(0, 120) }),
        });
      }
    }
    if (!convId) {
      const rows = await supa(`/conversations?contact_id=eq.${contactId}&subject=eq.${encodeURIComponent(subject)}&select=id`);
      if (rows?.length) {
        convId = rows[0].id;
        await supa(`/conversations?id=eq.${convId}`, {
          method: "PATCH",
          headers: { Prefer: "" },
          body: JSON.stringify({ last_message_at: new Date().toISOString(), last_direction: "outbound" }),
        });
      } else {
        const created = await supa("/conversations", {
          method: "POST",
          body: JSON.stringify({
            contact_id: contactId,
            subject,
            snippet: (bodyText || "").slice(0, 120),
            last_message_at: new Date().toISOString(),
            last_direction: "outbound",
            unread: false,
            gmail_thread_id: gmailThreadId || null,
          }),
        });
        convId = (Array.isArray(created) ? created[0] : created)?.id;
      }
    }
    if (convId) {
      await supa("/messages", {
        method: "POST",
        body: JSON.stringify({
          conversation_id: convId,
          direction: "outbound",
          from_email: fromEmail,
          subject,
          body_text: bodyText,
          sent_at: new Date().toISOString(),
          gmail_message_id: gmailMsgId || null,
        }),
      });
    }
    await supa(`/contacts?id=eq.${contactId}`, {
      method: "PATCH",
      headers: { Prefer: "" },
      body: JSON.stringify({ last_activity_date: new Date().toISOString().slice(0, 10) }),
    });
  } catch (e) {
    console.warn("Logging error (non-fatal):", e.message);
  }
}

// ── Time/day helpers ──────────────────────────────────────────────────────────

function getTZParts(date, tz) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", weekday: "short",
    hour12: false,
  });
  const parts = fmt.formatToParts(date);
  const get = type => parts.find(p => p.type === type)?.value;
  return {
    year: get("year"),
    month: get("month"),
    day: get("day"),
    hour: get("hour")?.padStart(2, "0"),
    minute: get("minute")?.padStart(2, "0"),
    weekday: get("weekday"), // "Mon", "Tue", etc.
  };
}

function isWithinWindow(windowStart, windowEnd, tz) {
  const { hour, minute } = getTZParts(new Date(), tz);
  const nowTime = `${hour}:${minute}`;
  return nowTime >= windowStart && nowTime <= windowEnd;
}

function isSendDay(sendDays, tz) {
  // sendDays is array like ["Mon","Tue","Wed","Thu","Fri"]
  if (!sendDays || !sendDays.length) return true; // no restriction = always send
  const { weekday } = getTZParts(new Date(), tz);
  return sendDays.includes(weekday);
}

function nextSendAt(windowStart, sendDays, tz) {
  // Find the next valid send day starting from tomorrow
  const now = new Date();
  for (let daysAhead = 1; daysAhead <= 14; daysAhead++) {
    const candidate = new Date(now.getTime() + daysAhead * 24 * 60 * 60 * 1000);
    const { weekday, year, month, day } = getTZParts(candidate, tz);
    if (!sendDays || !sendDays.length || sendDays.includes(weekday)) {
      return new Date(`${year}-${month}-${day}T${windowStart}:00`).toISOString();
    }
  }
  // Fallback: tomorrow
  const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);
  const { year, month, day } = getTZParts(tomorrow, tz);
  return new Date(`${year}-${month}-${day}T${windowStart}:00`).toISOString();
}

// ── Main handler ─────────────────────────────────────────────────────────────

export default async function handler(req, res) {
  if (CRON_SECRET && req.headers["x-cron-secret"] !== CRON_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const results = [];
  const now = new Date().toISOString();

  try {
    const pending = await supa(
      `/scheduled_sends?status=eq.scheduled&next_send_at=lte.${encodeURIComponent(now)}&select=*`
    );

    if (!pending?.length) {
      return res.status(200).json({ message: "No scheduled sends due.", results: [] });
    }

    let accessToken = null;
    try { accessToken = await getGmailAccessToken(); } catch (e) {
      console.warn("Gmail token unavailable:", e.message);
    }

    for (const job of pending) {
      const jobResult = { id: job.id, name: job.name, sent: 0, skipped: 0, errors: [] };
      try {
        const schedule = job.schedule || {};
        const {
          daily_limit = 50,
          window_start = "09:00",
          window_end = "17:00",
          send_days = [],
          timezone = TZ,
        } = schedule;

        const tz = timezone || TZ;

        // Check if today is a valid send day
        if (!isSendDay(send_days, tz)) {
          jobResult.skipped = `Not a send day (today is ${getTZParts(new Date(), tz).weekday}, send days: ${send_days.join(", ")})`;
          results.push(jobResult);
          await supa(`/scheduled_sends?id=eq.${job.id}`, {
            method: "PATCH",
            headers: { Prefer: "" },
            body: JSON.stringify({ next_send_at: nextSendAt(window_start, send_days, tz) }),
          });
          continue;
        }

        // Check send window
        if (!isWithinWindow(window_start, window_end, tz)) {
          jobResult.skipped = "Outside send window";
          results.push(jobResult);
          continue;
        }

        // Determine batch
        const recipientIds = job.recipient_ids || [];
        const startIdx = job.sends_completed || 0;
        if (startIdx >= recipientIds.length) {
          await supa(`/scheduled_sends?id=eq.${job.id}`, {
            method: "PATCH",
            headers: { Prefer: "" },
            body: JSON.stringify({ status: "completed", completed_at: new Date().toISOString() }),
          });
          jobResult.skipped = "All recipients done — marked complete";
          results.push(jobResult);
          continue;
        }

        const batchIds = recipientIds.slice(startIdx, startIdx + daily_limit);

        const contacts = await supa(
          `/contacts?id=in.(${batchIds.join(",")})&select=id,first_name,last_name,email,company,city,state,subscription_status,email_status`
        );
        const contactMap = {};
        (contacts || []).forEach(c => { contactMap[c.id] = c; });

        let template = null;
        if (job.mode === "template" && job.config?.template_id) {
          const tmplRows = await supa(`/templates?id=eq.${job.config.template_id}&select=*`);
          template = tmplRows?.[0] || null;
        }
        let signature = null;
        if (template?.signature_id || job.config?.sig_id) {
          const sigId = template?.signature_id || job.config.sig_id;
          const sigRows = await supa(`/signatures?id=eq.${sigId}&select=html_output`);
          signature = sigRows?.[0] || null;
        }

        for (const cid of batchIds) {
          const contact = contactMap[cid];
          if (!contact?.email) { jobResult.errors.push(`${cid}: no email`); continue; }

          const emailStatus = String(contact.email_status || "").toLowerCase();
          const subStatus = String(contact.subscription_status || "").toLowerCase();
          if (["bounced", "complained", "unsubscribed"].includes(emailStatus) ||
              subStatus === "false" || subStatus === "unsubscribed") {
            jobResult.skipped++;
            continue;
          }

          try {
            if (job.mode === "blast") {
              const subj = applyMerge(job.config.subject, contact);
              const html = applyMerge(job.config.html, contact) + unsubFooter(contact.id);
              await resendSend({ to: contact.email, subject: subj, html, fromName: job.config.from_name });
              await logToSupabase({ contactId: contact.id, subject: subj, bodyText: "", fromEmail: "noreply@urbansv.com" });
            } else {
              if (!accessToken) throw new Error("Gmail not connected");
              const rawSubject = job.mode === "compose" ? job.config.subject : (template?.subject || template?.name || "");
              const rawBody = job.mode === "compose" ? job.config.body : (template?.body || "");
              const subject = applyMerge(rawSubject, contact);
              const bodyText = applyMerge(rawBody, contact);
              const sigHtml = signature?.html_output || "";
              const bodyHtml = `<div style="white-space:pre-wrap;font-family:Arial,sans-serif;font-size:14px;">${bodyText}</div>${sigHtml}`;

              const sent = await gmailSend({ accessToken, to: contact.email, subject, bodyText, bodyHtml });
              await logToSupabase({
                contactId: contact.id,
                subject,
                bodyText,
                fromEmail: "me",
                gmailMsgId: sent.id,
                gmailThreadId: sent.threadId,
              });
            }
            jobResult.sent++;
          } catch (e) {
            jobResult.errors.push(`${contact.email}: ${e.message}`);
          }
        }

        const newCompleted = startIdx + batchIds.length;
        const allDone = newCompleted >= recipientIds.length;
        await supa(`/scheduled_sends?id=eq.${job.id}`, {
          method: "PATCH",
          headers: { Prefer: "" },
          body: JSON.stringify({
            sends_completed: newCompleted,
            status: allDone ? "completed" : "scheduled",
            next_send_at: allDone ? null : nextSendAt(window_start, send_days, tz),
            ...(allDone ? { completed_at: new Date().toISOString() } : {}),
          }),
        });

      } catch (e) {
        jobResult.errors.push("Job error: " + e.message);
        console.error("Scheduler job error:", e);
      }
      results.push(jobResult);
    }

    return res.status(200).json({ processed: pending.length, results });
  } catch (e) {
    console.error("Scheduler fatal error:", e);
    return res.status(500).json({ error: e.message });
  }
}
