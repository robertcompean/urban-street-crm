const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const sb = async (path, options = {}) => {
  const res = await fetch(`${SUPABASE_URL}/rest/v1${path}`, {
    ...options,
    headers: {
      apikey: SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=representation",
      ...(options.headers || {}),
    },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status}: ${text}`);
  return text ? JSON.parse(text) : null;
};

export default async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).end();

  try {
    const event = req.body;
    const type = event?.type;

    // Only process hard bounces
    if (type !== "email.bounced") {
      return res.status(200).json({ received: true, action: "ignored" });
    }

    const email = event?.data?.to?.[0] || event?.data?.email_id;
    const bounceType = event?.data?.bounce?.type; // "hard" or "soft"

    // Ignore soft bounces
    if (bounceType !== "hard") {
      return res.status(200).json({ received: true, action: "soft_bounce_ignored" });
    }

    if (!email) {
      return res.status(200).json({ received: true, action: "no_email_found" });
    }

    // Find contact by email
    const contacts = await sb(`/contacts?email=ilike.${encodeURIComponent(email)}&select=id,email`);
    if (!contacts?.length) {
      console.log(`Hard bounce for unknown email: ${email}`);
      return res.status(200).json({ received: true, action: "contact_not_found" });
    }

    const contactId = contacts[0].id;

    // Mark as invalid and unsubscribed
    await sb(`/contacts?id=eq.${contactId}`, {
      method: "PATCH",
      body: JSON.stringify({
        email_status: "Invalid",
        subscription_status: "Unsubscribed",
      }),
    });

    console.log(`Hard bounce processed: ${email} (id: ${contactId}) marked Invalid + Unsubscribed`);
    return res.status(200).json({ received: true, action: "marked_invalid", contactId });

  } catch (err) {
    console.error("Resend webhook error:", err.message);
    // Always return 200 to Resend so it doesn't retry
    return res.status(200).json({ received: true, error: err.message });
  }
}
