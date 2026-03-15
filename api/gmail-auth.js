const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const APP_URL = "https://urban-street-crm-si8d.vercel.app";

export default async function handler(req, res) {
  const { code, error } = req.query;

  if (error) {
    return res.redirect(`${APP_URL}?gmail_error=${encodeURIComponent(error)}`);
  }

  if (!code) {
    return res.status(400).send("Missing authorization code");
  }

  try {
    // Exchange code for tokens
    const tokenRes = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        code,
        client_id: GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_CLIENT_SECRET,
        redirect_uri: `${APP_URL}/api/gmail-auth`,
        grant_type: "authorization_code",
      }),
    });

    const tokens = await tokenRes.json();
    if (!tokenRes.ok || !tokens.refresh_token) {
      console.error("Token exchange failed:", tokens);
      return res.redirect(`${APP_URL}?gmail_error=token_exchange_failed`);
    }

    // Get user email
    const profileRes = await fetch("https://www.googleapis.com/oauth2/v2/userinfo", {
      headers: { Authorization: `Bearer ${tokens.access_token}` },
    });
    const profile = await profileRes.json();
    const userEmail = profile.email;

    // Store refresh token in Supabase
    const upsertRes = await fetch(`${SUPABASE_URL}/rest/v1/gmail_tokens`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
        "Prefer": "resolution=merge-duplicates",
      },
      body: JSON.stringify({
        user_email: userEmail,
        refresh_token: tokens.refresh_token,
        access_token: tokens.access_token,
        token_expiry: new Date(Date.now() + tokens.expires_in * 1000).toISOString(),
        updated_at: new Date().toISOString(),
      }),
    });

    if (!upsertRes.ok) {
      const err = await upsertRes.text();
      console.error("Supabase upsert failed:", err);
      return res.redirect(`${APP_URL}?gmail_error=storage_failed`);
    }

    // Redirect back to app with success
    return res.redirect(`${APP_URL}?gmail_connected=${encodeURIComponent(userEmail)}`);
  } catch (err) {
    console.error("Gmail auth error:", err);
    return res.redirect(`${APP_URL}?gmail_error=${encodeURIComponent(err.message)}`);
  }
}
