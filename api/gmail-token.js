const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;

export default async function handler(req, res) {
  if (req.method !== "GET") return res.status(405).end();

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
      return res.status(404).json({ error: "No Gmail token found" });
    }

    const { refresh_token, user_email } = tokens[0];

    // Get fresh access token
    const refreshRes = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        refresh_token,
        client_id: GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_CLIENT_SECRET,
        grant_type: "refresh_token",
      }),
    });

    const data = await refreshRes.json();
    if (!refreshRes.ok) {
      return res.status(401).json({ error: "Token refresh failed", detail: data.error });
    }

    // Update stored access token
    await fetch(`${SUPABASE_URL}/rest/v1/gmail_tokens?user_email=eq.${encodeURIComponent(user_email)}`, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
        "Prefer": "",
      },
      body: JSON.stringify({
        access_token: data.access_token,
        token_expiry: new Date(Date.now() + data.expires_in * 1000).toISOString(),
        updated_at: new Date().toISOString(),
      }),
    });

    return res.status(200).json({
      access_token: data.access_token,
      user_email,
      expires_in: data.expires_in,
    });
  } catch (err) {
    console.error("gmail-token error:", err);
    return res.status(500).json({ error: err.message });
  }
}
