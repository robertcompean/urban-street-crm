export default async function handler(req, res) {
  // Only allow POST
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const RESEND_API_KEY = "re_g6Jjwwvt_BcZDxunwTMGnH48nVhSgQURb";

  try {
    const { to, subject, html, fromName } = req.body;

    if (!to || !subject || !html) {
      return res.status(400).json({ error: "Missing required fields: to, subject, html" });
    }

    const from = fromName
      ? `${fromName} <robert@urbansv.com>`
      : "Urban Street Ventures <robert@urbansv.com>";

    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ from, to, subject, html }),
    });

    const data = await response.json();

    if (!response.ok) {
      return res.status(response.status).json({ error: data.message || "Resend error" });
    }

    return res.status(200).json({ success: true, id: data.id });

  } catch (err) {
    console.error("Blast send error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}
