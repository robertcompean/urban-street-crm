const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

export default async function handler(req, res) {
  const { id } = req.query;

  if (!id) {
    return res.status(400).send(renderPage("Invalid Request", "No contact ID provided."));
  }

  try {
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/contacts?id=eq.${id}`,
      {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "apikey": SUPABASE_SERVICE_KEY,
          "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
          "Prefer": "return=representation",
        },
        body: JSON.stringify({ subscription_status: "Unsubscribed" }),
      }
    );

    if (!response.ok) {
      throw new Error(`Supabase error: ${response.status}`);
    }

    return res.status(200).send(renderPage(
      "You've been unsubscribed",
      "You have been successfully removed from our mailing list. You will no longer receive marketing emails from Urban Street Ventures."
    ));
  } catch (err) {
    console.error("Unsubscribe error:", err);
    return res.status(500).send(renderPage(
      "Something went wrong",
      "We couldn't process your request. Please try again or contact us directly."
    ));
  }
}

function renderPage(title, message) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>${title} — Urban Street Ventures</title>
  <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700&display=swap" rel="stylesheet"/>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:#f4f4f5;font-family:'Barlow Condensed',sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;}
    .card{background:#fff;border:1px solid #e2e2e5;border-radius:6px;padding:48px;max-width:480px;width:90%;text-align:center;}
    .logo{font-size:11px;font-weight:700;letter-spacing:0.2em;color:#e8622a;text-transform:uppercase;margin-bottom:32px;}
    h1{font-size:24px;font-weight:700;color:#111;margin-bottom:16px;}
    p{font-size:16px;color:#666;line-height:1.6;}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">Urban Street Ventures</div>
    <h1>${title}</h1>
    <p>${message}</p>
  </div>
</body>
</html>`;
}
