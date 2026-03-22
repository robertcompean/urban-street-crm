const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";

const sb = async (path, options = {}) => {
  const res = await fetch(`${SUPABASE_URL}/rest/v1${path}`, {
    ...options,
    headers: {
      apikey: "sb_publishable_9whlg1wqquwmjgivsavs0A_H7HpbgE9",
      Authorization: `Bearer ${"sb_publishable_9whlg1wqquwmjgivsavs0A_H7HpbgE9"}`,
      "Content-Type": "application/json",
      Prefer: "return=representation",
      ...(options.headers || {}),
    },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(text);
  return text ? JSON.parse(text) : null;
};

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  if (req.method === "GET") {
    try {
      const data = await sb("/scoring_rules?order=created_at.asc");
      return res.status(200).json({ rules: data || [] });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  if (req.method === "POST") {
    const { rules } = req.body;
    if (!Array.isArray(rules)) return res.status(400).json({ error: "rules must be array" });
    try {
      for (const r of rules) {
        await sb(`/scoring_rules?id=eq.${r.id}`, {
          method: "PATCH",
          body: JSON.stringify({ points: Number(r.points), enabled: Boolean(r.enabled), label: r.label }),
        });
      }
      return res.status(200).json({ success: true });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  return res.status(405).json({ error: "Method not allowed" });
}
