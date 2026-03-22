const SUPABASE_URL = "https://kdxbkwcvrihcxqhukjee.supabase.co";
const SUPABASE_KEY = "sb_publishable_9whlg1wqquwmjgivsavs0A_H7HpbgE9";

const sb = async (path, options = {}) => {
  const res = await fetch(`${SUPABASE_URL}/rest/v1${path}`, {
    ...options,
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
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
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    // 1. Load enabled rules
    const rules = await sb("/scoring_rules?enabled=eq.true");
    if (!rules?.length) return res.status(200).json({ success: true, processed: 0 });

    // 2. Get investor bucket IDs to exclude
    const investorBuckets = await sb("/buckets?name=ilike.*investor*&select=id");
    let investorContactIds = [];
    if (investorBuckets?.length) {
      const bucketIds = investorBuckets.map(b => b.id).join(",");
      const invRows = await sb(`/contact_buckets?bucket_id=in.(${bucketIds})&select=contact_id`);
      investorContactIds = (invRows || []).map(r => r.contact_id);
    }

    // 3. Load all contacts with cs_ columns directly
    const PAGE = 500;
    let allContacts = [];
    let offset = 0;
    while (true) {
      let url = `/contacts?select=id,cs_done_deal,cs_responsiveness,cs_incoming_ads&limit=${PAGE}&offset=${offset}`;
      if (investorContactIds.length > 0) {
        url += `&id=not.in.(${investorContactIds.join(",")})`;
      }
      const batch = await sb(url);
      if (!batch?.length) break;
      allContacts = allContacts.concat(batch);
      if (batch.length < PAGE) break;
      offset += PAGE;
    }

    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    const BATCH = 200;
    let processed = 0;

    for (let i = 0; i < allContacts.length; i += BATCH) {
      const batch = allContacts.slice(i, i + BATCH);
      const idList = batch.map(c => c.id).join(",");

      // Recent conversations (last 30 days)
      const convRows = await sb(`/conversations?contact_id=in.(${idList})&last_message_at=gte.${thirtyDaysAgo}&select=contact_id,last_direction`) || [];
      const actMap = {};
      convRows.forEach(c => {
        if (!actMap[c.contact_id]) actMap[c.contact_id] = { hasActivity: false, hasSent: false, hasReceived: false };
        actMap[c.contact_id].hasActivity = true;
        if (c.last_direction === "outbound") actMap[c.contact_id].hasSent = true;
        if (c.last_direction === "inbound") actMap[c.contact_id].hasReceived = true;
      });

      // Score each contact
      for (const contact of batch) {
        const done_deal = (contact.cs_done_deal || "").toLowerCase().trim();
        const responsiveness = (contact.cs_responsiveness || "").toLowerCase().trim();
        const incoming_ads = (contact.cs_incoming_ads || "").toLowerCase().trim();
        const act = actMap[contact.id] || {};
        let score = 0;

        for (const rule of rules) {
          switch (rule.signal_key) {
            case "cs_done_deal_yes":
              if (["yes","1+","done","true","1"].includes(done_deal)) score += rule.points;
              break;
            case "cs_done_deal_attempted":
              if (["attempted","in progress","pending"].includes(done_deal)) score += rule.points;
              break;
            case "cs_responsiveness_good":
              if (["responsive","yes","true","1"].includes(responsiveness)) score += rule.points;
              break;
            case "cs_responsiveness_bad":
              if (["unresponsive","no","false","0"].includes(responsiveness)) score += rule.points;
              break;
            case "recent_activity":
              if (act.hasActivity) score += rule.points;
              break;
            case "email_thread":
              if (act.hasSent && act.hasReceived) score += rule.points;
              break;
            case "cs_incoming_ads":
              if (incoming_ads && !["","no","false","0","none"].includes(incoming_ads)) score += rule.points;
              break;
          }
        }

        score = Math.max(0, Math.min(100, score));
        await sb(`/contacts?id=eq.${contact.id}`, {
          method: "PATCH",
          headers: { Prefer: "" },
          body: JSON.stringify({ ai_score: score }),
        });
      }

      processed += batch.length;
    }

    return res.status(200).json({ success: true, processed });
  } catch (err) {
    console.error("calculate-scores error:", err);
    return res.status(500).json({ error: err.message });
  }
}
