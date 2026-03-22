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

    // 3. Get custom field IDs
    const customFields = await sb("/custom_field_defs?field_key=in.(cs_done_deal,cs_responsiveness,cs_incoming_ads)&select=id,field_key");
    const fieldIdMap = {};
    (customFields || []).forEach(f => { fieldIdMap[f.field_key] = f.id; });

    // 4. Load all contacts (paginated, excluding investors)
    const PAGE = 500;
    let allContacts = [];
    let offset = 0;
    while (true) {
      let url = `/contacts?select=id&limit=${PAGE}&offset=${offset}`;
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
      const ids = batch.map(c => c.id);
      const idList = ids.join(",");

      // Custom field values
      const fieldIds = Object.values(fieldIdMap).filter(Boolean);
      let cvRows = [];
      if (fieldIds.length > 0) {
        cvRows = await sb(`/contact_custom_values?contact_id=in.(${idList})&field_id=in.(${fieldIds.join(",")})&select=contact_id,field_id,value`) || [];
      }
      const cvMap = {};
      cvRows.forEach(row => {
        if (!cvMap[row.contact_id]) cvMap[row.contact_id] = {};
        const key = Object.keys(fieldIdMap).find(k => fieldIdMap[k] === row.field_id);
        if (key) cvMap[row.contact_id][key] = (row.value || "").toLowerCase().trim();
      });

      // Conversations in last 30 days
      const convRows = await sb(`/conversations?contact_id=in.(${idList})&last_message_at=gte.${thirtyDaysAgo}&select=contact_id,last_direction`) || [];
      const actMap = {};
      convRows.forEach(c => {
        if (!actMap[c.contact_id]) actMap[c.contact_id] = { hasActivity: false, hasSent: false, hasReceived: false };
        actMap[c.contact_id].hasActivity = true;
        if (c.last_direction === "outbound") actMap[c.contact_id].hasSent = true;
        if (c.last_direction === "inbound") actMap[c.contact_id].hasReceived = true;
      });

      // Score and upsert
      for (const contact of batch) {
        const cv = cvMap[contact.id] || {};
        const act = actMap[contact.id] || {};
        let score = 0;

        for (const rule of rules) {
          switch (rule.signal_key) {
            case "cs_done_deal_yes":
              if (["yes","1+","done","true","1"].includes(cv.cs_done_deal || "")) score += rule.points;
              break;
            case "cs_done_deal_attempted":
              if (["attempted","in progress","pending"].includes(cv.cs_done_deal || "")) score += rule.points;
              break;
            case "cs_responsiveness_good":
              if (["responsive","yes","true","1"].includes(cv.cs_responsiveness || "")) score += rule.points;
              break;
            case "cs_responsiveness_bad":
              if (["unresponsive","no","false","0"].includes(cv.cs_responsiveness || "")) score += rule.points;
              break;
            case "recent_activity":
              if (act.hasActivity) score += rule.points;
              break;
            case "email_thread":
              if (act.hasSent && act.hasReceived) score += rule.points;
              break;
            case "cs_incoming_ads":
              if ((cv.cs_incoming_ads || "") && !["","no","false","0","none"].includes(cv.cs_incoming_ads || "")) score += rule.points;
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
