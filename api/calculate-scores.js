const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(
  'https://kdxbkwcvrihcxqhukjee.supabase.co',
  process.env.SUPABASE_SERVICE_KEY
);

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    // 1. Load enabled rules
    const { data: rules, error: rulesErr } = await supabase
      .from('scoring_rules').select('*').eq('enabled', true);
    if (rulesErr) throw rulesErr;

    // 2. Get investor bucket contact IDs to exclude
    const { data: investorBucket } = await supabase
      .from('buckets').select('id').ilike('name', '%investor%').single();
    let investorIds = [];
    if (investorBucket) {
      const { data: inv } = await supabase
        .from('contact_buckets').select('contact_id').eq('bucket_id', investorBucket.id);
      investorIds = (inv || []).map(r => r.contact_id);
    }

    // 3. Load all contacts (excluding investors)
    let q = supabase.from('contacts').select('id');
    if (investorIds.length > 0) {
      q = q.not('id', 'in', `(${investorIds.join(',')})`);
    }
    const { data: contacts, error: cErr } = await q;
    if (cErr) throw cErr;

    // 4. Get custom field IDs for scoring fields
    const { data: customFields } = await supabase
      .from('custom_fields').select('id, field_key')
      .in('field_key', ['cs_done_deal', 'cs_responsiveness', 'cs_incoming_ads']);
    const fieldIdMap = {};
    (customFields || []).forEach(f => { fieldIdMap[f.field_key] = f.id; });

    // 5. Process in batches
    const BATCH = 200;
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    let processed = 0;

    for (let i = 0; i < contacts.length; i += BATCH) {
      const batch = contacts.slice(i, i + BATCH);
      const ids = batch.map(c => c.id);

      // Custom field values
      const { data: cvRows } = await supabase
        .from('contact_custom_values').select('contact_id, field_id, value')
        .in('contact_id', ids)
        .in('field_id', Object.values(fieldIdMap).filter(Boolean));

      const cvMap = {};
      (cvRows || []).forEach(row => {
        if (!cvMap[row.contact_id]) cvMap[row.contact_id] = {};
        const key = Object.keys(fieldIdMap).find(k => fieldIdMap[k] === row.field_id);
        if (key) cvMap[row.contact_id][key] = (row.value || '').toLowerCase().trim();
      });

      // Recent conversations
      const { data: convRows } = await supabase
        .from('conversations').select('contact_id, last_direction, last_message_at')
        .in('contact_id', ids);

      // Recent messages (last 30 days)
      const { data: msgRows } = await supabase
        .from('messages')
        .select('conversation_id, direction, sent_at')
        .gte('sent_at', thirtyDaysAgo);

      // Build conversation_id → contact_id map
      const convContactMap = {};
      (convRows || []).forEach(c => { convContactMap[c.contact_id] = convContactMap[c.contact_id] || []; });

      // Build actMap per contact
      const actMap = {};
      (convRows || []).forEach(conv => {
        if (!ids.includes(conv.contact_id)) return;
        if (!actMap[conv.contact_id]) actMap[conv.contact_id] = { hasActivity: false, hasSent: false, hasReceived: false };
        if (conv.last_message_at && conv.last_message_at >= thirtyDaysAgo) {
          actMap[conv.contact_id].hasActivity = true;
          if (conv.last_direction === 'outbound') actMap[conv.contact_id].hasSent = true;
          if (conv.last_direction === 'inbound') actMap[conv.contact_id].hasReceived = true;
        }
      });

      // Score each contact
      const upserts = batch.map(contact => {
        const cv = cvMap[contact.id] || {};
        const act = actMap[contact.id] || {};
        let score = 0;

        for (const rule of rules) {
          switch (rule.signal_key) {
            case 'cs_done_deal_yes': {
              const v = cv.cs_done_deal || '';
              if (['yes','1+','done','true','1'].includes(v)) score += rule.points;
              break;
            }
            case 'cs_done_deal_attempted': {
              const v = cv.cs_done_deal || '';
              if (['attempted','in progress','pending'].includes(v)) score += rule.points;
              break;
            }
            case 'cs_responsiveness_good': {
              const v = cv.cs_responsiveness || '';
              if (['responsive','yes','true','1'].includes(v)) score += rule.points;
              break;
            }
            case 'cs_responsiveness_bad': {
              const v = cv.cs_responsiveness || '';
              if (['unresponsive','no','false','0'].includes(v)) score += rule.points;
              break;
            }
            case 'recent_activity':
              if (act.hasActivity) score += rule.points;
              break;
            case 'email_thread':
              if (act.hasSent && act.hasReceived) score += rule.points;
              break;
            case 'cs_incoming_ads': {
              const v = cv.cs_incoming_ads || '';
              if (v && !['','no','false','0','none'].includes(v)) score += rule.points;
              break;
            }
          }
        }

        score = Math.max(0, Math.min(100, score));
        return { id: contact.id, ai_score: score };
      });

      const { error: upsertErr } = await supabase
        .from('contacts').upsert(upserts, { onConflict: 'id' });
      if (upsertErr) console.error('Upsert error:', upsertErr.message);
      processed += batch.length;
    }

    return res.json({ success: true, processed });
  } catch (err) {
    console.error('calculate-scores error:', err);
    return res.status(500).json({ error: err.message });
  }
};
