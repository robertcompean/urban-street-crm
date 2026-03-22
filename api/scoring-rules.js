const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  'https://kdxbkwcvrihcxqhukjee.supabase.co',
  process.env.SUPABASE_SERVICE_KEY
);

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // GET — return all rules
  if (req.method === 'GET') {
    const { data, error } = await supabase
      .from('scoring_rules')
      .select('*')
      .order('created_at');
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ rules: data });
  }

  // POST — save updated rules array
  if (req.method === 'POST') {
    const { rules } = req.body;
    if (!Array.isArray(rules)) return res.status(400).json({ error: 'rules must be an array' });

    const { error } = await supabase
      .from('scoring_rules')
      .upsert(
        rules.map(r => ({
          id: r.id,
          signal_key: r.signal_key,
          label: r.label,
          points: Number(r.points),
          enabled: Boolean(r.enabled),
        })),
        { onConflict: 'id' }
      );

    if (error) return res.status(500).json({ error: error.message });
    return res.json({ success: true });
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
