// Google sign-in, gender profile, and Razorpay payments for the paid gender filter.
const crypto = require('crypto');
const { Pool } = require('pg');
const { OAuth2Client } = require('google-auth-library');

const { GOOGLE_CLIENT_ID, SESSION_SECRET, RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET, DATABASE_URL } = process.env;

const PLANS = {
  week:  { label: '1 week trial', rupees: 700,  days: 7 },
  month: { label: '1 month',      rupees: 4000, days: 30 },
  six:   { label: '6 months',     rupees: 5000, days: 182 },
};

const google = new OAuth2Client(GOOGLE_CLIENT_ID);
const db = DATABASE_URL
  ? new Pool({ connectionString: DATABASE_URL, ssl: /localhost|127\.0\.0\.1/.test(DATABASE_URL) ? false : { rejectUnauthorized: false } })
  : null;

async function init() {
  if (!db || !SESSION_SECRET) return console.log('Accounts and payments are OFF (set DATABASE_URL and SESSION_SECRET).');
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, email TEXT, gender TEXT, premium_until TIMESTAMPTZ);
    CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, user_id TEXT, plan TEXT, paid BOOLEAN DEFAULT false, payment_id TEXT UNIQUE);
  `);
  console.log('Accounts and payments are ON.');
}

/* ---- signed login tokens (30 days) ---- */
const sign = p => crypto.createHmac('sha256', SESSION_SECRET).update(p).digest('base64url');
const makeToken = id => {
  const p = Buffer.from(JSON.stringify({ id, exp: Date.now() + 30 * 864e5 })).toString('base64url');
  return p + '.' + sign(p);
};
function readToken(t) {
  try {
    const [p, s] = String(t).split('.');
    if (!crypto.timingSafeEqual(Buffer.from(s), Buffer.from(sign(p)))) return null;
    const d = JSON.parse(Buffer.from(p, 'base64url'));
    return d.exp > Date.now() ? d.id : null;
  } catch { return null; }
}

async function getUser(id) {
  const u = (await db.query('SELECT email, gender, premium_until FROM users WHERE id = $1', [id])).rows[0];
  if (!u) return null;
  return {
    email: u.email,
    gender: u.gender,
    premiumUntil: u.premium_until,
    premium: !!u.premium_until && new Date(u.premium_until) > new Date(),
  };
}
const userFromToken = async t => (db && SESSION_SECRET && readToken(t) ? getUser(readToken(t)) : null);

/* ---- HTTP API ---- */
const body = req => new Promise(resolve => {
  let d = '';
  req.on('data', c => { d += c; if (d.length > 1e5) req.destroy(); });
  req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
});

async function handle(req, res) {
  if (!req.url.startsWith('/api/')) return false;
  const route = req.method + ' ' + req.url.split('?')[0];
  const out = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); return true; };

  try {
    const on = !!(db && SESSION_SECRET);
    if (route === 'GET /api/config')
      return out(200, { enabled: on, google: GOOGLE_CLIENT_ID || null, razorpay: !!RAZORPAY_KEY_ID, plans: PLANS });
    if (!on) return out(503, { error: 'Accounts are not set up on this server.' });

    if (route === 'POST /api/google') {
      const { credential } = await body(req);
      const t = await google.verifyIdToken({ idToken: credential, audience: GOOGLE_CLIENT_ID });
      const p = t.getPayload();
      await db.query('INSERT INTO users (id, email) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET email = $2', [p.sub, p.email]);
      return out(200, { token: makeToken(p.sub), user: await getUser(p.sub) });
    }

    const uid = readToken((req.headers.authorization || '').replace('Bearer ', ''));
    if (!uid) return out(401, { error: 'Please sign in again.' });

    if (route === 'GET /api/me') return out(200, { user: await getUser(uid) });

    if (route === 'POST /api/profile') {
      const { gender } = await body(req);
      if (!['male', 'female', 'other'].includes(gender)) return out(400, { error: 'Choose a gender.' });
      const r = await db.query('UPDATE users SET gender = $2 WHERE id = $1 AND gender IS NULL', [uid, gender]);
      if (!r.rowCount) return out(409, { error: 'Gender is already saved and cannot be changed.' });
      return out(200, { user: await getUser(uid) });
    }

    if (route === 'POST /api/order') {
      const plan = PLANS[(await body(req)).plan];
      const planKey = Object.keys(PLANS).find(k => PLANS[k] === plan);
      if (!plan || !RAZORPAY_KEY_ID) return out(400, { error: 'Payments are not available.' });
      const r = await fetch('https://api.razorpay.com/v1/orders', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: 'Basic ' + Buffer.from(RAZORPAY_KEY_ID + ':' + RAZORPAY_KEY_SECRET).toString('base64'),
        },
        body: JSON.stringify({ amount: plan.rupees * 100, currency: 'INR', receipt: uid.slice(0, 20) + '-' + Date.now() }),
      });
      const o = await r.json();
      if (!o.id) return out(502, { error: 'Could not start the payment.' });
      await db.query('INSERT INTO orders (id, user_id, plan) VALUES ($1, $2, $3)', [o.id, uid, planKey]);
      return out(200, { orderId: o.id, amount: o.amount, keyId: RAZORPAY_KEY_ID });
    }

    if (route === 'POST /api/verify') {
      const b = await body(req);
      const good = crypto.createHmac('sha256', RAZORPAY_KEY_SECRET).update(b.razorpay_order_id + '|' + b.razorpay_payment_id).digest('hex');
      if (good !== b.razorpay_signature) return out(400, { error: 'Payment could not be verified.' });
      const r = await db.query(
        'UPDATE orders SET paid = true, payment_id = $2 WHERE id = $1 AND user_id = $3 AND paid = false RETURNING plan',
        [b.razorpay_order_id, b.razorpay_payment_id, uid]);
      if (!r.rowCount) return out(409, { error: 'This payment was already applied.' });
      await db.query(
        'UPDATE users SET premium_until = GREATEST(COALESCE(premium_until, now()), now()) + make_interval(days => $2::int) WHERE id = $1',
        [uid, PLANS[r.rows[0].plan].days]);
      return out(200, { user: await getUser(uid) });
    }

    return out(404, { error: 'Not found.' });
  } catch (e) {
    console.error(e);
    return out(500, { error: 'Something went wrong. Please try again.' });
  }
}

module.exports = { init, handle, userFromToken };
