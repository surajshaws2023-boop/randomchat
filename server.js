// Matchmaking + WebRTC signaling server.
// Video/audio flows peer-to-peer; this server only pairs people and relays messages.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const billing = require('./billing');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'public', 'index.html');

const server = http.createServer(async (req, res) => {
  if (await billing.handle(req, res)) return;
  fs.readFile(INDEX, (err, data) => {
    if (err) { res.writeHead(500); return res.end('Server error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

let nextId = 1;
const queue = [];              // clients waiting for a partner
const bans = new Map();        // ip -> ban expiry (ms). In memory: cleared on restart.
const reports = new Map();     // ip -> Set of distinct reporter IPs
const BAN_MS = 24 * 60 * 60 * 1000;   // ban length
const REPORTS_TO_BAN = 3;             // distinct reporters needed
const FALLBACK_MS = 10000;            // wait this long for an interest match, then match anyone

const send = (ws, msg) => ws.readyState === 1 && ws.send(JSON.stringify(msg));
const clean = s => String(s || '').toLowerCase().trim().slice(0, 30);

function broadcastCount() {
  const n = wss.clients.size;
  wss.clients.forEach(c => send(c, { type: 'count', n }));
}

function removeFromQueue(ws) {
  clearTimeout(ws.timer);
  const i = queue.indexOf(ws);
  if (i !== -1) queue.splice(i, 1);
}

function unpair(ws, notify = true) {
  const p = ws.partner;
  ws.partner = null;
  if (p) {
    p.partner = null;
    if (notify) send(p, { type: 'left' });
  }
}

// Interests: if either person has none, that's fine; otherwise they need one in common.
const shared = (a, b) => !a.interests.length || !b.interests.length || a.interests.some(i => b.interests.includes(i));
// Country/language filters: honored if either person asked for them.
function allowed(a, b) {
  for (const k of ['country', 'language']) {
    if ((a.filter === k || b.filter === k) && a[k] !== b[k]) return false;
  }
  // Paid gender filter: honored only if that person chose it (and had premium when searching).
  if (a.want && b.gender !== a.want) return false;
  if (b.want && a.gender !== b.want) return false;
  return true;
}

function tryMatch(ws, strict) {
  const peer = queue.find(c => c !== ws && c.readyState === 1 && c.mode === ws.mode && allowed(ws, c) && (!strict || shared(ws, c)));
  if (!peer) return false;
  removeFromQueue(peer);
  removeFromQueue(ws);
  ws.partner = peer;
  peer.partner = ws;
  const common = ws.interests.filter(i => peer.interests.includes(i));
  send(ws, { type: 'matched', initiator: true, common });
  send(peer, { type: 'matched', initiator: false, common });
  return true;
}

function find(ws, m) {
  unpair(ws);
  removeFromQueue(ws);
  if (m) {   // new preferences (sent on first search); "Next" reuses the saved ones
    ws.mode = m.mode === 'text' ? 'text' : 'video';
    ws.interests = [...new Set((Array.isArray(m.interests) ? m.interests : []).map(clean).filter(Boolean))].slice(0, 5);
    ws.country = clean(m.country);
    ws.language = clean(m.language);
    ws.filter = ['country', 'language'].includes(m.filter) ? m.filter : 'any';
    ws.want = ['male', 'female'].includes(m.want) && Date.now() < ws.premiumUntil ? m.want : '';
  }
  if (tryMatch(ws, true)) return;
  queue.push(ws);
  send(ws, { type: 'waiting' });
  if (ws.interests.length) ws.timer = setTimeout(() => tryMatch(ws, false), FALLBACK_MS);
}

// Reads the sign-in token to learn the user's gender and whether premium is active.
async function applyAuth(ws, token) {
  ws.gender = ''; ws.premiumUntil = 0;
  try {
    const u = await billing.userFromToken(token);
    if (u) { ws.gender = u.gender || ''; ws.premiumUntil = u.premiumUntil ? +new Date(u.premiumUntil) : 0; }
  } catch {}
}

wss.on('connection', (ws, req) => {
  ws.ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (bans.get(ws.ip) > Date.now()) { send(ws, { type: 'banned' }); return ws.close(); }
  bans.delete(ws.ip);

  ws.id = nextId++;
  ws.partner = null;
  ws.lastChat = 0;
  ws.mode = 'video'; ws.interests = []; ws.country = ''; ws.language = ''; ws.filter = 'any';
  ws.gender = ''; ws.want = ''; ws.premiumUntil = 0;
  broadcastCount();

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    switch (m.type) {
      case 'find': applyAuth(ws, m.token).then(() => find(ws, m)); break;
      case 'next': find(ws); break;
      case 'stop': unpair(ws); removeFromQueue(ws); break;
      case 'signal':
        if (ws.partner) send(ws.partner, { type: 'signal', data: m.data });
        break;
      case 'chat': {
        const now = Date.now();
        if (!ws.partner || now - ws.lastChat < 250) break;   // basic rate limit
        ws.lastChat = now;
        const text = String(m.text || '').slice(0, 500);
        if (text.trim()) send(ws.partner, { type: 'chat', text });
        break;
      }
      case 'report': {
        const p = ws.partner;
        if (!p) break;
        const who = reports.get(p.ip) || new Set();
        who.add(ws.ip);
        reports.set(p.ip, who);
        console.log(`REPORT: user ${ws.id} reported user ${p.id} (${who.size}/${REPORTS_TO_BAN})`);
        if (who.size >= REPORTS_TO_BAN) {
          bans.set(p.ip, Date.now() + BAN_MS);
          reports.delete(p.ip);
          console.log(`BANNED: ${p.ip} for 24h`);
          unpair(p, false);
          send(p, { type: 'banned' });
          p.close();
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    unpair(ws);
    removeFromQueue(ws);
    broadcastCount();
  });
});

billing.init().catch(console.error);
server.listen(PORT, () => console.log(`Running on http://localhost:${PORT}`));
