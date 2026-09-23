// Matchmaking + WebRTC signaling server.
// Video/audio flows peer-to-peer; this server only pairs people and relays messages.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'public', 'index.html');

const server = http.createServer((req, res) => {
  fs.readFile(INDEX, (err, data) => {
    if (err) { res.writeHead(500); return res.end('Server error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

let nextId = 1;
const waiting = { video: [], text: [] };   // queues of clients waiting for a partner

const send = (ws, msg) => ws.readyState === 1 && ws.send(JSON.stringify(msg));

function broadcastCount() {
  const n = wss.clients.size;
  wss.clients.forEach(c => send(c, { type: 'count', n }));
}

function removeFromQueue(ws) {
  for (const q of Object.values(waiting)) {
    const i = q.indexOf(ws);
    if (i !== -1) q.splice(i, 1);
  }
}

function unpair(ws, notify = true) {
  const p = ws.partner;
  ws.partner = null;
  if (p) {
    p.partner = null;
    if (notify) send(p, { type: 'left' });
  }
}

function find(ws, mode) {
  unpair(ws);
  removeFromQueue(ws);
  ws.mode = mode === 'text' ? 'text' : 'video';
  const q = waiting[ws.mode];
  const peer = q.find(c => c !== ws && c.readyState === 1);
  if (peer) {
    q.splice(q.indexOf(peer), 1);
    ws.partner = peer;
    peer.partner = ws;
    send(ws, { type: 'matched', initiator: true });
    send(peer, { type: 'matched', initiator: false });
  } else {
    q.push(ws);
    send(ws, { type: 'waiting' });
  }
}

wss.on('connection', ws => {
  ws.id = nextId++;
  ws.partner = null;
  ws.lastChat = 0;
  broadcastCount();

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    switch (m.type) {
      case 'find':
        find(ws, m.mode);
        break;
      case 'next':
        find(ws, ws.mode);
        break;
      case 'stop':
        unpair(ws);
        removeFromQueue(ws);
        break;
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
      case 'report':
        // Hook this up to real moderation (database, review queue, bans).
        if (ws.partner) console.log(`REPORT: user ${ws.id} reported user ${ws.partner.id}`);
        break;
    }
  });

  ws.on('close', () => {
    unpair(ws);
    removeFromQueue(ws);
    broadcastCount();
  });
});

server.listen(PORT, () => console.log(`Running on http://localhost:${PORT}`));
