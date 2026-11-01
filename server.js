// Lap Chaser server: serves index.html and relays player state over WebSocket.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const PAGE = fs.readFileSync(path.join(__dirname, 'index.html'));

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(PAGE);
});

const wss = new WebSocketServer({ server, maxPayload: 2048 });
const rooms = new Map();
const KEYS = new Set(['n', 'st', 'host', 'rnd', 'c', 'fin', 'x', 'y', 'a', 'pr']);
const clean = p => {
  const o = {};
  if (!p || typeof p !== 'object') return o;
  for (const k of Object.keys(p)) {
    if (!KEYS.has(k)) continue;
    const v = p[k];
    if (v === null || typeof v === 'boolean' || (typeof v === 'number' && isFinite(v)) ||
        (typeof v === 'string' && v.length <= 24)) o[k] = v;
  }
  return o;
};

wss.on('connection', ws => {
  let name = null, room = null, id = null, pres = {};
  const bc = m => {
    const s = JSON.stringify(m);
    room.forEach((c, cid) => { if (cid !== id && c.ws.readyState === 1) c.ws.send(s); });
  };
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'join' && !room && /^lc\d{4}$/.test(m.room)) {
      let r = rooms.get(m.room);
      if (!r) { r = new Map(); rooms.set(m.room, r); }
      if (r.size >= 8) { ws.send('{"t":"full"}'); return ws.close(); }
      name = m.room; room = r;
      id = Math.random().toString(36).slice(2, 10);
      const peers = [...r.entries()].map(([cid, c]) => ({ id: cid, p: c.p }));
      r.set(id, { ws, get p() { return pres; } });
      ws.send(JSON.stringify({ t: 'init', id, peers }));
    } else if (m.t === 'p' && room) {
      const patch = clean(m.p);
      for (const k of Object.keys(patch)) {
        if (patch[k] === null) delete pres[k]; else pres[k] = patch[k];
      }
      bc({ t: 'p', id, p: pres });
    }
  });
  ws.on('close', () => {
    if (!room) return;
    room.delete(id);
    if (room.size === 0) rooms.delete(name); else bc({ t: 'left', id });
  });
});

server.listen(PORT, () => console.log('Lap Chaser running on port ' + PORT));
