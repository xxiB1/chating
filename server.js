const path = require('path');
const http = require('http');
const express = require('express');
const WebSocket = require('ws');
const { TikTokLiveConnection, WebcastEvent, ControlEvent } = require('tiktok-live-connector');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_, res) => res.send('ok'));
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';
const PUSHER = 'wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0&flash=false';

// إعادة محاولة واحدة فقط في نفس الوقت
function retrier(run, isStopped) {
  let timer = null;
  const fn = ms => { if (!isStopped() && !timer) timer = setTimeout(() => { timer = null; run(); }, ms); };
  fn.clear = () => { clearTimeout(timer); timer = null; };
  return fn;
}

// ---------- TikTok ----------
// حقل النص في رسائل تيك توك اسمه content (وليس comment)، والإيموجي تجي بمواقع داخل النص
const CDN = /^https:\/\/[\w.-]*(tiktokcdn|byteimg|ibyteimg|ibytedtos)[\w.-]*\//;
function ttText(d) {
  let s = String(d.content ?? d.comment ?? '');
  const em = (d.emotes || [])
    .map(e => ({ i: Number(e.index) || 0, url: e.emote?.image?.urlList?.[0] }))
    .filter(e => e.url && CDN.test(e.url) && !/[\]\s]/.test(e.url))
    .sort((a, b) => b.i - a.i);
  for (const e of em) { const i = Math.min(Math.max(e.i, 0), s.length); s = s.slice(0, i) + `[tt:${e.url}]` + s.slice(i); }
  return s;
}

function tiktok(name, emit, status) {
  let stopped = false, conn;
  const retry = retrier(() => run(), () => stopped);
  async function run() {
    if (stopped) return;
    status('connecting');
    conn = new TikTokLiveConnection(name, {});
    conn.on(WebcastEvent.CHAT, d => emit({
      platform: 'tiktok',
      user: d.user?.nickname || d.user?.uniqueId || '?',
      text: ttText(d)
    }));
    conn.on(WebcastEvent.EMOTE, d => {
      const t = (d.emoteList || []).map(e => e.image?.urlList?.[0]).filter(u => u && CDN.test(u) && !/[\]\s]/.test(u)).map(u => `[tt:${u}]`).join(' ');
      if (t) emit({ platform: 'tiktok', user: d.user?.nickname || d.user?.uniqueId || '?', text: t });
    });
    conn.on(ControlEvent.DISCONNECTED, () => { status('offline'); retry(10000); });
    try { await conn.connect(); status('connected'); }
    catch (e) { status('offline', String(e.message || e).slice(0, 120)); retry(20000); }
  }
  run();
  return () => { stopped = true; retry.clear(); conn?.disconnect().catch(() => {}); };
}

// ---------- Kick ----------
async function kickChatroomId(slug) {
  const r = await fetch(`https://kick.com/api/v2/channels/${encodeURIComponent(slug)}`,
    { headers: { 'user-agent': UA, accept: 'application/json' } });
  if (!r.ok) throw new Error('HTTP ' + r.status);
  return (await r.json()).chatroom.id;
}

function kick(slug, givenId, emit, status) {
  let stopped = false, sock, id = givenId;
  const retry = retrier(() => run(), () => stopped);
  async function run() {
    if (stopped) return;
    status('connecting');
    if (!id) {
      try { id = await kickChatroomId(slug); }
      catch { status('need_id'); return retry(60000); }
    }
    sock = new WebSocket(PUSHER);
    sock.on('message', raw => {
      try {
        const m = JSON.parse(raw);
        if (m.event === 'pusher:connection_established')
          sock.send(JSON.stringify({ event: 'pusher:subscribe', data: { auth: '', channel: `chatrooms.${id}.v2` } }));
        else if (m.event === 'pusher_internal:subscription_succeeded') status('connected');
        else if (m.event === 'pusher:ping') sock.send(JSON.stringify({ event: 'pusher:pong', data: {} }));
        else if (m.event === 'App\\Events\\ChatMessageEvent') {
          const d = JSON.parse(m.data);
          emit({ platform: 'kick', user: d.sender?.username || '?', text: d.content || '', color: d.sender?.identity?.color });
        }
      } catch {}
    });
    sock.on('close', () => { status('offline'); retry(5000); });
    sock.on('error', () => {});
  }
  run();
  return () => { stopped = true; retry.clear(); sock?.terminate(); };
}

// ---------- الغرف المشتركة (اتصال واحد لكل قناة مهما كثر المشاهدين) ----------
const rooms = new Map();
function join(ws, key, start) {
  let r = rooms.get(key);
  if (!r) {
    r = { subs: new Set(), last: null };
    const send = o => { const s = JSON.stringify(o); r.subs.forEach(c => c.readyState === 1 && c.send(s)); };
    r.stop = start(
      m => send({ type: 'chat', ...m }),
      (state, detail) => { r.last = { type: 'status', platform: key.split(':')[0], state, detail }; send(r.last); }
    );
    rooms.set(key, r);
  }
  r.subs.add(ws); ws.keys.add(key);
  if (r.last) ws.send(JSON.stringify(r.last));
}
function leave(ws) {
  for (const key of ws.keys) {
    const r = rooms.get(key); if (!r) continue;
    r.subs.delete(ws);
    if (!r.subs.size) { r.stop(); rooms.delete(key); }
  }
  ws.keys.clear();
}

const clean = s => String(s || '').trim().replace(/^@/, '').match(/^[\w.\-]{1,40}$/)?.[0] || '';

wss.on('connection', ws => {
  ws.keys = new Set(); ws.alive = true;
  ws.on('pong', () => (ws.alive = true));
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type !== 'join') return;
    leave(ws);
    const tt = clean(m.tiktok), kk = clean(m.kick);
    if (tt) join(ws, 'tiktok:' + tt.toLowerCase(), (e, s) => tiktok(tt, e, s));
    if (kk) join(ws, 'kick:' + kk.toLowerCase(), (e, s) => kick(kk, /^\d{1,12}$/.test(m.kickId) ? m.kickId : null, e, s));
  });
  ws.on('close', () => leave(ws));
});
setInterval(() => wss.clients.forEach(c => { if (!c.alive) return c.terminate(); c.alive = false; c.ping(); }), 25000);

process.on('uncaughtException', e => console.error('uncaught', e.message));
process.on('unhandledRejection', e => console.error('unhandled', e?.message || e));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log('Chat hub running on port ' + PORT));
