// npm install ws
const { WebSocketServer } = require('ws');

const PORT = 8080;
const PING_INTERVAL = 5000;
const ROOM_KEYS = ['neon', 'azure', 'ember'];

const clients = new Set();         // все подключённые ws
const rooms = new Map();            // roomId → Set<ws>
ROOM_KEYS.forEach(k => rooms.set(k, new Set()));

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcastRoom(roomId, obj, except) {
  const raw = JSON.stringify(obj);
  const set = rooms.get(roomId);
  if (!set) return;
  for (const c of set) {
    if (c !== except && c.readyState === 1) c.send(raw);
  }
}
function broadcastAll(obj) {
  const raw = JSON.stringify(obj);
  for (const c of clients) {
    if (c.readyState === 1) c.send(raw);
  }
}
function broadcastCounts() {
  const counts = {};
  for (const [k, set] of rooms) counts[k] = set.size;
  broadcastAll({ type: 'counts', counts });
}

const wss = new WebSocketServer({ port: PORT });

wss.on('connection', (ws) => {
  ws.id = Math.random().toString(36).slice(2, 8);
  ws.room = null;
  ws.nick = 'ANON';
  ws.ship = 'falcon';
  ws.alive = true;
  clients.add(ws);

  send(ws, { type: 'welcome', id: ws.id, players: [] });
  // Пришлём сразу все counts, чтобы меню знало кто где
  const counts = {};
  for (const [k, set] of rooms) counts[k] = set.size;
  send(ws, { type: 'counts', counts });

  ws.on('message', raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'enter': {
        const roomId = String(msg.room || '').slice(0, 16);
        if (!rooms.has(roomId)) rooms.set(roomId, new Set());

        // Выйти из старой комнаты
        if (ws.room && rooms.has(ws.room)) {
          rooms.get(ws.room).delete(ws);
          broadcastRoom(ws.room, { type: 'leave', id: ws.id });
        }

        ws.room = roomId;
        ws.nick = String(msg.nick || 'ANON').slice(0, 12).toUpperCase();
        ws.ship = String(msg.ship || 'falcon').slice(0, 16);
        rooms.get(roomId).add(ws);

        // Список тех, кто уже тут
        const others = [];
        for (const c of rooms.get(roomId)) {
          if (c !== ws) others.push({ id: c.id, nick: c.nick, ship: c.ship });
        }
        send(ws, { type: 'welcome', id: ws.id, players: others });

        // Сообщить остальным
        broadcastRoom(roomId, {
          type: 'join',
          id: ws.id, nick: ws.nick, ship: ws.ship,
        }, ws);

        broadcastCounts();
        break;
      }

      case 'leave': {
        if (ws.room && rooms.has(ws.room)) {
          rooms.get(ws.room).delete(ws);
          broadcastRoom(ws.room, { type: 'leave', id: ws.id });
          ws.room = null;
          broadcastCounts();
        }
        break;
      }

      case 'state': {
        if (!ws.room) break;
        broadcastRoom(ws.room, { type: 'state', id: ws.id, data: msg.data }, ws);
        break;
      }

      case 'chat': {
        if (!ws.room) break;
        const text = String(msg.text || '').slice(0, 100);
        if (!text) break;
        broadcastRoom(ws.room, {
          type: 'chat',
          id: ws.id, nick: ws.nick, text,
        });
        break;
      }

      case 'pong':
        ws.alive = true;
        break;
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    if (ws.room && rooms.has(ws.room)) {
      rooms.get(ws.room).delete(ws);
      broadcastRoom(ws.room, { type: 'leave', id: ws.id });
      broadcastCounts();
    }
  });
});

setInterval(() => {
  for (const ws of clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    send(ws, { type: 'ping' });
  }
}, PING_INTERVAL);

console.log('WS listening on :' + PORT);