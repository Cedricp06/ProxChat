// ============================================================================
// ProxVoice signaling server
// Tiny WebSocket relay for WebRTC handshake (offer/answer/ICE) + room roster.
// Runs locally now; deploys unchanged to a free host (Render/Fly/Railway) later.
//
//   node server.js            -> listens on ws://localhost:9000
//
// It never touches audio. It only relays small JSON messages so peers can find
// each other and exchange SDP/ICE, after which voice flows peer-to-peer.
// ============================================================================
const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 9000;

// A plain HTTP server so hosting platforms (Render/Railway/Fly) can health-check
// the app with a normal GET. The WebSocket server rides on top of it.
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ProxVoice signaling OK\n");
});

const wss = new WebSocketServer({ server: httpServer });

// rooms: roomCode -> Map(peerId -> ws)
const rooms = new Map();

function send(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch {}
}

function roster(roomCode) {
  const room = rooms.get(roomCode);
  return room ? [...room.keys()] : [];
}

function broadcastRoster(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const members = [...room.keys()];
  for (const ws of room.values()) {
    send(ws, { type: "roster", members });
  }
}

wss.on("connection", (ws) => {
  ws.meta = { room: null, id: null };

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.type) {
      case "join": {
        // { type:"join", room, id }
        const { room, id } = msg;
        ws.meta.room = room;
        ws.meta.id = id;
        if (!rooms.has(room)) rooms.set(room, new Map());
        rooms.get(room).set(id, ws);
        console.log(`[join] ${id} -> room ${room} (now ${roster(room).length})`);
        // tell the newcomer who's already here, and everyone the updated list
        send(ws, { type: "welcome", id, members: roster(room) });
        broadcastRoster(room);
        break;
      }

      case "signal": {
        // { type:"signal", to, from, data }  -- relay SDP/ICE to one peer
        const room = rooms.get(ws.meta.room);
        if (!room) return;
        const target = room.get(msg.to);
        if (target) send(target, { type: "signal", from: msg.from, data: msg.data });
        break;
      }

      case "leave": {
        cleanup(ws);
        break;
      }
    }
  });

  ws.on("close", () => cleanup(ws));
  ws.on("error", () => cleanup(ws));
});

function cleanup(ws) {
  const { room, id } = ws.meta || {};
  if (!room || !id) return;
  const r = rooms.get(room);
  if (r && r.get(id) === ws) {
    r.delete(id);
    console.log(`[leave] ${id} <- room ${room} (now ${r.size})`);
    if (r.size === 0) rooms.delete(room);
    else broadcastRoster(room);
  }
  ws.meta = {};
}

httpServer.listen(PORT, () => {
  console.log(`ProxVoice signaling server listening on port ${PORT}`);
});
