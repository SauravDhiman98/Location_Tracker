const express = require('express');
const path = require('path');
const crypto = require('crypto');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const PORT = process.env.PORT || 3000;

// How long (ms) before a device is considered "offline" and dropped from the map
const STALE_AFTER_MS = 2 * 60 * 1000; // 2 minutes

// In-memory store: Map<deviceId, { label, lat, lng, accuracy, updatedAt }>
const locations = new Map();

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Receive a location update from a sharing device
app.post('/api/location', (req, res) => {
  const { deviceId, label, lat, lng, accuracy } = req.body || {};

  if (!deviceId || typeof lat !== 'number' || typeof lng !== 'number') {
    return res.status(400).json({ error: 'deviceId, lat, and lng are required.' });
  }

  locations.set(deviceId, {
    label: label || `Device-${deviceId.slice(0, 5)}`,
    lat,
    lng,
    accuracy: accuracy || null,
    updatedAt: Date.now(),
  });

  res.json({ ok: true });
});

// Return all currently-active (non-stale) locations
app.get('/api/locations', (req, res) => {
  const now = Date.now();
  const active = [];

  for (const [deviceId, data] of locations.entries()) {
    if (now - data.updatedAt > STALE_AFTER_MS) {
      locations.delete(deviceId);
      continue;
    }
    active.push({ deviceId, ...data });
  }

  res.json(active);
});

// Allow a device to stop sharing immediately
app.post('/api/stop', (req, res) => {
  const { deviceId } = req.body || {};
  if (deviceId) locations.delete(deviceId);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Meetings (video/audio calls via WebRTC, signaled through Socket.IO)
// ---------------------------------------------------------------------------

// Map<meetingId, Map<socketId, { name }>>
const meetings = new Map();

function getParticipants(meetingId) {
  const room = meetings.get(meetingId);
  if (!room) return [];
  return [...room.entries()].map(([id, data]) => ({ id, name: data.name }));
}

// Create a new meeting and return its id
app.post('/api/meetings', (req, res) => {
  const meetingId = crypto.randomBytes(4).toString('hex');
  meetings.set(meetingId, new Map());
  res.json({ meetingId });
});

io.on('connection', (socket) => {
  // Let a visitor "peek" at a meeting's current participants before joining,
  // and keep them subscribed so the lobby list updates live.
  socket.on('lobby:peek', (meetingId) => {
    if (!meetings.has(meetingId)) meetings.set(meetingId, new Map());
    socket.join(`lobby-${meetingId}`);
    socket.data.peekMeetingId = meetingId;
    socket.emit('lobby:update', { participants: getParticipants(meetingId) });
  });

  // Actually join the call
  socket.on('meeting:join', ({ meetingId, name }) => {
    if (!meetingId || !name) return;
    if (!meetings.has(meetingId)) meetings.set(meetingId, new Map());

    const room = meetings.get(meetingId);
    const existingParticipants = getParticipants(meetingId);

    room.set(socket.id, { name: String(name).slice(0, 40) });
    socket.data.meetingId = meetingId;
    socket.data.name = name;
    socket.join(`meeting-${meetingId}`);

    // Tell the new joiner who is already in the call (they will initiate offers)
    socket.emit('meeting:joined', { selfId: socket.id, participants: existingParticipants });

    // Tell everyone else (in call + lobby watchers) about the new participant
    socket.to(`meeting-${meetingId}`).emit('meeting:participant-joined', { id: socket.id, name });
    io.to(`lobby-${meetingId}`).emit('lobby:update', { participants: getParticipants(meetingId) });
  });

  // Relay WebRTC signaling data (offers/answers/ICE candidates) between peers
  socket.on('meeting:signal', ({ to, data }) => {
    if (!to) return;
    io.to(to).emit('meeting:signal', { from: socket.id, data });
  });

  socket.on('meeting:leave', () => leaveMeeting(socket));
  socket.on('disconnect', () => leaveMeeting(socket));

  function leaveMeeting(sock) {
    const meetingId = sock.data.meetingId;
    if (!meetingId) return;
    const room = meetings.get(meetingId);
    if (room) {
      room.delete(sock.id);
      if (room.size === 0) meetings.delete(meetingId);
    }
    sock.to(`meeting-${meetingId}`).emit('meeting:participant-left', { id: sock.id });
    io.to(`lobby-${meetingId}`).emit('lobby:update', { participants: getParticipants(meetingId) });
    sock.data.meetingId = null;
  }
});

server.listen(PORT, () => {
  console.log(`Location Tracker running at http://localhost:${PORT}`);
  console.log(`- Share link for family:  http://localhost:${PORT}/`);
  console.log(`- Live map dashboard:     http://localhost:${PORT}/map.html`);
  console.log(`- Create a meeting:       http://localhost:${PORT}/create-meeting.html`);
});
