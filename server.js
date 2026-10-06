const express = require('express');
const path = require('path');

const app = express();
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

app.listen(PORT, () => {
  console.log(`Location Tracker running at http://localhost:${PORT}`);
  console.log(`- Share link for family:  http://localhost:${PORT}/`);
  console.log(`- Live map dashboard:     http://localhost:${PORT}/map.html`);
});
