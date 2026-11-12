const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const os = require('os');

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(__dirname, 'timer-state.json');

const TOTAL_DURATION_24H_MS = 24 * 60 * 60 * 1000; // 86,400,000 ms
const THRESHOLD_12H_MS = 12 * 60 * 60 * 1000;      // 43,200,000 ms

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Default canonical state
function createDefaultState() {
  return {
    totalDurationMs: TOTAL_DURATION_24H_MS,
    remainingMsWhenPaused: TOTAL_DURATION_24H_MS,
    isRunning: false,
    endTimestamp: null,
    threshold12hTriggered: false,
    expiredTriggered: false,
    cracks: [],
    lastUpdated: Date.now()
  };
}

function loadState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const raw = fs.readFileSync(STATE_FILE, 'utf8');
      const parsed = JSON.parse(raw);
      // Validate fields
      const state = { ...createDefaultState(), ...parsed };
      if (state.isRunning && state.endTimestamp) {
        const remaining = state.endTimestamp - Date.now();
        if (remaining <= 0) {
          state.isRunning = false;
          state.remainingMsWhenPaused = 0;
          state.endTimestamp = null;
          state.expiredTriggered = true;
          state.threshold12hTriggered = true;
        }
      }
      return state;
    }
  } catch (err) {
    console.error('Failed to load timer-state.json, using default:', err.message);
  }
  return createDefaultState();
}

let timerState = loadState();

function saveState() {
  try {
    timerState.lastUpdated = Date.now();
    fs.writeFileSync(STATE_FILE, JSON.stringify(timerState, null, 2), 'utf8');
  } catch (err) {
    console.error('Failed to save timer-state.json:', err.message);
  }
}

function getCurrentRemainingMs() {
  if (timerState.isRunning && timerState.endTimestamp !== null) {
    return Math.max(0, timerState.endTimestamp - Date.now());
  }
  return Math.max(0, timerState.remainingMsWhenPaused);
}

function getLanUrls() {
  const interfaces = os.networkInterfaces();
  const urls = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        urls.push(`http://${iface.address}:${PORT}`);
      }
    }
  }
  return urls;
}

app.get('/api/network-info', (req, res) => {
  res.json({
    port: PORT,
    lanUrls: getLanUrls(),
    localhostUrl: `http://localhost:${PORT}`,
    connectedClients: wss ? wss.clients.size : 0
  });
});

// Dedicated health check & uptime ping endpoints (Render, UptimeRobot, Pingdom, etc.)
function handleHealthCheck(req, res) {
  const remainingMs = getCurrentRemainingMs();
  res.status(200).json({
    status: 'healthy',
    service: 'doom-n-debug-central-clock',
    uptimeSeconds: Math.floor(process.uptime()),
    timestamp: Date.now(),
    isoTime: new Date().toISOString(),
    timer: {
      isRunning: timerState.isRunning,
      remainingSeconds: Math.round(remainingMs / 1000),
      threshold12hTriggered: timerState.threshold12hTriggered,
      expired: timerState.expiredTriggered
    },
    connectedClients: wss ? wss.clients.size : 0
  });
}

app.get('/health', handleHealthCheck);
app.get('/api/health', handleHealthCheck);
app.get('/status', handleHealthCheck);
app.get('/api/status', handleHealthCheck);

// Lightweight plain text ping for low-overhead monitor bots
app.get('/ping', (req, res) => res.status(200).send('pong'));

// Support HEAD requests for pingers that only check headers to save bandwidth
app.head('/health', (req, res) => res.status(200).end());
app.head('/api/health', (req, res) => res.status(200).end());
app.head('/api/status', (req, res) => res.status(200).end());
app.head('/ping', (req, res) => res.status(200).end());

app.get('/api/state', (req, res) => {
  res.json({
    state: timerState,
    remainingMs: getCurrentRemainingMs(),
    serverNow: Date.now(),
    connectedClients: wss ? wss.clients.size : 0
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function buildSyncPayload(extra = {}) {
  return JSON.stringify({
    type: 'sync',
    state: timerState,
    remainingMs: getCurrentRemainingMs(),
    serverNow: Date.now(),
    connectedClients: wss.clients.size,
    ...extra
  });
}

function broadcast(payloadObj) {
  const data = typeof payloadObj === 'string' ? payloadObj : JSON.stringify(payloadObj);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

function broadcastSync(eventName = null, eventData = {}) {
  const payload = {
    type: 'sync',
    state: timerState,
    remainingMs: getCurrentRemainingMs(),
    serverNow: Date.now(),
    connectedClients: wss.clients.size
  };
  if (eventName) {
    payload.event = eventName;
    payload.eventData = eventData;
  }
  broadcast(payload);
}

function addCrack(x = null, y = null, scale = 1.0) {
  const crack = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    x: typeof x === 'number' ? Math.min(0.95, Math.max(0.05, x)) : 0.15 + Math.random() * 0.7,
    y: typeof y === 'number' ? Math.min(0.95, Math.max(0.05, y)) : 0.15 + Math.random() * 0.7,
    seed: Math.floor(Math.random() * 1000000),
    scale
  };
  timerState.cracks.push(crack);
  if (timerState.cracks.length > 14) {
    timerState.cracks = timerState.cracks.slice(-14);
  }
  return crack;
}

wss.on('connection', (ws) => {
  // Send immediate sync to the newly connected device, and notify everyone of updated node count
  ws.send(buildSyncPayload({ event: 'connected' }));
  broadcast({
    type: 'clients_update',
    connectedClients: wss.clients.size,
    serverNow: Date.now()
  });

  ws.on('message', (rawMsg) => {
    let msg;
    try {
      msg = JSON.parse(rawMsg.toString());
    } catch {
      return;
    }

    const now = Date.now();

    switch (msg.type) {
      case 'ping': {
        ws.send(JSON.stringify({
          type: 'pong',
          clientTime: msg.clientTime,
          serverTime: now
        }));
        break;
      }

      case 'start': {
        if (timerState.isRunning) break;
        if (timerState.remainingMsWhenPaused <= 0) {
          timerState.remainingMsWhenPaused = timerState.totalDurationMs;
          timerState.threshold12hTriggered = false;
          timerState.expiredTriggered = false;
        }
        timerState.isRunning = true;
        timerState.endTimestamp = now + timerState.remainingMsWhenPaused;
        saveState();
        broadcastSync('doom_launch');
        break;
      }

      case 'pause': {
        if (!timerState.isRunning) break;
        timerState.remainingMsWhenPaused = Math.max(0, timerState.endTimestamp - now);
        timerState.isRunning = false;
        timerState.endTimestamp = null;
        saveState();
        broadcastSync('clock_paused');
        break;
      }

      case 'reset': {
        const customTotal = typeof msg.totalSeconds === 'number' && msg.totalSeconds > 0
          ? msg.totalSeconds * 1000
          : TOTAL_DURATION_24H_MS;
        timerState.totalDurationMs = customTotal;
        timerState.remainingMsWhenPaused = customTotal;
        timerState.isRunning = Boolean(msg.autoStart);
        timerState.endTimestamp = timerState.isRunning ? now + customTotal : null;
        timerState.threshold12hTriggered = customTotal <= THRESHOLD_12H_MS;
        timerState.expiredTriggered = false;
        timerState.cracks = [];
        saveState();
        broadcastSync('clock_reset');
        break;
      }

      case 'jump': {
        const targetSec = Math.max(0, Number(msg.seconds) || 0);
        const targetMs = targetSec * 1000;
        if (targetMs > timerState.totalDurationMs) {
          timerState.totalDurationMs = targetMs;
        }
        timerState.remainingMsWhenPaused = targetMs;
        const shouldStart = msg.autoStart !== undefined ? Boolean(msg.autoStart) : true;
        if (targetMs > 0 && shouldStart) {
          timerState.isRunning = true;
          timerState.endTimestamp = now + targetMs;
        } else {
          timerState.isRunning = false;
          timerState.endTimestamp = null;
        }
        timerState.threshold12hTriggered = targetMs <= THRESHOLD_12H_MS;
        timerState.expiredTriggered = targetMs <= 0;
        saveState();
        broadcastSync('clock_jumped', { seconds: targetSec });
        break;
      }

      case 'trigger_doom': {
        const crack = addCrack(msg.x, msg.y, 1.15);
        saveState();
        broadcastSync('doom_sequence', { crack });
        break;
      }

      case 'shatter_screen': {
        const crack = addCrack(msg.x, msg.y, msg.scale || 1.0);
        saveState();
        broadcastSync('screen_shatter', { crack });
        break;
      }

      case 'repair_screen': {
        timerState.cracks = [];
        saveState();
        broadcastSync('screen_repaired');
        break;
      }

      case 'trigger_siren': {
        broadcast({
          type: 'event',
          event: 'siren_blast',
          sirenType: msg.sirenType || 'doomsday',
          serverNow: now
        });
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    broadcast({
      type: 'clients_update',
      connectedClients: wss.clients.size,
      serverNow: Date.now()
    });
  });
});

// Server-side canonical threshold evaluator (100ms tick)
setInterval(() => {
  if (!timerState.isRunning || timerState.endTimestamp === null) return;

  const remainingMs = getCurrentRemainingMs();

  // Check 12:00:00 breach
  if (!timerState.threshold12hTriggered && remainingMs <= THRESHOLD_12H_MS && remainingMs > 0) {
    timerState.threshold12hTriggered = true;
    addCrack(0.28, 0.35, 1.2);
    addCrack(0.74, 0.62, 1.1);
    saveState();
    broadcastSync('12h_breach');
  }

  // Check 00:00:00 expiration
  if (!timerState.expiredTriggered && remainingMs <= 0) {
    timerState.expiredTriggered = true;
    timerState.isRunning = false;
    timerState.remainingMsWhenPaused = 0;
    timerState.endTimestamp = null;
    addCrack(0.5, 0.5, 1.5);
    addCrack(0.22, 0.28, 1.25);
    addCrack(0.78, 0.32, 1.25);
    addCrack(0.65, 0.75, 1.3);
    saveState();
    broadcastSync('final_doom');
  }
}, 100);

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n======================================================');
  console.log('  DOOM N DEBUG // CENTRALIZED SURVIVAL CLOCK SERVER');
  console.log('======================================================');
  console.log(`  Local Access:   http://localhost:${PORT}`);
  const lans = getLanUrls();
  if (lans.length > 0) {
    lans.forEach((u) => console.log(`  Network Access: ${u}`));
  }
  console.log(`  Kiosk Mode:     Append ?kiosk=1 for clean display`);
  console.log('======================================================\n');
});
