const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const NodeMediaServer = require('node-media-server');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());

// ==========================================
// RTMP & HTTP-FLV STREAMING SERVER CONFIG
// Each drone streams to its own key, e.g. rtmp://<host>/live/<droneId>
// so any number of drones can stream concurrently without extra config.
// ==========================================
const nmsConfig = {
    rtmp: {
        port: 1935,
        chunk_size: 60000,
        gop_cache: true,
        ping: 30,
        ping_timeout: 60
    },
    http: {
        port: 8000,
        mediaroot: './media',
        allow_origin: '*'
    }
};

const nms = new NodeMediaServer(nmsConfig);
nms.run();

// ==========================================
// 1. DATA STORAGE & LOGGING
// ==========================================
const LOG_FILE = path.join(__dirname, 'drone_log.txt');
const CONTROL_LOG_FILE = path.join(__dirname, 'control_log.txt');
const DEFAULT_DRONE_ID = 'drone-1';
const OFFLINE_AFTER_MS = 15000; // a drone with no telemetry in this window is shown as offline
const DEFAULT_FOV_DEG = 78; // typical FPV/action-cam horizontal field of view

// droneId -> latest telemetry payload (+ bookkeeping)
const drones = {};

// droneId -> latest control command queued for that drone to pick up
const controlState = {};

function getOrCreateControlState(droneId) {
    if (!controlState[droneId]) {
        controlState[droneId] = { commandId: 0, command: null, params: null, updatedAt: null };
    }
    return controlState[droneId];
}

// Normalizes whatever shape "camels" arrived in into an array of
// { id, lat, lng } so the client can always drop red dots on the map.
// Accepts: array of {lat,lng}/{latitude,longitude}, or a bare count/anything
// else (in which case there are no positions to plot, just a count).
function normalizeCamels(raw) {
    if (!Array.isArray(raw)) return [];
    return raw
        .map((c, i) => {
            if (c && typeof c === 'object') {
                const lat = c.lat ?? c.latitude;
                const lng = c.lng ?? c.lon ?? c.longitude;
                if (typeof lat === 'number' && typeof lng === 'number') {
                    return { id: c.id ?? String(i), lat, lng, confidence: c.confidence ?? null };
                }
            }
            return null;
        })
        .filter(Boolean);
}

function fleetSummary() {
    const now = Date.now();
    return Object.keys(drones).map((id) => {
        const d = drones[id];
        const camels = normalizeCamels(d.camels);
        return {
            droneId: id,
            name: d.name || id,
            lat: d.drone?.lat ?? 0,
            lng: d.drone?.lng ?? 0,
            alt: d.drone?.alt ?? 0,
            yaw: d.drone?.yaw ?? 0,
            pitch: d.drone?.pitch ?? 0,
            hasGPSFix: !!d.drone?.hasGPSFix,
            camelCount: Array.isArray(d.camels) ? d.camels.length : 0,
            camels, // full positions (may be empty even if camelCount > 0)
            fovDeg: (typeof d.camera?.fovDeg === 'number') ? d.camera.fovDeg : DEFAULT_FOV_DEG,
            isLive: !!d.isLive,
            streamKey: d.streamKey || id,
            armed: !!d.armed,
            lastUpdate: d.lastUpdate,
            online: (now - (d.lastUpdate || 0)) < OFFLINE_AFTER_MS
        };
    });
}

// ==========================================
// 2. TELEMETRY INGEST (one or many drones)
// ==========================================
// Body shape (droneId optional, defaults to "drone-1" for single-drone setups):
// { droneId, name, clientTimestamp,
//   drone: {lat,lng,yaw,pitch,alt,hasGPSFix},
//   camera: { fovDeg },                      // optional, defaults to 78
//   camels: [{id,lat,lng,confidence}, ...],  // positions, OR a bare count
//   isLive, streamKey, armed }
app.post('/api/telemetry', (req, res) => {
    const serverReceiveTime = Date.now();
    const body = req.body || {};
    const droneId = body.droneId || body.streamKey || DEFAULT_DRONE_ID;
    const clientTimestamp = body.clientTimestamp || serverReceiveTime;
    const networkDelay = serverReceiveTime - clientTimestamp;

    const record = {
        ...body,
        droneId,
        streamKey: body.streamKey || droneId,
        lastUpdate: serverReceiveTime
    };
    drones[droneId] = record;

    const logEntry = `[${new Date().toISOString()}] [${droneId}] [Delay: ${networkDelay}ms] ${JSON.stringify(record)}\n`;
    fs.appendFile(LOG_FILE, logEntry, (err) => {
        if (err) console.error('Failed to write to log file:', err);
    });

    // Emit the normalized shape (with camels positions + fovDeg resolved) so
    // every connected client — including ones that just reconnected — gets
    // a consistent record, whether it came from a live socket push or a
    // fresh /api/drones fetch after a page refresh.
    const emitRecord = {
        ...record,
        camels: normalizeCamels(record.camels),
        camelCount: Array.isArray(record.camels) ? record.camels.length : 0,
        fovDeg: (typeof record.camera?.fovDeg === 'number') ? record.camera.fovDeg : DEFAULT_FOV_DEG
    };
    io.emit('telemetry_update', emitRecord);

    res.status(200).json({
        message: 'Data received and logged',
        droneId,
        serverDelayMs: networkDelay
    });
});

// Snapshot of the whole fleet — used by the dashboard on load,
// before any live socket events have arrived, AND used as a periodic
// resync safety net by the client so a dropped socket event (or a page
// refresh that races a slow first fetch) never makes a drone "disappear".
app.get('/api/drones', (req, res) => {
    res.status(200).json(fleetSummary());
});

// ==========================================
// 3. BASIC DRONE CONTROL
// ==========================================
// The dashboard's control panel posts commands here. Firmware can either
// listen for the 'control_command' socket event, or poll
// GET /api/control/:droneId/latest and act whenever commandId increases.
const VALID_COMMANDS = ['ARM', 'DISARM', 'TAKEOFF', 'LAND', 'RTL', 'SET_GIMBAL', 'SET_YAW', 'EMERGENCY_STOP'];

app.post('/api/control/:droneId', (req, res) => {
    const { droneId } = req.params;
    const { command, params } = req.body || {};

    if (!command || !VALID_COMMANDS.includes(command)) {
        return res.status(400).json({ message: `Unknown command. Valid commands: ${VALID_COMMANDS.join(', ')}` });
    }

    const state = getOrCreateControlState(droneId);
    state.commandId += 1;
    state.command = command;
    state.params = params || null;
    state.updatedAt = Date.now();

    if (command === 'ARM' && drones[droneId]) drones[droneId].armed = true;
    if ((command === 'DISARM' || command === 'EMERGENCY_STOP') && drones[droneId]) drones[droneId].armed = false;

    const logEntry = `[${new Date().toISOString()}] [${droneId}] ${command} ${params ? JSON.stringify(params) : ''}\n`;
    fs.appendFile(CONTROL_LOG_FILE, logEntry, (err) => {
        if (err) console.error('Failed to write to control log file:', err);
    });

    const payload = { droneId, commandId: state.commandId, command, params: state.params, updatedAt: state.updatedAt };
    io.emit('control_command', payload);

    res.status(200).json(payload);
});

app.get('/api/control/:droneId/latest', (req, res) => {
    const state = getOrCreateControlState(req.params.droneId);
    res.status(200).json({ droneId: req.params.droneId, ...state });
});

// ==========================================
// 4. SHARED PAGE CHROME (design tokens + top bar)
// ==========================================
function sharedStyles() {
    return `
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600&display=swap');

        :root {
            --bg: #0d1117;
            --panel: #141a24;
            --panel-raised: #1b2330;
            --border: #262f3d;
            --text: #dde3ed;
            --text-dim: #8592a6;
            --text-faint: #57657a;
            --cyan: #5fd4d0;
            --amber: #e8a33d;
            --red: #e5484d;
            --green: #57c77a;
            --radius: 10px;
            --topbar-h: 66px;
        }

        * { box-sizing: border-box; }

        html {
            height: 100%;
        }

        html, body {
            margin: 0;
            padding: 0;
            background: var(--bg);
            background-image:
                linear-gradient(rgba(255,255,255,0.025) 1px, transparent 1px),
                linear-gradient(90deg, rgba(255,255,255,0.025) 1px, transparent 1px);
            background-size: 28px 28px;
            color: var(--text);
            font-family: 'Inter', -apple-system, sans-serif;
        }

        /* Page never grows taller than the viewport, and never taller than
           980px — it fits the screen instead of forcing outer page scroll. */
        body {
            display: flex;
            flex-direction: column;
            height: 100vh;
            max-height: 980px;
            overflow: hidden;
        }

        .mono { font-family: 'JetBrains Mono', monospace; }
        a { color: inherit; text-decoration: none; }
        ::selection { background: rgba(95,212,208,0.3); }

        .topbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 16px 28px;
            border-bottom: 1px solid var(--border);
            background: rgba(20,26,36,0.7);
            backdrop-filter: blur(6px);
            flex: 0 0 auto;
            height: var(--topbar-h);
        }

        .brand { display: flex; align-items: center; gap: 12px; }

        .brand-mark {
            width: 34px; height: 34px; border-radius: 8px;
            background: linear-gradient(145deg, var(--cyan), #2a8f8c);
            display: flex; align-items: center; justify-content: center;
            font-family: 'Space Grotesk', sans-serif; font-weight: 700;
            color: #06201f; font-size: 15px; flex-shrink: 0;
        }

        .brand-text h1 {
            font-family: 'Space Grotesk', sans-serif; font-size: 16px; font-weight: 600;
            letter-spacing: 0.02em; margin: 0; color: var(--text);
        }

        .brand-text span {
            font-family: 'JetBrains Mono', monospace; font-size: 11px;
            color: var(--text-faint); letter-spacing: 0.04em;
        }

        .topbar-right { display: flex; align-items: center; gap: 14px; }

        .nav-link {
            font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text-dim);
            border: 1px solid var(--border); border-radius: 20px; padding: 6px 12px;
            transition: all 0.12s ease;
        }
        .nav-link:hover { border-color: var(--cyan); color: var(--cyan); }
        .nav-link.active { border-color: var(--cyan); color: var(--cyan); background: rgba(95,212,208,0.08); }

        .fleet-pill {
            font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text-dim);
            border: 1px solid var(--border); border-radius: 20px; padding: 5px 12px;
            display: flex; align-items: center; gap: 6px;
        }

        .conn-pill {
            font-family: 'JetBrains Mono', monospace; font-size: 11px;
            border: 1px solid var(--border); border-radius: 20px; padding: 5px 12px;
            display: flex; align-items: center; gap: 6px; color: var(--text-dim);
        }
        .conn-pill.ok { color: var(--green); border-color: var(--green); }
        .conn-pill.bad { color: var(--red); border-color: var(--red); }

        .dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
        .dot.online { background: var(--green); box-shadow: 0 0 0 0 rgba(87,199,122,0.6); animation: pulse 2s infinite; }
        .dot.offline { background: var(--text-faint); }
        .dot.armed { background: var(--amber); box-shadow: 0 0 0 0 rgba(232,163,61,0.6); animation: pulse 2s infinite; }

        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(87,199,122,0.55); }
            70% { box-shadow: 0 0 0 7px rgba(87,199,122,0); }
            100% { box-shadow: 0 0 0 0 rgba(87,199,122,0); }
        }

        .panel { background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius); display: flex; flex-direction: column; min-height: 0; }

        .panel-header {
            display: flex; align-items: center; justify-content: space-between;
            padding: 14px 18px; border-bottom: 1px solid var(--border);
            flex: 0 0 auto;
        }

        .panel-header h3 {
            font-family: 'Space Grotesk', sans-serif; font-size: 12px; font-weight: 600;
            letter-spacing: 0.08em; text-transform: uppercase; color: var(--text-dim); margin: 0;
        }

        .scrollbar::-webkit-scrollbar { width: 8px; height: 8px; }
        .scrollbar::-webkit-scrollbar-thumb { background: var(--border); border-radius: 8px; }
        .scrollbar::-webkit-scrollbar-track { background: transparent; }

        button { font-family: 'Space Grotesk', sans-serif; cursor: pointer; }
        button:focus-visible, a:focus-visible, input:focus-visible { outline: 2px solid var(--cyan); outline-offset: 2px; }

        @media (prefers-reduced-motion: reduce) {
            .dot.online, .dot.armed { animation: none; }
        }

        /* ---- dashboard layout (fills remaining viewport, capped by body's max-height) ---- */
        .layout {
            flex: 1 1 auto;
            min-height: 0;
            display: grid;
            grid-template-columns: 280px 1fr;
            grid-template-rows: 1fr;
            gap: 20px;
            padding: 20px 28px 28px;
        }
        @media (max-width: 980px) {
            .layout { grid-template-columns: 1fr; grid-template-rows: auto 1fr; }
        }

        .fleet-list { max-height: 100%; overflow-y: auto; }
        .fleet-empty { padding: 24px 18px; color: var(--text-faint); font-size: 13px; line-height: 1.6; }

        .drone-card {
            display: flex; align-items: center; gap: 10px; padding: 12px 18px;
            border-bottom: 1px solid var(--border); cursor: pointer; transition: background 0.15s ease;
            flex: 0 0 auto;
        }
        .drone-card:last-child { border-bottom: none; }
        .drone-card:hover { background: var(--panel-raised); }
        .drone-card.selected { background: var(--panel-raised); box-shadow: inset 3px 0 0 var(--cyan); }
        .drone-card .info { flex: 1; min-width: 0; }
        .drone-card .name { font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; }
        .drone-card .meta { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text-faint); margin-top: 2px; }
        .drone-card .status { font-size: 10px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-faint); }

        /* strict 2x2, each cell fills its share of the viewport height */
        .quad {
            display: grid;
            grid-template-columns: 1fr 1fr;
            grid-template-rows: 1fr 1fr;
            gap: 20px;
            min-height: 0;
        }
        @media (max-width: 980px) { .quad { grid-template-columns: 1fr; grid-template-rows: repeat(4, minmax(220px, 1fr)); overflow-y: auto; } }

        #map { flex: 1 1 auto; min-height: 0; width: 100%; border-radius: 0 0 var(--radius) var(--radius); filter: saturate(0.35) brightness(0.85) contrast(1.05); }

        .map-legend {
            position: absolute; left: 8px; bottom: 8px; z-index: 500;
            background: rgba(13,17,23,0.78); backdrop-filter: blur(3px);
            border: 1px solid var(--border); border-radius: 6px;
            padding: 6px 9px; font-family: 'JetBrains Mono', monospace; font-size: 10px;
            color: var(--text-dim); display: flex; flex-direction: column; gap: 4px;
            pointer-events: none;
        }
        .map-legend .row { display: flex; align-items: center; gap: 6px; }
        .map-legend .swatch { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
        .map-legend .swatch.camel { background: var(--red); box-shadow: 0 0 0 2px rgba(229,72,77,0.3); }
        .map-legend .swatch.fov { background: rgba(95,212,208,0.35); border: 1px solid var(--cyan); border-radius: 2px; }

        .video-container {
            position: relative; width: 100%; flex: 1 1 auto; min-height: 0; background: #05070a;
            overflow: hidden;
            display: flex; align-items: center; justify-content: center;
        }
        video { width: 100%; height: 100%; object-fit: cover; }
        .video-placeholder { color: var(--text-faint); font-size: 12px; font-family: 'JetBrains Mono', monospace; text-align: center; padding: 20px; }

        .stream-controls { display: flex; gap: 8px; align-items: center; padding: 12px 16px; border-top: 1px solid var(--border); flex: 0 0 auto; }
        .stream-controls input {
            flex: 1; background: var(--bg); border: 1px solid var(--border); color: var(--text);
            padding: 8px 10px; border-radius: 6px; font-family: 'JetBrains Mono', monospace; font-size: 12px;
        }
        .stream-controls button {
            background: var(--panel-raised); border: 1px solid var(--border); color: var(--text);
            padding: 8px 14px; border-radius: 6px; font-size: 12px; font-weight: 600;
        }
        .stream-controls button:hover { border-color: var(--cyan); color: var(--cyan); }
        .stream-status { padding: 0 16px 12px; font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text-faint); flex: 0 0 auto; }

        .telemetry-grid { flex: 1 1 auto; min-height: 0; overflow-y: auto; display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); grid-auto-rows: min-content; gap: 1px; background: var(--border); }
        .stat-card { background: var(--panel); padding: 14px 16px; }
        .stat-card h4 { margin: 0 0 6px; font-size: 10px; text-transform: uppercase; letter-spacing: 0.07em; color: var(--text-faint); font-weight: 600; }
        .stat-card .value { font-family: 'JetBrains Mono', monospace; font-size: 18px; font-weight: 500; color: var(--text); }
        .stat-card .value.accent { color: var(--cyan); }
        .stat-card .value.warn { color: var(--amber); }

        /* ---- control panel ---- */
        .control-body { padding: 18px; display: flex; flex-direction: column; gap: 16px; flex: 1 1 auto; min-height: 0; overflow-y: auto; }
        .arm-row { display: flex; gap: 10px; }
        .btn {
            flex: 1; padding: 12px; border-radius: 8px; border: 1px solid var(--border);
            background: var(--panel-raised); color: var(--text); font-size: 12px; font-weight: 600;
            letter-spacing: 0.02em; transition: all 0.12s ease;
        }
        .btn:hover { transform: translateY(-1px); }
        .btn:active { transform: translateY(0); }
        .btn.arm { border-color: var(--green); color: var(--green); }
        .btn.arm:hover { background: rgba(87,199,122,0.12); }
        .btn.disarm { border-color: var(--text-faint); }
        .btn:disabled { opacity: 0.4; cursor: not-allowed; transform: none; }
        .mode-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        .btn.mode { border-color: var(--cyan); color: var(--cyan); }
        .btn.mode:hover { background: rgba(95,212,208,0.1); }
        .btn.stop { background: var(--red); border-color: var(--red); color: #1a0505; font-weight: 700; padding: 14px; }
        .btn.stop:hover { background: #f16569; }
        .slider-block label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-faint); font-weight: 600; }
        .slider-row { display: flex; align-items: center; gap: 14px; margin-top: 8px; }
        input[type="range"] { flex: 1; accent-color: var(--cyan); }
        .slider-value { font-family: 'JetBrains Mono', monospace; font-size: 13px; width: 50px; text-align: right; color: var(--cyan); }
        .yaw-nudges { display: flex; gap: 8px; margin-top: 8px; }
        .btn.nudge { flex: 1; padding: 10px; font-size: 12px; }
        .status-box {
            background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
            padding: 10px 12px; font-family: 'JetBrains Mono', monospace; font-size: 11px;
            color: var(--text-dim); min-height: 20px; flex: 0 0 auto;
        }

        /* ---- fleet wall: per-drone card = video + map inset + telemetry strip ---- */
        .fw-wrap {
            flex: 1 1 auto;
            min-height: 0;
            padding: 20px 28px 28px;
            display: flex;
            flex-direction: column;
            gap: 14px;
        }
        .fw-toolbar {
            display: flex; align-items: center; justify-content: space-between;
            flex: 0 0 auto;
        }
        .fw-toolbar .count {
            font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--text-dim);
        }
        .fw-grid {
            flex: 1 1 auto;
            min-height: 0;
            overflow-y: auto;
            display: grid;
            gap: 16px;
            align-content: start;
        }
        .fw-empty {
            flex: 1 1 auto; display: flex; align-items: center; justify-content: center;
            color: var(--text-faint); font-family: 'JetBrains Mono', monospace; font-size: 13px;
            border: 1px dashed var(--border); border-radius: var(--radius);
        }
        /* Each card is a real flex column (title bar -> video box -> telemetry
           strip), the same pattern the dashboard's Live Feed panel uses. The
           video box gets flex:1 1 auto inside a column with a real pixel
           height (from the grid row), so height:100% on <video> resolves to
           an actual number instead of the 0px it was collapsing to when the
           video sat inside a card that only had min-height set. */
        .fw-card {
            display: flex;
            flex-direction: column;
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            overflow: hidden;
        }
        .fw-card-header {
            display: flex; align-items: center; justify-content: space-between;
            padding: 8px 12px; border-bottom: 1px solid var(--border);
            flex: 0 0 auto;
        }
        .fw-card-header .fw-label {
            display: flex; align-items: center; gap: 6px;
            font-family: 'JetBrains Mono', monospace; font-size: 11px;
        }
        .fw-card .fw-status {
            font-family: 'JetBrains Mono', monospace; font-size: 10px;
            padding: 3px 7px; border-radius: 10px; border: 1px solid var(--border);
            color: var(--text-faint);
        }
        .fw-card .fw-status.live { color: var(--green); border-color: var(--green); }
        .fw-card .fw-status.offline { color: var(--text-faint); }
        .fw-video-box {
            position: relative;
            flex: 1 1 auto;
            min-height: 0;
            background: #05070a;
            overflow: hidden;
            display: flex; align-items: center; justify-content: center;
        }
        .fw-video-box video { width: 100%; height: 100%; object-fit: cover; display: block; }
        .fw-card .fw-placeholder {
            color: var(--text-faint); font-size: 12px; font-family: 'JetBrains Mono', monospace;
            text-align: center; padding: 16px;
        }
        .fw-card .fw-map {
            position: absolute; right: 8px; bottom: 8px; z-index: 3;
            width: 120px; height: 100px;
            border: 1px solid var(--border); border-radius: 6px;
            overflow: hidden;
            filter: saturate(0.35) brightness(0.85) contrast(1.05);
            box-shadow: 0 2px 10px rgba(0,0,0,0.5);
        }
        .fw-card .fw-telemetry {
            flex: 0 0 auto;
            background: var(--panel-raised);
            border-top: 1px solid var(--border);
            display: flex; gap: 14px; flex-wrap: wrap;
            padding: 7px 12px;
            font-family: 'JetBrains Mono', monospace; font-size: 10.5px;
            color: var(--text-dim);
        }
        .fw-card .fw-telemetry b { color: var(--text); font-weight: 600; }
        .fw-card .fw-telemetry .warn { color: var(--amber); }
        .fw-card .fw-telemetry .accent { color: var(--cyan); }
    `;
}

function topBar(fleetCount, activeNav) {
    const navLink = (href, label, key) =>
        `<a class="nav-link ${activeNav === key ? 'active' : ''}" href="${href}">${label}</a>`;
    return `
        <div class="topbar">
            <div class="brand">
                <div class="brand-mark">FF</div>
                <div class="brand-text">
                    <h1>Camel Project</h1>
                    <span>GROUND CONTROL &middot; MULTI-DRONE</span>
                </div>
            </div>
            <div class="topbar-right">
                ${navLink('/dashboard', 'DASHBOARD', 'dashboard')}
                ${navLink('/fleet-wall', 'FLEET WALL', 'fleet-wall')}
                <span class="conn-pill" id="connPill"><span class="dot online"></span><span id="connLabel">connecting…</span></span>
                <span class="fleet-pill"><span class="dot online"></span><span id="fleetCount">${fleetCount}</span> in fleet</span>
            </div>
        </div>
    `;
}

// ==========================================
// 4b. SHARED CLIENT-SIDE HELPERS
// Inlined into every page's <script> block so there is one source of
// truth for: geo math (FOV cone / destination-point), the flv.js player
// wrapper (with the long-flight stall fix), and the socket/connection
// resync logic (the "drone disappears on refresh" fix).
// ==========================================
function clientCoreScript() {
    return `
        // ---------- geo math ----------
        // Great-circle destination point given a start point, bearing, and
        // distance — used to draw the drone's camera FOV footprint on the map.
        function destPoint(lat, lng, bearingDeg, distanceM) {
            const R = 6378137;
            const brng = bearingDeg * Math.PI / 180;
            const lat1 = lat * Math.PI / 180;
            const lng1 = lng * Math.PI / 180;
            const dR = distanceM / R;
            const lat2 = Math.asin(Math.sin(lat1) * Math.cos(dR) + Math.cos(lat1) * Math.sin(dR) * Math.cos(brng));
            const lng2 = lng1 + Math.atan2(
                Math.sin(brng) * Math.sin(dR) * Math.cos(lat1),
                Math.cos(dR) - Math.sin(lat1) * Math.sin(lat2)
            );
            return [lat2 * 180 / Math.PI, lng2 * 180 / Math.PI];
        }

        // Builds a wedge (apex + arc) representing the drone's ground FOV,
        // centered on its heading (yaw). Radius scales with altitude so the
        // footprint grows as the drone climbs.
        function fovPolygon(lat, lng, yawDeg, altM, fovDeg) {
            const radius = Math.max(15, (altM || 10) * 2.2);
            const half = (fovDeg || 78) / 2;
            const steps = 10;
            const pts = [[lat, lng]];
            for (let i = 0; i <= steps; i++) {
                const bearing = (yawDeg || 0) - half + (i * (fovDeg || 78) / steps);
                pts.push(destPoint(lat, lng, bearing, radius));
            }
            pts.push([lat, lng]);
            return pts;
        }

        // ---------- flv.js live player with long-flight stall fix ----------
        // Symptom this fixes: video freezes after streaming for a long time
        // and only comes back after a manual page refresh. Root causes are
        // (a) flv.js's internal source buffer growing unbounded over a long
        // live session, and (b) playback silently drifting behind the live
        // edge with nothing forcing it to catch up. autoCleanupSourceBuffer
        // fixes (a); the watchdog below fixes (b) and also recovers from a
        // stalled decoder without the user having to do anything.
        function createLivePlayer(streamKey, videoEl, onStatus) {
            if (typeof flvjs === 'undefined' || !flvjs.isSupported()) {
                if (onStatus) onStatus('unsupported', false);
                return null;
            }
            const url = 'http://' + window.location.hostname + ':8000/live/' + streamKey + '.flv';
            const player = flvjs.createPlayer(
                { type: 'flv', isLive: true, url: url },
                {
                    enableStashBuffer: false,
                    stashInitialSize: 128,
                    autoCleanupSourceBuffer: true,
                    autoCleanupMaxBackwardDuration: 10,
                    autoCleanupMinBackwardDuration: 6,
                    lazyLoad: false
                }
            );

            let destroyed = false;
            let stallTicks = 0;
            let lastTime = -1;

            function teardown() {
                destroyed = true;
                if (watchdog) clearInterval(watchdog);
                try { player.pause(); player.unload(); player.detachMediaElement(); player.destroy(); } catch (e) {}
            }

            player.on(flvjs.Events.ERROR, (errType, errDetail) => {
                if (onStatus) onStatus('retrying', false);
                if (!destroyed) setTimeout(() => { if (!destroyed) restart(); }, 3000);
            });
            player.on(flvjs.Events.LOADING_COMPLETE, () => { if (onStatus) onStatus('ended', false); });

            videoEl.addEventListener('playing', () => { if (onStatus) onStatus('live', true); }, { once: false });

            // Watches for two failure modes that a plain onerror handler
            // misses: (1) the decoder stalls with no error event, and
            // (2) playback slowly drifts seconds behind the live edge.
            // Every 3s: if currentTime hasn't advanced, count a stall tick;
            // two ticks in a row (~6s frozen) forces a full reconnect.
            // If it HAS advanced but has drifted >4s behind the buffered
            // edge, jump forward instead of tearing down the whole player.
            const watchdog = setInterval(() => {
                if (destroyed || !videoEl.isConnected) { clearInterval(watchdog); return; }
                if (videoEl.paused || videoEl.ended) return;
                if (videoEl.currentTime === lastTime) {
                    stallTicks += 1;
                    if (stallTicks >= 2) {
                        stallTicks = 0;
                        if (onStatus) onStatus('recovering', false);
                        restart();
                        return;
                    }
                } else {
                    stallTicks = 0;
                    const buf = videoEl.buffered;
                    if (buf && buf.length) {
                        const end = buf.end(buf.length - 1);
                        if (end - videoEl.currentTime > 4) {
                            try { videoEl.currentTime = end - 0.5; } catch (e) {}
                        }
                    }
                }
                lastTime = videoEl.currentTime;
            }, 3000);

            function restart() {
                try { player.unload(); } catch (e) {}
                try {
                    player.load();
                    player.play().catch(() => {});
                } catch (e) {
                    // player object is in a bad state — caller should recreate it
                    if (onStatus) onStatus('retrying', false);
                }
            }

            player.attachMediaElement(videoEl);
            player.load();
            player.play().catch(() => { if (onStatus) onStatus('tap to play', false); });

            return { player, destroy: teardown, restart };
        }

        // ---------- socket + resync (the "drone disappears on refresh" fix) ----------
        // A page refresh used to rely on a single fetch('/api/drones') racing
        // against the socket connecting. If that fetch ran before the server
        // had anything, or failed, or the socket reconnected later and missed
        // an event, drones would vanish until the next lucky telemetry packet.
        // This wraps that in: retry-on-failure, a periodic resync poll as a
        // backup to the socket, and a resync on every socket (re)connect.
        function setupResilientFeed(applyFn, connPillId, connLabelId) {
            const socket = io({ reconnection: true, reconnectionDelay: 1000, reconnectionDelayMax: 5000 });

            function setConn(ok, label) {
                const pill = document.getElementById(connPillId);
                const dot = pill ? pill.querySelector('.dot') : null;
                const lab = document.getElementById(connLabelId);
                if (pill) pill.className = 'conn-pill ' + (ok ? 'ok' : 'bad');
                if (dot) dot.className = 'dot ' + (ok ? 'online' : 'offline');
                if (lab) lab.innerText = label;
            }

            function fetchSnapshot(retriesLeft) {
                fetch('/api/drones')
                    .then(r => r.json())
                    .then(list => { list.forEach(applyFn); })
                    .catch(() => {
                        if (retriesLeft > 0) setTimeout(() => fetchSnapshot(retriesLeft - 1), 1500);
                    });
            }

            socket.on('connect', () => {
                setConn(true, 'live');
                // Always resync on (re)connect — this is what stops a missed
                // event during a dropped socket from leaving a stale/missing
                // drone on screen after the socket comes back.
                fetchSnapshot(5);
            });
            socket.on('disconnect', () => setConn(false, 'reconnecting…'));
            socket.on('connect_error', () => setConn(false, 'reconnecting…'));

            socket.on('telemetry_update', applyFn);

            // Backup to the socket: even with a healthy connection, poll the
            // full snapshot periodically so a silently-missed event (or a
            // drone the socket never got a chance to announce) can't leave
            // the fleet list stuck.
            setInterval(() => fetchSnapshot(1), 5000);

            // Kick off the first load immediately, with retries.
            fetchSnapshot(5);

            return socket;
        }
    `;
}

// ==========================================
// 5. DASHBOARD — fleet picker, map, control, live video, telemetry (2x2)
// ==========================================
app.get('/dashboard', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>FFly Command Center</title>
            <script src="/socket.io/socket.io.js"></script>
            <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css" />
            <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/flv.js@1.6.2/dist/flv.min.js"></script>
            <style>${sharedStyles()}</style>
        </head>
        <body>
            ${topBar(0, 'dashboard')}
            <div class="layout">
                <div class="panel fleet-list scrollbar" id="fleetList">
                    <div class="fleet-empty">Waiting for the first telemetry packet from any drone…</div>
                </div>

                <div class="quad">
                    <div class="panel">
                        <div class="panel-header"><h3>Fleet Position</h3><span class="mono" style="font-size:11px;color:var(--text-faint);" id="selectedLabel">no drone selected</span></div>
                        <div style="position:relative; flex:1 1 auto; min-height:0; display:flex;">
                            <div id="map"></div>
                            <div class="map-legend">
                                <div class="row"><span class="swatch fov"></span>camera FOV</div>
                                <div class="row"><span class="swatch camel"></span>camel tracked</div>
                            </div>
                        </div>
                    </div>

                    <div class="panel">
                        <div class="panel-header"><h3>Flight Control</h3><span class="mono" id="targetDroneLabel" style="font-size:11px;color:var(--text-faint);">no target</span></div>
                        <div class="control-body">
                            <div class="arm-row">
                                <button class="btn arm" id="btnArm" disabled>ARM</button>
                                <button class="btn disarm" id="btnDisarm" disabled>DISARM</button>
                            </div>
                            <div class="mode-grid">
                                <button class="btn mode" id="btnTakeoff" disabled>TAKEOFF</button>
                                <button class="btn mode" id="btnLand" disabled>LAND</button>
                                <button class="btn mode" id="btnRtl" disabled style="grid-column: span 2;">RTL</button>
                            </div>
                            <button class="btn stop" id="btnEmergency" disabled>EMERGENCY STOP</button>

                            <div class="slider-block">
                                <label>Gimbal Pitch</label>
                                <div class="slider-row">
                                    <input type="range" id="gimbalPitch" min="-90" max="30" value="0" disabled />
                                    <span class="slider-value" id="gimbalPitchVal">0°</span>
                                </div>
                            </div>

                            <div class="slider-block">
                                <label>Yaw Nudge</label>
                                <div class="yaw-nudges">
                                    <button class="btn nudge" id="btnYawLeft" disabled>&larr; -15°</button>
                                    <button class="btn nudge" id="btnYawReset" disabled>Reset</button>
                                    <button class="btn nudge" id="btnYawRight" disabled>+15° &rarr;</button>
                                </div>
                            </div>

                            <div class="status-box" id="commandStatus">Select a drone on the left to enable controls.</div>
                        </div>
                    </div>

                    <div class="panel">
                        <div class="panel-header"><h3>Live Feed</h3></div>
                        <div class="video-container">
                            <video id="videoElement" muted playsinline></video>
                            <div class="video-placeholder" id="videoPlaceholder">Select a drone to load its stream</div>
                        </div>
                        <div class="stream-controls">
                            <input id="streamKeyInput" type="text" placeholder="stream key" />
                            <button id="loadStreamBtn">Load</button>
                        </div>
                        <div class="stream-status" id="streamStatus">Idle</div>
                    </div>
                    <div class="panel">
                        <div class="panel-header"><h3>Telemetry</h3></div>
                        <div class="telemetry-grid">
                            <div class="stat-card"><h4>Latitude</h4><div class="value accent" id="lat">—</div></div>
                            <div class="stat-card"><h4>Longitude</h4><div class="value accent" id="lng">—</div></div>
                            <div class="stat-card"><h4>Altitude</h4><div class="value" id="alt">—</div></div>
                            <div class="stat-card"><h4>Heading</h4><div class="value" id="yaw">—</div></div>
                            <div class="stat-card"><h4>Gimbal</h4><div class="value" id="pitch">—</div></div>
                            <div class="stat-card"><h4>Camels</h4><div class="value" id="camels">—</div></div>
                            <div class="stat-card"><h4>GPS Fix</h4><div class="value" id="gps">—</div></div>
                            <div class="stat-card"><h4>State</h4><div class="value warn" id="armState">—</div></div>
                        </div>
                    </div>
                </div>
            </div>

            <script>
                ${clientCoreScript()}

                const dronesById = {};
                let selectedDroneId = null;
                let livePlayer = null;
                try { selectedDroneId = localStorage.getItem('ffly_selected_drone') || null; } catch (e) {}

                // ---------- Map ----------
                const map = L.map('map', { zoomControl: true }).setView([22.3098, 39.1065], 17);
                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 22 }).addTo(map);
                const markers = {};
                const fovLayers = {};      // droneId -> L.polygon (camera FOV wedge)
                const camelLayers = {};    // droneId -> L.layerGroup of red dots

                function droneIcon(online, selected) {
                    const ring = selected ? '0 0 0 6px rgba(95,212,208,0.25)' : 'none';
                    return L.divIcon({
                        className: '',
                        html: '<div style="width:14px;height:14px;border-radius:50%;background:' + (online ? '#57c77a' : '#57657a') + ';border:2px solid #0d1117;box-shadow:' + ring + ';"></div>',
                        iconSize: [14, 14],
                        iconAnchor: [7, 7]
                    });
                }

                function fmtNum(n, digits) {
                    return (typeof n === 'number' && !isNaN(n)) ? n.toFixed(digits) : '—';
                }

                // ---------- Fleet list (drone picker) ----------
                function renderFleetList() {
                    const list = document.getElementById('fleetList');
                    const ids = Object.keys(dronesById);
                    if (ids.length === 0) {
                        list.innerHTML = '<div class="fleet-empty">Waiting for the first telemetry packet from any drone…</div>';
                        return;
                    }
                    list.innerHTML = ids.map((id) => {
                        const d = dronesById[id];
                        const online = (Date.now() - (d.lastUpdate || 0)) < 15000;
                        const sel = id === selectedDroneId ? 'selected' : '';
                        const camelCount = Array.isArray(d.camels) ? d.camels.length : (d.camelCount || 0);
                        return '<div class="drone-card ' + sel + '" data-id="' + id + '">' +
                            '<span class="dot ' + (online ? (d.armed ? 'armed' : 'online') : 'offline') + '"></span>' +
                            '<div class="info"><div class="name">' + (d.name || id) + '</div>' +
                            '<div class="meta">' + fmtNum(d.drone?.alt, 1) + 'm &middot; ' + camelCount + ' tracked</div></div>' +
                            '<div class="status">' + (online ? (d.armed ? 'armed' : 'online') : 'offline') + '</div>' +
                        '</div>';
                    }).join('');

                    list.querySelectorAll('.drone-card').forEach((el) => {
                        el.addEventListener('click', () => selectDrone(el.getAttribute('data-id')));
                    });
                }

                function selectDrone(id) {
                    selectedDroneId = id;
                    try { localStorage.setItem('ffly_selected_drone', id); } catch (e) {}
                    document.getElementById('selectedLabel').innerText = id;
                    document.getElementById('targetDroneLabel').innerText = 'target: ' + id;
                    setControlsEnabled(true);
                    renderFleetList();
                    updateTelemetryPanel();
                    const d = dronesById[id];
                    if (d) {
                        const key = d.streamKey || id;
                        document.getElementById('streamKeyInput').value = key;
                        startLiveVideo(key);
                        if (d.drone && d.drone.lat) map.panTo([d.drone.lat, d.drone.lng]);
                    }
                }

                // ---------- Telemetry panel (shows the currently selected drone) ----------
                function updateTelemetryPanel() {
                    const d = selectedDroneId ? dronesById[selectedDroneId] : null;
                    const camelCount = d ? (Array.isArray(d.camels) ? d.camels.length : (d.camelCount || 0)) : null;
                    document.getElementById('lat').innerText = d ? fmtNum(d.drone?.lat, 6) : '—';
                    document.getElementById('lng').innerText = d ? fmtNum(d.drone?.lng, 6) : '—';
                    document.getElementById('alt').innerText = d ? fmtNum(d.drone?.alt, 1) + ' m' : '—';
                    document.getElementById('yaw').innerText = d ? fmtNum(d.drone?.yaw, 1) + '°' : '—';
                    document.getElementById('pitch').innerText = d ? fmtNum(d.drone?.pitch, 1) + '°' : '—';
                    document.getElementById('camels').innerText = camelCount === null ? '—' : camelCount;
                    document.getElementById('gps').innerText = d ? (d.drone?.hasGPSFix ? 'LOCKED' : 'NO FIX') : '—';
                    document.getElementById('armState').innerText = d ? (d.armed ? 'ARMED' : 'STANDBY') : '—';
                }

                function upsertMarker(id, d) {
                    if (!d.drone || !d.drone.lat || !d.drone.lng) return;
                    const online = (Date.now() - (d.lastUpdate || 0)) < 15000;
                    const latlng = [d.drone.lat, d.drone.lng];

                    if (markers[id]) {
                        markers[id].setLatLng(latlng);
                        markers[id].setIcon(droneIcon(online, id === selectedDroneId));
                    } else {
                        markers[id] = L.marker(latlng, { icon: droneIcon(online, id === selectedDroneId) })
                            .addTo(map)
                            .bindPopup(d.name || id)
                            .on('click', () => selectDrone(id));
                    }

                    // Camera FOV wedge, oriented by yaw, sized by altitude.
                    const poly = fovPolygon(d.drone.lat, d.drone.lng, d.drone.yaw, d.drone.alt, d.fovDeg);
                    if (fovLayers[id]) {
                        fovLayers[id].setLatLngs(poly);
                    } else {
                        fovLayers[id] = L.polygon(poly, {
                            color: '#5fd4d0', weight: 1, fillColor: '#5fd4d0', fillOpacity: 0.16, interactive: false
                        }).addTo(map);
                    }

                    // Red dots for every camel currently tracked by this drone.
                    const camels = Array.isArray(d.camels) ? d.camels : [];
                    if (!camelLayers[id]) camelLayers[id] = L.layerGroup().addTo(map);
                    camelLayers[id].clearLayers();
                    camels.forEach((c) => {
                        if (typeof c.lat === 'number' && typeof c.lng === 'number') {
                            L.circleMarker([c.lat, c.lng], {
                                radius: 5, color: '#e5484d', weight: 1, fillColor: '#e5484d', fillOpacity: 0.85, interactive: false
                            }).addTo(camelLayers[id]);
                        }
                    });
                }

                function applyUpdate(data) {
                    const id = data.droneId || 'drone-1';
                    dronesById[id] = data;
                    if (!selectedDroneId) selectDrone(id);
                    upsertMarker(id, data);
                    renderFleetList();

                    // Only re-render the telemetry panel if this update is for the
                    // currently selected drone (or nothing is selected yet).
                    if (!selectedDroneId || id === selectedDroneId) updateTelemetryPanel();

                    const countEl = document.getElementById('fleetCount');
                    if (countEl) countEl.innerText = Object.keys(dronesById).length;

                    // If we had a remembered selection (e.g. from before a
                    // refresh) but hadn't loaded its stream yet, do it now that
                    // its telemetry has arrived.
                    if (id === selectedDroneId && !livePlayer && data.streamKey) {
                        document.getElementById('selectedLabel').innerText = id;
                        document.getElementById('targetDroneLabel').innerText = 'target: ' + id;
                        setControlsEnabled(true);
                        document.getElementById('streamKeyInput').value = data.streamKey;
                        startLiveVideo(data.streamKey);
                    }
                }

                const socket = setupResilientFeed(applyUpdate, 'connPill', 'connLabel');
                socket.on('control_command', (entry) => {
                    if (entry.droneId === selectedDroneId) {
                        setCommandStatus(entry.command + (entry.params ? ' ' + JSON.stringify(entry.params) : '') + ' confirmed (id: ' + entry.commandId + ')', 'var(--green)');
                    }
                });

                // ---------- Live video ----------
                function setStreamStatus(msg, color) {
                    const el = document.getElementById('streamStatus');
                    if (el) {
                        el.innerText = msg;
                        el.style.color = color || 'var(--text-faint)';
                    }
                }

                function startLiveVideo(streamKey) {
                    document.getElementById('videoPlaceholder').style.display = 'none';
                    if (livePlayer) { livePlayer.destroy(); livePlayer = null; }
                    setStreamStatus('Connecting to ' + streamKey + ' …');
                    const videoElement = document.getElementById('videoElement');
                    livePlayer = createLivePlayer(streamKey, videoElement, (status, isLive) => {
                        if (status === 'live') setStreamStatus('Live ●', 'var(--green)');
                        else if (status === 'retrying') setStreamStatus('Stream error — retrying…', 'var(--red)');
                        else if (status === 'recovering') setStreamStatus('Stream stalled — reconnecting…', 'var(--amber)');
                        else if (status === 'ended') setStreamStatus('Stream ended.', 'var(--amber)');
                        else if (status === 'unsupported') setStreamStatus('FLV.js is not supported in this browser.', 'var(--red)');
                        else if (status === 'tap to play') setStreamStatus('Autoplay blocked — click the video to start playback.', 'var(--amber)');
                    });
                }

                document.getElementById('loadStreamBtn').addEventListener('click', () => {
                    const key = document.getElementById('streamKeyInput').value.trim() || (selectedDroneId || 'drone-1');
                    startLiveVideo(key);
                });
                document.getElementById('videoElement').addEventListener('click', () => { if (livePlayer) livePlayer.player.play().catch(() => {}); });

                // ---------- Flight control ----------
                const controlIds = ['btnArm','btnDisarm','btnTakeoff','btnLand','btnRtl','btnEmergency','gimbalPitch','btnYawLeft','btnYawReset','btnYawRight'];
                function setControlsEnabled(enabled) {
                    controlIds.forEach((id) => {
                        const btn = document.getElementById(id);
                        if (btn) btn.disabled = !enabled;
                    });
                }

                function setCommandStatus(msg, color) {
                    const box = document.getElementById('commandStatus');
                    if (box) {
                        box.innerText = msg;
                        box.style.color = color || 'var(--text-dim)';
                    }
                }

                function sendCommand(command, params) {
                    if (!selectedDroneId) {
                        setCommandStatus('No drone selected.', 'var(--red)');
                        return;
                    }
                    setCommandStatus('Sending ' + command + ' to ' + selectedDroneId + '…', 'var(--cyan)');
                    fetch('/api/control/' + encodeURIComponent(selectedDroneId), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ command, params })
                    })
                    .then(r => r.json())
                    .then(res => {
                        if (res.message && res.message.includes('Unknown')) {
                            setCommandStatus(res.message, 'var(--red)');
                        } else {
                            setCommandStatus(command + ' dispatched (id: ' + res.commandId + ')', 'var(--green)');
                        }
                    })
                    .catch(err => setCommandStatus('Failed to send command: ' + err.message, 'var(--red)'));
                }

                document.getElementById('btnArm').addEventListener('click', () => sendCommand('ARM'));
                document.getElementById('btnDisarm').addEventListener('click', () => sendCommand('DISARM'));
                document.getElementById('btnTakeoff').addEventListener('click', () => sendCommand('TAKEOFF', { altitude: 10 }));
                document.getElementById('btnLand').addEventListener('click', () => sendCommand('LAND'));
                document.getElementById('btnRtl').addEventListener('click', () => sendCommand('RTL'));
                document.getElementById('btnEmergency').addEventListener('click', () => {
                    if (confirm('WARNING: Trigger Emergency Stop?')) sendCommand('EMERGENCY_STOP');
                });

                const gimbalSlider = document.getElementById('gimbalPitch');
                const gimbalVal = document.getElementById('gimbalPitchVal');
                if (gimbalSlider) {
                    gimbalSlider.addEventListener('input', (e) => { gimbalVal.innerText = e.target.value + '°'; });
                    gimbalSlider.addEventListener('change', (e) => sendCommand('SET_GIMBAL', { pitchDeg: parseInt(e.target.value, 10) }));
                }

                const yawLeftBtn = document.getElementById('btnYawLeft');
                if (yawLeftBtn) {
                    yawLeftBtn.addEventListener('click', () => sendCommand('SET_YAW', { relative: -15 }));
                    document.getElementById('btnYawReset').addEventListener('click', () => sendCommand('SET_YAW', { absolute: 0 }));
                    document.getElementById('btnYawRight').addEventListener('click', () => sendCommand('SET_YAW', { relative: 15 }));
                }
            </script>
        </body>
        </html>
    `);
});

// Old bookmark/link compatibility — control lives inside the dashboard now.
app.get('/control', (req, res) => {
    res.redirect('/dashboard');
});

// Old bookmark/link compatibility — video wall was merged into fleet wall.
app.get('/video-wall', (req, res) => {
    res.redirect('/fleet-wall');
});

// ==========================================
// 5b. FLEET WALL — one page, one card per drone: video + map inset + telemetry strip
// Every connected drone gets its own tile with its own live flv.js player
// (same stall-recovery wrapper as the dashboard) and its own small map
// inset showing position + FOV wedge + tracked camels. Cards are laid out
// in an explicit CSS grid (not auto-fit) sized to the current drone count,
// so multiple simultaneous videos always render side by side instead of
// collapsing or overlapping.
// ==========================================
app.get('/fleet-wall', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>FFly Fleet Wall</title>
            <script src="/socket.io/socket.io.js"></script>
            <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css" />
            <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/flv.js@1.6.2/dist/flv.min.js"></script>
            <style>${sharedStyles()}</style>
        </head>
        <body>
            ${topBar(0, 'fleet-wall')}
            <div class="fw-wrap">
                <div class="fw-toolbar">
                    <span class="count mono" id="fwCount">0 connections</span>
                    <span class="count mono" id="fwLiveCount">0 live</span>
                </div>
                <div class="fw-grid" id="fwGrid"></div>
                <div class="fw-empty" id="fwEmpty">Waiting for drones to connect…</div>
            </div>

            <script>
                ${clientCoreScript()}

                const dronesById = {};   // droneId -> latest telemetry record
                const players = {};      // droneId -> live player wrapper from createLivePlayer
                const maps = {};         // droneId -> { map, marker, fov, camelLayer }
                const renderState = {};  // droneId -> {online, live, hasFix} as of the last DOM rebuild
                const cardEls = {};      // droneId -> cached element references for this card (see cacheCardEls)
                const OFFLINE_AFTER_MS = 15000;

                function fmtNum(n, digits) {
                    return (typeof n === 'number' && !isNaN(n)) ? n.toFixed(digits) : '—';
                }

                // Turns an arbitrary droneId (which may contain spaces, slashes,
                // or other characters some DJI apps send in a device name) into
                // a safe string for use in an HTML id attribute. Two different
                // real droneIds could otherwise map to colliding DOM ids, or a
                // stray character could produce invalid markup — either of
                // which can throw partway through building the grid and take
                // every other drone's card down with it.
                function safeId(id) {
                    return 'd_' + String(id).replace(/[^a-zA-Z0-9_-]/g, '_');
                }

                // Explicit column count for the current drone count, so cards
                // never collapse to zero width or overlap when several drones
                // are streaming video at once. 1 -> 1 col, 2-4 -> 2 cols,
                // beyond that -> roughly square grid.
                function columnsFor(n) {
                    if (n <= 1) return 1;
                    if (n <= 4) return 2;
                    return Math.ceil(Math.sqrt(n));
                }

                function isOnline(d) {
                    return (Date.now() - (d.lastUpdate || 0)) < OFFLINE_AFTER_MS;
                }

                function computeState(d) {
                    const online = isOnline(d);
                    const hasFix = !!d.drone?.hasGPSFix && typeof d.drone?.lat === 'number';
                    return { online, live: !!d.isLive && online, hasFix };
                }

                function statesEqual(a, b) {
                    return !!a && !!b && a.online === b.online && a.live === b.live && a.hasFix === b.hasFix;
                }

                // ---------- video (shared flv.js wrapper with the stall fix) ----------
                function destroyPlayer(id) {
                    try {
                        if (players[id]) players[id].destroy();
                    } catch (e) {
                        console.error('[fleet-wall] error destroying player for', id, e);
                    } finally {
                        delete players[id];
                    }
                }

                // Wrapped so that a bad stream (missing key, unreachable host,
                // whatever flv.js chokes on for THIS drone) can never throw out
                // of the render loop and abort processing of every other card.
                function startPlayer(id, streamKey, videoEl, statusEl) {
                    destroyPlayer(id);
                    try {
                        players[id] = createLivePlayer(streamKey, videoEl, (status) => {
                            if (!statusEl) return;
                            if (status === 'live') { statusEl.innerText = 'live'; statusEl.className = 'fw-status live'; }
                            else if (status === 'retrying' || status === 'recovering') { statusEl.innerText = status === 'recovering' ? 'reconnecting' : 'retrying'; statusEl.className = 'fw-status offline'; }
                            else if (status === 'unsupported') { statusEl.innerText = 'unsupported'; statusEl.className = 'fw-status offline'; }
                            else if (status === 'tap to play') { statusEl.innerText = 'tap to play'; statusEl.className = 'fw-status offline'; }
                        });
                    } catch (e) {
                        console.error('[fleet-wall] failed to start player for', id, streamKey, e);
                        if (statusEl) { statusEl.innerText = 'error'; statusEl.className = 'fw-status offline'; }
                    }
                }

                // ---------- per-card map inset (FOV wedge + red camel dots) ----------
                function droneIcon(online) {
                    return L.divIcon({
                        className: '',
                        html: '<div style="width:12px;height:12px;border-radius:50%;background:' + (online ? '#57c77a' : '#57657a') + ';border:2px solid #0d1117;"></div>',
                        iconSize: [12, 12],
                        iconAnchor: [6, 6]
                    });
                }

                function destroyMap(id) {
                    const m = maps[id];
                    if (!m) return;
                    try { m.map.remove(); } catch (e) {}
                    delete maps[id];
                }

                // Wrapped for the same reason as startPlayer — a malformed
                // lat/lng or a Leaflet edge case for one drone must not stop
                // the map insets or video for any other drone from rendering.
                function ensureMap(id, d, mapEl) {
                    try {
                        const lat = d.drone?.lat;
                        const lng = d.drone?.lng;
                        if (typeof lat !== 'number' || typeof lng !== 'number') return;

                        const fovPts = fovPolygon(lat, lng, d.drone?.yaw, d.drone?.alt, d.fovDeg);
                        const camels = Array.isArray(d.camels) ? d.camels : [];

                        if (!maps[id]) {
                            const map = L.map(mapEl, {
                                zoomControl: false,
                                dragging: false,
                                scrollWheelZoom: false,
                                doubleClickZoom: false,
                                attributionControl: false
                            }).setView([lat, lng], 17);
                            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 22 }).addTo(map);
                            const marker = L.marker([lat, lng], { icon: droneIcon(isOnline(d)) }).addTo(map);
                            const fov = L.polygon(fovPts, {
                                color: '#5fd4d0', weight: 1, fillColor: '#5fd4d0', fillOpacity: 0.18, interactive: false
                            }).addTo(map);
                            const camelLayer = L.layerGroup().addTo(map);
                            camels.forEach((c) => {
                                if (typeof c.lat === 'number' && typeof c.lng === 'number') {
                                    L.circleMarker([c.lat, c.lng], {
                                        radius: 4, color: '#e5484d', weight: 1, fillColor: '#e5484d', fillOpacity: 0.85, interactive: false
                                    }).addTo(camelLayer);
                                }
                            });
                            maps[id] = { map, marker, fov, camelLayer };
                            // Tiles can render gray until the container has real layout dimensions.
                            requestAnimationFrame(() => { try { map.invalidateSize(); } catch (e) {} });
                            setTimeout(() => { try { map.invalidateSize(); } catch (e) {} }, 250);
                        } else {
                            maps[id].map.setView([lat, lng], maps[id].map.getZoom());
                            maps[id].marker.setLatLng([lat, lng]);
                            maps[id].marker.setIcon(droneIcon(isOnline(d)));
                            maps[id].fov.setLatLngs(fovPts);
                            maps[id].camelLayer.clearLayers();
                            camels.forEach((c) => {
                                if (typeof c.lat === 'number' && typeof c.lng === 'number') {
                                    L.circleMarker([c.lat, c.lng], {
                                        radius: 4, color: '#e5484d', weight: 1, fillColor: '#e5484d', fillOpacity: 0.85, interactive: false
                                    }).addTo(maps[id].camelLayer);
                                }
                            });
                        }
                    } catch (e) {
                        console.error('[fleet-wall] error rendering map for', id, e);
                    }
                }

                // ---------- telemetry fields (shared by full card build + in-place update) ----------
                function telemetryFieldsHTML(d) {
                    const hasFix = !!d.drone?.hasGPSFix && typeof d.drone?.lat === 'number';
                    const camelCount = Array.isArray(d.camels) ? d.camels.length : (d.camelCount || 0);
                    return '<span>alt <b>' + fmtNum(d.drone?.alt, 1) + 'm</b></span>' +
                        '<span>yaw <b>' + fmtNum(d.drone?.yaw, 0) + '°</b></span>' +
                        '<span>gimbal <b>' + fmtNum(d.drone?.pitch, 0) + '°</b></span>' +
                        '<span>🐫 <b class="accent">' + camelCount + '</b></span>' +
                        '<span>gps <b class="' + (hasFix ? '' : 'warn') + '">' + (hasFix ? 'LOCKED' : 'NO FIX') + '</b></span>' +
                        '<span>state <b class="' + (d.armed ? 'warn' : '') + '">' + (d.armed ? 'ARMED' : 'STANDBY') + '</b></span>';
                }

                // ---------- card markup ----------
                // Mirrors the dashboard's Live Feed panel: the <video> element
                // is ALWAYS in the DOM (never conditionally left out), and a
                // placeholder overlay is just hidden/shown on top of it. Names
                // go into a plain text span (escaped via textContent later, not
                // concatenated raw) and every dynamic element uses the sanitized
                // safeId() so no drone's name/id can produce invalid or
                // colliding markup.
                function cardHTML(id, d) {
                    const sid = safeId(id);
                    const online = isOnline(d);
                    const live = !!d.isLive && online;
                    const hasFix = !!d.drone?.hasGPSFix && typeof d.drone?.lat === 'number';
                    const displayName = String(d.name || id).replace(/[<>&]/g, (c) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c]));
                    return '<div class="fw-card" data-id="' + sid + '">' +
                        '<div class="fw-card-header">' +
                            '<div class="fw-label"><span class="dot ' + (online ? (d.armed ? 'armed' : 'online') : 'offline') + '"></span><span class="fw-name">' + displayName + '</span></div>' +
                            '<span class="fw-status ' + (live ? 'live' : 'offline') + '" id="fwstatus-' + sid + '">' + (live ? 'connecting' : (online ? 'no stream' : 'offline')) + '</span>' +
                        '</div>' +
                        '<div class="fw-video-box">' +
                            '<video id="fwvideo-' + sid + '" muted playsinline autoplay style="display:' + (live ? 'block' : 'none') + ';"></video>' +
                            '<div class="fw-placeholder" id="fwplaceholder-' + sid + '" style="display:' + (live ? 'none' : 'block') + ';">' + (online ? 'Connected — not streaming' : 'No signal') + '</div>' +
                            (hasFix ? '<div class="fw-map" id="fwmap-' + sid + '"></div>' : '') +
                        '</div>' +
                        '<div class="fw-telemetry" id="fwtelemetry-' + sid + '">' + telemetryFieldsHTML(d) + '</div>' +
                    '</div>';
                }

                // After the grid's innerHTML is set, this grabs and caches a
                // direct element reference for every drone's card pieces.
                // Using getElementById once here (instead of re-querying the
                // DOM by concatenated id/selector strings on every update)
                // means later code never has to worry about id escaping and
                // never silently no-ops because a lookup string didn't match.
                function cacheCardEls(id) {
                    const sid = safeId(id);
                    cardEls[id] = {
                        root: document.querySelector('.fw-card[data-id="' + sid + '"]'),
                        video: document.getElementById('fwvideo-' + sid),
                        placeholder: document.getElementById('fwplaceholder-' + sid),
                        status: document.getElementById('fwstatus-' + sid),
                        map: document.getElementById('fwmap-' + sid),
                        telemetry: document.getElementById('fwtelemetry-' + sid)
                    };
                }

                // Rebuilds the whole grid. Only called when a drone joins/leaves,
                // or its online/live/GPS-fix status changes — never on a plain
                // telemetry tick — so a playing video is never torn down and
                // reconnected just because a position update came in. Also
                // sets an explicit column/row count sized to the drone count,
                // so multiple simultaneous videos always lay out side by side.
                //
                // Every per-drone step (video start, map render) is wrapped in
                // its own try/catch inside the loop below, so a failure for one
                // drone (bad stream key, malformed telemetry, whatever) is
                // logged and skipped instead of throwing out of the forEach and
                // silently aborting setup for every drone processed after it.
                function renderGrid() {
                    const ids = Object.keys(dronesById);
                    const grid = document.getElementById('fwGrid');
                    const empty = document.getElementById('fwEmpty');

                    if (ids.length === 0) {
                        grid.style.display = 'none';
                        empty.style.display = 'flex';
                        Object.keys(players).forEach(destroyPlayer);
                        Object.keys(maps).forEach(destroyMap);
                        Object.keys(renderState).forEach((id) => delete renderState[id]);
                        Object.keys(cardEls).forEach((id) => delete cardEls[id]);
                        updateCounts();
                        return;
                    }
                    grid.style.display = 'grid';
                    empty.style.display = 'none';

                    const cols = columnsFor(ids.length);
                    const rows = Math.ceil(ids.length / cols);
                    grid.style.gridTemplateColumns = 'repeat(' + cols + ', 1fr)';
                    grid.style.gridTemplateRows = 'repeat(' + rows + ', minmax(220px, 1fr))';

                    grid.innerHTML = ids.map((id) => {
                        try {
                            return cardHTML(id, dronesById[id]);
                        } catch (e) {
                            console.error('[fleet-wall] failed to build card for', id, e);
                            return '';
                        }
                    }).join('');

                    Object.keys(players).forEach((id) => {
                        if (!dronesById[id] || !dronesById[id].isLive || !isOnline(dronesById[id])) destroyPlayer(id);
                    });
                    Object.keys(maps).forEach((id) => {
                        if (!dronesById[id]) destroyMap(id);
                    });

                    ids.forEach((id) => {
                        try {
                            cacheCardEls(id);
                            const d = dronesById[id];
                            const els = cardEls[id];
                            if (!els || !els.root) return; // this card's HTML failed to build; skip it, don't touch others

                            if (d.isLive && isOnline(d) && !players[id] && els.video) {
                                startPlayer(id, d.streamKey || id, els.video, els.status);
                            }

                            if (els.map) ensureMap(id, d, els.map);

                            renderState[id] = computeState(d);
                        } catch (e) {
                            console.error('[fleet-wall] error setting up card for', id, e);
                        }
                    });

                    Object.keys(renderState).forEach((id) => { if (!dronesById[id]) delete renderState[id]; });
                    Object.keys(cardEls).forEach((id) => { if (!dronesById[id]) delete cardEls[id]; });

                    updateCounts();
                }

                function updateCounts() {
                    const ids = Object.keys(dronesById);
                    document.getElementById('fwCount').innerText = ids.length + ' connection' + (ids.length === 1 ? '' : 's');
                    const liveCount = ids.filter((id) => dronesById[id].isLive && isOnline(dronesById[id])).length;
                    document.getElementById('fwLiveCount').innerText = liveCount + ' live';
                    const countEl = document.getElementById('fleetCount');
                    if (countEl) countEl.innerText = ids.length;
                }

                // Cheap per-tick update: refreshes telemetry numbers, the armed
                // dot, and the map marker/FOV/camel-dot positions in place —
                // never touches the video element, so a playing stream is never
                // interrupted by an ordinary telemetry packet. Uses the cached
                // element references from cacheCardEls instead of re-querying
                // the DOM, and is wrapped so a bad update for one drone can't
                // break the periodic sweep for the rest of the fleet.
                function updateCardInPlace(id, d) {
                    try {
                        const els = cardEls[id];
                        if (!els || !els.root) { renderGrid(); return; }

                        const online = isOnline(d);
                        const dot = els.root.querySelector('.fw-label .dot');
                        if (dot) dot.className = 'dot ' + (online ? (d.armed ? 'armed' : 'online') : 'offline');

                        const nameEl = els.root.querySelector('.fw-name');
                        if (nameEl) nameEl.textContent = d.name || id;

                        if (els.telemetry) els.telemetry.innerHTML = telemetryFieldsHTML(d);

                        if (els.map) ensureMap(id, d, els.map);
                    } catch (e) {
                        console.error('[fleet-wall] error updating card in place for', id, e);
                    }
                }

                // Decides whether this update needs a full grid rebuild (a
                // drone's online/live/GPS-fix status changed, or it's new) or
                // just a lightweight in-place refresh — this is what stops the
                // video from being torn down and reconnected on every telemetry
                // packet.
                function refresh(id) {
                    const d = dronesById[id];
                    if (!d) return;
                    const newState = computeState(d);
                    const isNew = !(id in renderState);
                    if (isNew || !statesEqual(renderState[id], newState)) {
                        renderGrid();
                    } else {
                        updateCardInPlace(id, d);
                        updateCounts();
                    }
                }

                function applyUpdate(data) {
                    const id = data.droneId || 'drone-1';
                    dronesById[id] = data;
                    try {
                        refresh(id);
                    } catch (e) {
                        console.error('[fleet-wall] error applying update for', id, e);
                    }
                }

                setupResilientFeed(applyUpdate, 'connPill', 'connLabel');

                // Periodic sweep to catch a drone going offline purely from the
                // passage of time (no new packet needed to notice a timeout).
                // Uses the same change-detection as refresh(), so it still
                // won't touch a live video unless a status actually flipped.
                setInterval(() => {
                    Object.keys(dronesById).forEach((id) => {
                        try { refresh(id); } catch (e) { console.error('[fleet-wall] error in periodic refresh for', id, e); }
                    });
                }, 5000);

                // Recompute map sizing on window resize so insets in a
                // multi-column grid stay correctly rendered.
                window.addEventListener('resize', () => {
                    Object.values(maps).forEach((m) => { try { m.map.invalidateSize(); } catch (e) {} });
                });
            </script>
        </body>
        </html>
    `);
});

// ==========================================
// 6. SERVER INITIALIZATION
// ==========================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`============================================`);
    console.log(`FFly Ground Control Server active on port ${PORT}`);
    console.log(`- RTMP ingest: rtmp://localhost:1935/live/<droneId>`);
    console.log(`- HTTP-FLV stream: http://localhost:8000/live/<droneId>.flv`);
    console.log(`- Dashboard (map + control + live + telemetry): http://localhost:3000/dashboard`);
    console.log(`- Fleet wall (per-drone card: video + map + telemetry): http://localhost:3000/fleet-wall`);
    console.log(`============================================`);
});
