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

function fleetSummary() {
    const now = Date.now();
    return Object.keys(drones).map((id) => {
        const d = drones[id];
        return {
            droneId: id,
            name: d.name || id,
            lat: d.drone?.lat ?? 0,
            lng: d.drone?.lng ?? 0,
            alt: d.drone?.alt ?? 0,
            yaw: d.drone?.yaw ?? 0,
            pitch: d.drone?.pitch ?? 0,
            hasGPSFix: !!d.drone?.hasGPSFix,
            camels: Array.isArray(d.camels) ? d.camels.length : 0,
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
// { droneId, name, clientTimestamp, drone: {lat,lng,yaw,pitch,alt,hasGPSFix}, camels: [...], isLive, streamKey, armed }
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

    io.emit('telemetry_update', record);

    res.status(200).json({
        message: 'Data received and logged',
        droneId,
        serverDelayMs: networkDelay
    });
});

// Snapshot of the whole fleet — used by the dashboard on load,
// before any live socket events have arrived.
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

        /* ---- video wall ---- */
        .wall-wrap {
            flex: 1 1 auto;
            min-height: 0;
            padding: 20px 28px 28px;
            display: flex;
            flex-direction: column;
            gap: 14px;
        }
        .wall-toolbar {
            display: flex; align-items: center; justify-content: space-between;
            flex: 0 0 auto;
        }
        .wall-toolbar .count {
            font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--text-dim);
        }
        .wall-grid {
            flex: 1 1 auto;
            min-height: 0;
            display: grid;
            gap: 14px;
        }
        .wall-empty {
            flex: 1 1 auto; display: flex; align-items: center; justify-content: center;
            color: var(--text-faint); font-family: 'JetBrains Mono', monospace; font-size: 13px;
            border: 1px dashed var(--border); border-radius: var(--radius);
        }
        .wall-tile {
            position: relative;
            background: #05070a;
            border: 1px solid var(--border);
            border-radius: var(--radius);
            overflow: hidden;
            display: flex;
            min-height: 0;
        }
        .wall-tile video { width: 100%; height: 100%; object-fit: cover; }
        .wall-tile .tile-placeholder {
            margin: auto; color: var(--text-faint); font-size: 12px;
            font-family: 'JetBrains Mono', monospace; text-align: center; padding: 16px;
        }
        .wall-tile .tile-label {
            position: absolute; left: 8px; top: 8px; z-index: 2;
            background: rgba(13,17,23,0.72); backdrop-filter: blur(3px);
            border: 1px solid var(--border); border-radius: 6px;
            padding: 4px 8px; font-family: 'JetBrains Mono', monospace; font-size: 11px;
            display: flex; align-items: center; gap: 6px;
        }
        .wall-tile .tile-status {
            position: absolute; right: 8px; top: 8px; z-index: 2;
            font-family: 'JetBrains Mono', monospace; font-size: 10px;
            padding: 3px 7px; border-radius: 10px; border: 1px solid var(--border);
            background: rgba(13,17,23,0.72); color: var(--text-faint);
        }
        .wall-tile .tile-status.live { color: var(--green); border-color: var(--green); }
        .wall-tile .tile-status.offline { color: var(--text-faint); }

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
            grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
            grid-auto-rows: minmax(220px, 1fr);
            gap: 16px;
            align-content: start;
        }
        .fw-empty {
            flex: 1 1 auto; display: flex; align-items: center; justify-content: center;
            color: var(--text-faint); font-family: 'JetBrains Mono', monospace; font-size: 13px;
            border: 1px dashed var(--border); border-radius: var(--radius);
        }
        .fw-card {
            position: relative;
            aspect-ratio: 16 / 10;
            background: #05070a;
            border: 1px solid var(--border);
            border-radius: var(--radius);
            overflow: hidden;
        }
        .fw-card video { width: 100%; height: 100%; object-fit: cover; display: block; }
        .fw-card .fw-placeholder {
            position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
            color: var(--text-faint); font-size: 12px; font-family: 'JetBrains Mono', monospace;
            text-align: center; padding: 16px;
        }
        .fw-card .fw-label {
            position: absolute; left: 8px; top: 8px; z-index: 3;
            background: rgba(13,17,23,0.72); backdrop-filter: blur(3px);
            border: 1px solid var(--border); border-radius: 6px;
            padding: 4px 8px; font-family: 'JetBrains Mono', monospace; font-size: 11px;
            display: flex; align-items: center; gap: 6px;
        }
        .fw-card .fw-status {
            position: absolute; right: 8px; top: 8px; z-index: 3;
            font-family: 'JetBrains Mono', monospace; font-size: 10px;
            padding: 3px 7px; border-radius: 10px; border: 1px solid var(--border);
            background: rgba(13,17,23,0.72); color: var(--text-faint);
        }
        .fw-card .fw-status.live { color: var(--green); border-color: var(--green); }
        .fw-card .fw-status.offline { color: var(--text-faint); }
        .fw-card .fw-map {
            position: absolute; right: 8px; bottom: 40px; z-index: 3;
            width: 110px; height: 90px;
            border: 1px solid var(--border); border-radius: 6px;
            overflow: hidden;
            filter: saturate(0.35) brightness(0.85) contrast(1.05);
            box-shadow: 0 2px 10px rgba(0,0,0,0.5);
        }
        .fw-card .fw-telemetry {
            position: absolute; left: 0; right: 0; bottom: 0; z-index: 3;
            background: rgba(13,17,23,0.82); backdrop-filter: blur(4px);
            border-top: 1px solid var(--border);
            display: flex; gap: 14px; flex-wrap: wrap;
            padding: 7px 10px;
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
                ${navLink('/video-wall', 'VIDEO WALL', 'video-wall')}
                <span class="fleet-pill"><span class="dot online"></span><span id="fleetCount">${fleetCount}</span> in fleet</span>
            </div>
        </div>
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
                        <div id="map"></div>
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
                const socket = io();
                const dronesById = {};
                let selectedDroneId = null;
                let flvPlayer = null;
                let retryTimer = null;

                // ---------- Map ----------
                const map = L.map('map', { zoomControl: true }).setView([22.3098, 39.1065], 17);
                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 22 }).addTo(map);
                const markers = {};

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
                        return '<div class="drone-card ' + sel + '" data-id="' + id + '">' +
                            '<span class="dot ' + (online ? (d.armed ? 'armed' : 'online') : 'offline') + '"></span>' +
                            '<div class="info"><div class="name">' + (d.name || id) + '</div>' +
                            '<div class="meta">' + fmtNum(d.drone?.alt, 1) + 'm &middot; ' + (Array.isArray(d.camels) ? d.camels.length : 0) + ' tracked</div></div>' +
                            '<div class="status">' + (online ? (d.armed ? 'armed' : 'online') : 'offline') + '</div>' +
                        '</div>';
                    }).join('');

                    list.querySelectorAll('.drone-card').forEach((el) => {
                        el.addEventListener('click', () => selectDrone(el.getAttribute('data-id')));
                    });
                }

                function selectDrone(id) {
                    selectedDroneId = id;
                    document.getElementById('selectedLabel').innerText = id;
                    document.getElementById('targetDroneLabel').innerText = 'target: ' + id;
                    setControlsEnabled(true);
                    renderFleetList();
                    updateTelemetryPanel();
                    const d = dronesById[id];
                    if (d) {
                        const key = d.streamKey || id;
                        document.getElementById('streamKeyInput').value = key;
                        startFlvPlayer(key);
                        if (d.drone && d.drone.lat) map.panTo([d.drone.lat, d.drone.lng]);
                    }
                }

                // ---------- Telemetry panel (shows the currently selected drone) ----------
                function updateTelemetryPanel() {
                    const d = selectedDroneId ? dronesById[selectedDroneId] : null;
                    document.getElementById('lat').innerText = d ? fmtNum(d.drone?.lat, 6) : '—';
                    document.getElementById('lng').innerText = d ? fmtNum(d.drone?.lng, 6) : '—';
                    document.getElementById('alt').innerText = d ? fmtNum(d.drone?.alt, 1) + ' m' : '—';
                    document.getElementById('yaw').innerText = d ? fmtNum(d.drone?.yaw, 1) + '°' : '—';
                    document.getElementById('pitch').innerText = d ? fmtNum(d.drone?.pitch, 1) + '°' : '—';
                    document.getElementById('camels').innerText = d && Array.isArray(d.camels) ? d.camels.length : '—';
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
                }

                fetch('/api/drones').then(r => r.json()).then((list) => {
                    list.forEach((d) => applyUpdate({ ...d, drone: { lat: d.lat, lng: d.lng, yaw: d.yaw, pitch: d.pitch, alt: d.alt, hasGPSFix: d.hasGPSFix } }));
                }).catch(() => {});

                socket.on('telemetry_update', applyUpdate);

                // ---------- Live video ----------
                function setStreamStatus(msg, color) {
                    const el = document.getElementById('streamStatus');
                    if (el) {
                        el.innerText = msg;
                        el.style.color = color || 'var(--text-faint)';
                    }
                }

                function startFlvPlayer(streamKey) {
                    document.getElementById('videoPlaceholder').style.display = 'none';
                    if (typeof flvjs === 'undefined' || !flvjs.isSupported()) {
                        setStreamStatus('FLV.js is not supported in this browser.', 'var(--red)');
                        return;
                    }
                    if (retryTimer) { clearTimeout(retryTimer); retryTimer = null; }
                    if (flvPlayer) {
                        try { flvPlayer.pause(); flvPlayer.unload(); flvPlayer.detachMediaElement(); flvPlayer.destroy(); } catch (e) {}
                        flvPlayer = null;
                    }
                    const url = 'http://' + window.location.hostname + ':8000/live/' + streamKey + '.flv';
                    setStreamStatus('Connecting to ' + streamKey + ' …');

                    const videoElement = document.getElementById('videoElement');
                    flvPlayer = flvjs.createPlayer({ type: 'flv', isLive: true, url: url });
                    flvPlayer.on(flvjs.Events.ERROR, (errType, errDetail) => {
                        setStreamStatus('Stream error: ' + errType + ' — retrying in 3s…', 'var(--red)');
                        retryTimer = setTimeout(() => startFlvPlayer(streamKey), 3000);
                    });
                    flvPlayer.on(flvjs.Events.LOADING_COMPLETE, () => setStreamStatus('Stream ended.', 'var(--amber)'));
                    videoElement.addEventListener('playing', () => setStreamStatus('Live ●', 'var(--green)'), { once: true });

                    flvPlayer.attachMediaElement(videoElement);
                    flvPlayer.load();
                    flvPlayer.play().catch(() => setStreamStatus('Autoplay blocked — click the video to start playback.', 'var(--amber)'));
                }

                document.getElementById('loadStreamBtn').addEventListener('click', () => {
                    const key = document.getElementById('streamKeyInput').value.trim() || (selectedDroneId || 'drone-1');
                    startFlvPlayer(key);
                });
                document.getElementById('videoElement').addEventListener('click', () => { if (flvPlayer) flvPlayer.play().catch(() => {}); });

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

                socket.on('control_command', (entry) => {
                    if (entry.droneId === selectedDroneId) {
                        setCommandStatus(entry.command + (entry.params ? ' ' + JSON.stringify(entry.params) : '') + ' confirmed (id: ' + entry.commandId + ')', 'var(--green)');
                    }
                });
            </script>
        </body>
        </html>
    `);
});

// Old bookmark/link compatibility — control lives inside the dashboard now.
app.get('/control', (req, res) => {
    res.redirect('/dashboard');
});

// ==========================================
// 5b. VIDEO WALL — every connected drone's live feed at once
// ==========================================
app.get('/video-wall', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>FFly Video Wall</title>
            <script src="/socket.io/socket.io.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/flv.js@1.6.2/dist/flv.min.js"></script>
            <style>${sharedStyles()}</style>
        </head>
        <body>
            ${topBar(0, 'video-wall')}
            <div class="wall-wrap">
                <div class="wall-toolbar">
                    <span class="count mono" id="wallCount">0 connections</span>
                    <span class="count mono" id="wallLiveCount">0 live</span>
                </div>
                <div class="wall-grid" id="wallGrid"></div>
                <div class="wall-empty" id="wallEmpty">Waiting for drones to connect…</div>
            </div>

            <script>
                const socket = io();
                const dronesById = {};   // droneId -> latest telemetry record
                const players = {};      // droneId -> flv.js player instance
                const renderState = {};  // droneId -> {online, live} as of the last DOM rebuild
                const OFFLINE_AFTER_MS = 15000;

                function fmtNum(n, digits) {
                    return (typeof n === 'number' && !isNaN(n)) ? n.toFixed(digits) : '—';
                }

                function isOnline(d) {
                    return (Date.now() - (d.lastUpdate || 0)) < OFFLINE_AFTER_MS;
                }

                function computeState(d) {
                    const online = isOnline(d);
                    return { online, live: !!d.isLive && online };
                }

                function statesEqual(a, b) {
                    return !!a && !!b && a.online === b.online && a.live === b.live;
                }

                function columnsFor(n) {
                    if (n <= 1) return 1;
                    return Math.ceil(Math.sqrt(n));
                }

                function destroyPlayer(id) {
                    const p = players[id];
                    if (!p) return;
                    try { p.pause(); p.unload(); p.detachMediaElement(); p.destroy(); } catch (e) {}
                    delete players[id];
                }

                function startPlayer(id, streamKey, videoEl, statusEl) {
                    destroyPlayer(id);
                    if (typeof flvjs === 'undefined' || !flvjs.isSupported()) {
                        if (statusEl) { statusEl.innerText = 'unsupported'; statusEl.className = 'tile-status offline'; }
                        return;
                    }
                    const url = 'http://' + window.location.hostname + ':8000/live/' + streamKey + '.flv';
                    const player = flvjs.createPlayer({ type: 'flv', isLive: true, url: url });
                    players[id] = player;
                    player.on(flvjs.Events.ERROR, () => {
                        if (statusEl) { statusEl.innerText = 'retrying'; statusEl.className = 'tile-status offline'; }
                        setTimeout(() => { if (dronesById[id] && dronesById[id].isLive) startPlayer(id, streamKey, videoEl, statusEl); }, 3000);
                    });
                    videoEl.addEventListener('playing', () => {
                        if (statusEl) { statusEl.innerText = 'live'; statusEl.className = 'tile-status live'; }
                    }, { once: true });
                    player.attachMediaElement(videoEl);
                    player.load();
                    player.play().catch(() => {});
                }

                function tileHTML(id, d) {
                    const online = isOnline(d);
                    const live = !!d.isLive && online;
                    return '<div class="wall-tile" data-id="' + id + '">' +
                        '<div class="tile-label"><span class="dot ' + (online ? 'online' : 'offline') + '"></span>' + (d.name || id) + '</div>' +
                        '<span class="tile-status ' + (live ? 'live' : 'offline') + '" id="status-' + id + '">' + (live ? 'connecting' : (online ? 'no stream' : 'offline')) + '</span>' +
                        (live ? '<video id="video-' + id + '" muted playsinline></video>' : '<div class="tile-placeholder">' + (online ? 'Connected — not streaming' : 'No signal') + '</div>') +
                    '</div>';
                }

                // Rebuilds the whole grid. Only called when a drone joins/leaves
                // or its online/live status actually changes — never on a plain
                // telemetry tick — so a playing video is never torn down and
                // reconnected just because a position update came in.
                function renderGrid() {
                    const ids = Object.keys(dronesById);
                    const grid = document.getElementById('wallGrid');
                    const empty = document.getElementById('wallEmpty');

                    if (ids.length === 0) {
                        grid.style.display = 'none';
                        empty.style.display = 'flex';
                        Object.keys(players).forEach(destroyPlayer);
                        Object.keys(renderState).forEach((id) => delete renderState[id]);
                        updateCounts();
                        return;
                    }
                    grid.style.display = 'grid';
                    empty.style.display = 'none';

                    const cols = columnsFor(ids.length);
                    const rows = Math.ceil(ids.length / cols);
                    grid.style.gridTemplateColumns = 'repeat(' + cols + ', 1fr)';
                    grid.style.gridTemplateRows = 'repeat(' + rows + ', 1fr)';

                    grid.innerHTML = ids.map((id) => tileHTML(id, dronesById[id])).join('');

                    Object.keys(players).forEach((id) => {
                        if (!dronesById[id] || !dronesById[id].isLive || !isOnline(dronesById[id])) destroyPlayer(id);
                    });

                    ids.forEach((id) => {
                        const d = dronesById[id];
                        if (d.isLive && isOnline(d) && !players[id]) {
                            const videoEl = document.getElementById('video-' + id);
                            const statusEl = document.getElementById('status-' + id);
                            if (videoEl) startPlayer(id, d.streamKey || id, videoEl, statusEl);
                        }
                        renderState[id] = computeState(d);
                    });

                    Object.keys(renderState).forEach((id) => { if (!dronesById[id]) delete renderState[id]; });

                    updateCounts();
                }

                function updateCounts() {
                    const ids = Object.keys(dronesById);
                    document.getElementById('wallCount').innerText = ids.length + ' connection' + (ids.length === 1 ? '' : 's');
                    const liveCount = ids.filter((id) => dronesById[id].isLive && isOnline(dronesById[id])).length;
                    document.getElementById('wallLiveCount').innerText = liveCount + ' live';
                    const countEl = document.getElementById('fleetCount');
                    if (countEl) countEl.innerText = ids.length;
                }

                // Cheap per-tick update: only refreshes the drone's name label
                // in place. Anything that affects layout (online/live/new/gone)
                // goes through renderGrid() instead.
                function updateTileInPlace(id, d) {
                    const tile = document.querySelector('.wall-tile[data-id="' + id + '"]');
                    if (!tile) return;
                    const label = tile.querySelector('.tile-label');
                    if (label && label.lastChild && label.lastChild.nodeType === Node.TEXT_NODE) {
                        label.lastChild.textContent = d.name || id;
                    }
                }

                // Decides whether this update needs a full grid rebuild (a
                // drone's online/live status changed, or it's new) or just a
                // lightweight in-place refresh — this is what stops the video
                // from being torn down and reconnected on every telemetry packet.
                function refresh(id) {
                    const d = dronesById[id];
                    if (!d) return;
                    const newState = computeState(d);
                    const isNew = !(id in renderState);
                    if (isNew || !statesEqual(renderState[id], newState)) {
                        renderGrid();
                    } else {
                        updateTileInPlace(id, d);
                        updateCounts();
                    }
                }

                function applyUpdate(data) {
                    const id = data.droneId || 'drone-1';
                    dronesById[id] = data;
                    refresh(id);
                }

                fetch('/api/drones').then(r => r.json()).then((list) => {
                    list.forEach((d) => applyUpdate(d));
                }).catch(() => {});

                socket.on('telemetry_update', applyUpdate);

                // Periodic sweep to catch a drone going offline purely from the
                // passage of time (no new packet needed to notice a timeout).
                // Uses the same change-detection as refresh(), so it still won't
                // touch a live video unless a status actually flipped.
                setInterval(() => {
                    Object.keys(dronesById).forEach(refresh);
                }, 5000);
            </script>
        </body>
        </html>
    `);
});

// ==========================================
// 5c. FLEET WALL — one page, one card per drone: video + map inset + telemetry strip
// No shared map. Same flv.js live-video system as the dashboard/video-wall pages.
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
                const socket = io();
                const dronesById = {};   // droneId -> latest telemetry record
                const players = {};      // droneId -> flv.js player instance
                const maps = {};         // droneId -> { map, marker }
                const renderState = {};  // droneId -> {online, live, hasFix} as of the last DOM rebuild
                const OFFLINE_AFTER_MS = 15000;

                function fmtNum(n, digits) {
                    return (typeof n === 'number' && !isNaN(n)) ? n.toFixed(digits) : '—';
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

                // ---------- video (same flv.js pattern as dashboard/video-wall) ----------
                function destroyPlayer(id) {
                    const p = players[id];
                    if (!p) return;
                    try { p.pause(); p.unload(); p.detachMediaElement(); p.destroy(); } catch (e) {}
                    delete players[id];
                }

                function startPlayer(id, streamKey, videoEl, statusEl) {
                    destroyPlayer(id);
                    if (typeof flvjs === 'undefined' || !flvjs.isSupported()) {
                        if (statusEl) { statusEl.innerText = 'unsupported'; statusEl.className = 'fw-status offline'; }
                        return;
                    }
                    const url = 'http://' + window.location.hostname + ':8000/live/' + streamKey + '.flv';
                    const player = flvjs.createPlayer({ type: 'flv', isLive: true, url: url });
                    players[id] = player;
                    player.on(flvjs.Events.ERROR, () => {
                        if (statusEl) { statusEl.innerText = 'retrying'; statusEl.className = 'fw-status offline'; }
                        setTimeout(() => { if (dronesById[id] && dronesById[id].isLive) startPlayer(id, streamKey, videoEl, statusEl); }, 3000);
                    });
                    videoEl.addEventListener('playing', () => {
                        if (statusEl) { statusEl.innerText = 'live'; statusEl.className = 'fw-status live'; }
                    }, { once: true });
                    player.attachMediaElement(videoEl);
                    player.load();
                    player.play().catch(() => {});
                }

                // ---------- per-card map inset ----------
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

                function ensureMap(id, d, mapEl) {
                    const lat = d.drone?.lat;
                    const lng = d.drone?.lng;
                    if (typeof lat !== 'number' || typeof lng !== 'number') return;

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
                        maps[id] = { map, marker };
                        // Tiles can render gray until the container has real layout dimensions.
                        requestAnimationFrame(() => map.invalidateSize());
                    } else {
                        maps[id].map.setView([lat, lng], maps[id].map.getZoom());
                        maps[id].marker.setLatLng([lat, lng]);
                        maps[id].marker.setIcon(droneIcon(isOnline(d)));
                    }
                }

                // ---------- telemetry fields (shared by full card build + in-place update) ----------
                function telemetryFieldsHTML(d) {
                    const hasFix = !!d.drone?.hasGPSFix && typeof d.drone?.lat === 'number';
                    return '<span>alt <b>' + fmtNum(d.drone?.alt, 1) + 'm</b></span>' +
                        '<span>yaw <b>' + fmtNum(d.drone?.yaw, 0) + '°</b></span>' +
                        '<span>gimbal <b>' + fmtNum(d.drone?.pitch, 0) + '°</b></span>' +
                        '<span>🐫 <b class="accent">' + (Array.isArray(d.camels) ? d.camels.length : 0) + '</b></span>' +
                        '<span>gps <b class="' + (hasFix ? '' : 'warn') + '">' + (hasFix ? 'LOCKED' : 'NO FIX') + '</b></span>' +
                        '<span>state <b class="' + (d.armed ? 'warn' : '') + '">' + (d.armed ? 'ARMED' : 'STANDBY') + '</b></span>';
                }

                // ---------- card markup ----------
                function cardHTML(id, d) {
                    const online = isOnline(d);
                    const live = !!d.isLive && online;
                    const hasFix = !!d.drone?.hasGPSFix && typeof d.drone?.lat === 'number';
                    return '<div class="fw-card" data-id="' + id + '">' +
                        '<div class="fw-label"><span class="dot ' + (online ? (d.armed ? 'armed' : 'online') : 'offline') + '"></span>' + (d.name || id) + '</div>' +
                        '<span class="fw-status ' + (live ? 'live' : 'offline') + '" id="fwstatus-' + id + '">' + (live ? 'connecting' : (online ? 'no stream' : 'offline')) + '</span>' +
                        (live ? '<video id="fwvideo-' + id + '" muted playsinline></video>' : '<div class="fw-placeholder">' + (online ? 'Connected — not streaming' : 'No signal') + '</div>') +
                        (hasFix ? '<div class="fw-map" id="fwmap-' + id + '"></div>' : '') +
                        '<div class="fw-telemetry">' + telemetryFieldsHTML(d) + '</div>' +
                    '</div>';
                }

                // Rebuilds the whole grid. Only called when a drone joins/leaves,
                // or its online/live/GPS-fix status changes — never on a plain
                // telemetry tick — so a playing video is never torn down and
                // reconnected just because a position update came in.
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
                        updateCounts();
                        return;
                    }
                    grid.style.display = 'grid';
                    empty.style.display = 'none';

                    grid.innerHTML = ids.map((id) => cardHTML(id, dronesById[id])).join('');

                    Object.keys(players).forEach((id) => {
                        if (!dronesById[id] || !dronesById[id].isLive || !isOnline(dronesById[id])) destroyPlayer(id);
                    });
                    Object.keys(maps).forEach((id) => {
                        if (!dronesById[id]) destroyMap(id);
                    });

                    ids.forEach((id) => {
                        const d = dronesById[id];

                        if (d.isLive && isOnline(d) && !players[id]) {
                            const videoEl = document.getElementById('fwvideo-' + id);
                            const statusEl = document.getElementById('fwstatus-' + id);
                            if (videoEl) startPlayer(id, d.streamKey || id, videoEl, statusEl);
                        }

                        const mapEl = document.getElementById('fwmap-' + id);
                        if (mapEl) ensureMap(id, d, mapEl);

                        renderState[id] = computeState(d);
                    });

                    Object.keys(renderState).forEach((id) => { if (!dronesById[id]) delete renderState[id]; });

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
                // dot, and the map marker position in place — never touches the
                // video element, so a playing stream is never interrupted by an
                // ordinary telemetry packet.
                function updateCardInPlace(id, d) {
                    const card = document.querySelector('.fw-card[data-id="' + id + '"]');
                    if (!card) return;

                    const online = isOnline(d);
                    const dot = card.querySelector('.fw-label .dot');
                    if (dot) dot.className = 'dot ' + (online ? (d.armed ? 'armed' : 'online') : 'offline');

                    const label = card.querySelector('.fw-label');
                    if (label && label.lastChild && label.lastChild.nodeType === Node.TEXT_NODE) {
                        label.lastChild.textContent = d.name || id;
                    }

                    const telemetry = card.querySelector('.fw-telemetry');
                    if (telemetry) telemetry.innerHTML = telemetryFieldsHTML(d);

                    const mapEl = document.getElementById('fwmap-' + id);
                    if (mapEl) ensureMap(id, d, mapEl);
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
                    refresh(id);
                }

                fetch('/api/drones').then(r => r.json()).then((list) => {
                    list.forEach((d) => applyUpdate({ ...d, drone: { lat: d.lat, lng: d.lng, yaw: d.yaw, pitch: d.pitch, alt: d.alt, hasGPSFix: d.hasGPSFix } }));
                }).catch(() => {});

                socket.on('telemetry_update', applyUpdate);

                // Periodic sweep to catch a drone going offline purely from the
                // passage of time (no new packet needed to notice a timeout).
                // Uses the same change-detection as refresh(), so it still
                // won't touch a live video unless a status actually flipped.
                setInterval(() => {
                    Object.keys(dronesById).forEach(refresh);
                }, 5000);
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
    console.log(`- Video wall (all live feeds): http://localhost:3000/video-wall`);
    console.log(`============================================`);
});
