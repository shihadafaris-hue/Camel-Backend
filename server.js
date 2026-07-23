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
    const droneId = body.droneId || DEFAULT_DRONE_ID;
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

// Snapshot of the whole fleet — used by the dashboard/control pages on load,
// before any live socket events have arrived.
app.get('/api/drones', (req, res) => {
    res.status(200).json(fleetSummary());
});

// ==========================================
// 3. BASIC DRONE CONTROL
// ==========================================
// The control page posts commands here. Firmware can either listen for the
// 'control_command' socket event, or poll GET /api/control/:droneId/latest
// and act whenever commandId increases.
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
        }

        * { box-sizing: border-box; }

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

        .mono { font-family: 'JetBrains Mono', monospace; }

        a { color: inherit; }

        ::selection { background: rgba(95,212,208,0.3); }

        .topbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 16px 28px;
            border-bottom: 1px solid var(--border);
            background: rgba(20,26,36,0.7);
            backdrop-filter: blur(6px);
            position: sticky;
            top: 0;
            z-index: 20;
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .brand-mark {
            width: 34px;
            height: 34px;
            border-radius: 8px;
            background: linear-gradient(145deg, var(--cyan), #2a8f8c);
            display: flex;
            align-items: center;
            justify-content: center;
            font-family: 'Space Grotesk', sans-serif;
            font-weight: 700;
            color: #06201f;
            font-size: 15px;
            flex-shrink: 0;
        }

        .brand-text h1 {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 16px;
            font-weight: 600;
            letter-spacing: 0.02em;
            margin: 0;
            color: var(--text);
        }

        .brand-text span {
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            color: var(--text-faint);
            letter-spacing: 0.04em;
        }

        .nav-links {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .nav-link {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 13px;
            font-weight: 600;
            text-decoration: none;
            padding: 8px 16px;
            border-radius: 7px;
            border: 1px solid transparent;
            color: var(--text-dim);
            transition: all 0.15s ease;
        }

        .nav-link:hover { color: var(--text); border-color: var(--border); }
        .nav-link.active { color: var(--bg); background: var(--cyan); }

        .fleet-pill {
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            color: var(--text-dim);
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 5px 12px;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .dot {
            width: 7px;
            height: 7px;
            border-radius: 50%;
            flex-shrink: 0;
        }
        .dot.online { background: var(--green); box-shadow: 0 0 0 0 rgba(87,199,122,0.6); animation: pulse 2s infinite; }
        .dot.offline { background: var(--text-faint); }
        .dot.armed { background: var(--amber); box-shadow: 0 0 0 0 rgba(232,163,61,0.6); animation: pulse 2s infinite; }

        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(87,199,122,0.55); }
            70% { box-shadow: 0 0 0 7px rgba(87,199,122,0); }
            100% { box-shadow: 0 0 0 0 rgba(87,199,122,0); }
        }

        .panel {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: var(--radius);
        }

        .panel-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 14px 18px;
            border-bottom: 1px solid var(--border);
        }

        .panel-header h3 {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 12px;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--text-dim);
            margin: 0;
        }

        .scrollbar::-webkit-scrollbar { width: 8px; height: 8px; }
        .scrollbar::-webkit-scrollbar-thumb { background: var(--border); border-radius: 8px; }
        .scrollbar::-webkit-scrollbar-track { background: transparent; }

        button { font-family: 'Space Grotesk', sans-serif; cursor: pointer; }
        button:focus-visible, a:focus-visible, input:focus-visible { outline: 2px solid var(--cyan); outline-offset: 2px; }

        @media (prefers-reduced-motion: reduce) {
            .dot.online, .dot.armed { animation: none; }
        }
    `;
}

function topBar(activePage, fleetCount) {
    return `
        <div class="topbar">
            <div class="brand">
                <div class="brand-mark">FF</div>
                <div class="brand-text">
                    <h1>Camel Project</h1>
                    <span>GROUND CONTROL &middot; MULTI-DRONE</span>
                </div>
            </div>
            <div class="nav-links">
                <span class="fleet-pill"><span class="dot online"></span>${fleetCount} in fleet</span>
                <a class="nav-link ${activePage === 'dashboard' ? 'active' : ''}" href="/dashboard">Dashboard</a>
                <a class="nav-link ${activePage === 'control' ? 'active' : ''}" href="/control">Control</a>
            </div>
        </div>
    `;
}

// ==========================================
// 5. DASHBOARD — fleet map, live video, telemetry
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
            <style>
                ${sharedStyles()}

                .layout {
                    display: grid;
                    grid-template-columns: 300px 1fr;
                    gap: 20px;
                    padding: 20px 28px 28px;
                    align-items: start;
                }
                @media (max-width: 980px) { .layout { grid-template-columns: 1fr; } }

                .fleet-list {
                    display: flex;
                    flex-direction: column;
                    max-height: calc(100vh - 110px);
                    overflow-y: auto;
                }

                .fleet-empty {
                    padding: 24px 18px;
                    color: var(--text-faint);
                    font-size: 13px;
                    line-height: 1.6;
                }

                .drone-card {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    padding: 12px 18px;
                    border-bottom: 1px solid var(--border);
                    cursor: pointer;
                    transition: background 0.15s ease;
                }
                .drone-card:last-child { border-bottom: none; }
                .drone-card:hover { background: var(--panel-raised); }
                .drone-card.selected { background: var(--panel-raised); box-shadow: inset 3px 0 0 var(--cyan); }

                .drone-card .info { flex: 1; min-width: 0; }
                .drone-card .name { font-family: 'Space Grotesk', sans-serif; font-size: 13px; font-weight: 600; }
                .drone-card .meta { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text-faint); margin-top: 2px; }
                .drone-card .status { font-size: 10px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-faint); }

                .main-col { display: flex; flex-direction: column; gap: 20px; }

                #map { height: 420px; width: 100%; border-radius: 0 0 var(--radius) var(--radius); filter: saturate(0.35) brightness(0.85) contrast(1.05); }

                .media-row { display: grid; grid-template-columns: 1.3fr 1fr; gap: 20px; }
                @media (max-width: 980px) { .media-row { grid-template-columns: 1fr; } }

                .video-container {
                    position: relative;
                    width: 100%;
                    height: 340px;
                    background: #05070a;
                    border-radius: 0 0 var(--radius) var(--radius);
                    overflow: hidden;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }
                video { width: 100%; height: 100%; object-fit: cover; }
                .video-placeholder { color: var(--text-faint); font-size: 12px; font-family: 'JetBrains Mono', monospace; text-align: center; padding: 20px; }

                .stream-controls { display: flex; gap: 8px; align-items: center; padding: 12px 16px; border-top: 1px solid var(--border); }
                .stream-controls input {
                    flex: 1; background: var(--bg); border: 1px solid var(--border); color: var(--text);
                    padding: 8px 10px; border-radius: 6px; font-family: 'JetBrains Mono', monospace; font-size: 12px;
                }
                .stream-controls button {
                    background: var(--panel-raised); border: 1px solid var(--border); color: var(--text);
                    padding: 8px 14px; border-radius: 6px; font-size: 12px; font-weight: 600;
                }
                .stream-controls button:hover { border-color: var(--cyan); color: var(--cyan); }
                .stream-status { padding: 0 16px 12px; font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text-faint); }

                .telemetry-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 1px; background: var(--border); }
                .stat-card { background: var(--panel); padding: 16px 18px; }
                .stat-card h4 { margin: 0 0 6px; font-size: 10px; text-transform: uppercase; letter-spacing: 0.07em; color: var(--text-faint); font-weight: 600; }
                .stat-card .value { font-family: 'JetBrains Mono', monospace; font-size: 20px; font-weight: 500; color: var(--text); }
                .stat-card .value.accent { color: var(--cyan); }
                .stat-card .value.warn { color: var(--amber); }
            </style>
        </head>
        <body>
            ${topBar('dashboard', 0)}
            <div class="layout">
                <div class="panel fleet-list scrollbar" id="fleetList">
                    <div class="fleet-empty">Waiting for the first telemetry packet from any drone…</div>
                </div>

                <div class="main-col">
                    <div class="panel">
                        <div class="panel-header"><h3>Fleet Position</h3><span class="mono" style="font-size:11px;color:var(--text-faint);" id="selectedLabel">no drone selected</span></div>
                        <div id="map"></div>
                    </div>

                    <div class="media-row">
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
            </div>

            <script>
                const socket = io();
                const dronesById = {};
                let selectedDroneId = null;
                let flvPlayer = null;
                let retryTimer = null;
                let currentStreamKey = null;

                const map = L.map('map', { zoomControl: true }).setView([22.3098, 39.1065], 17);
                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 22 }).addTo(map);
                const markers = {};

                function droneIcon(online, selected) {
                    const color = online ? 'var(--green)' : 'var(--text-faint)';
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
                    if (id === selectedDroneId) updateTelemetryPanel();
                }

                // Seed from whatever the server already knows before the first socket event.
                fetch('/api/drones').then(r => r.json()).then((list) => {
                    list.forEach((d) => applyUpdate({ ...d, drone: { lat: d.lat, lng: d.lng, yaw: d.yaw, pitch: d.pitch, alt: d.alt, hasGPSFix: d.hasGPSFix } }));
                }).catch(() => {});

                socket.on('telemetry_update', applyUpdate);

                function setStatus(msg, color) {
                    const el = document.getElementById('streamStatus');
                    el.innerText = msg;
                    el.style.color = color || 'var(--text-faint)';
                }

                function startFlvPlayer(streamKey) {
                    document.getElementById('videoPlaceholder').style.display = 'none';
                    if (typeof flvjs === 'undefined' || !flvjs.isSupported()) {
                        setStatus('FLV.js is not supported in this browser.', 'var(--red)');
                        return;
                    }
                    if (retryTimer) { clearTimeout(retryTimer); retryTimer = null; }
                    if (flvPlayer) {
                        try { flvPlayer.pause(); flvPlayer.unload(); flvPlayer.detachMediaElement(); flvPlayer.destroy(); } catch (e) {}
                        flvPlayer = null;
                    }
                    currentStreamKey = streamKey;
                    const url = 'http://' + window.location.hostname + ':8000/live/' + streamKey + '.flv';
                    setStatus('Connecting to ' + streamKey + ' …');

                    const videoElement = document.getElementById('videoElement');
                    flvPlayer = flvjs.createPlayer({ type: 'flv', isLive: true, url: url });
                    flvPlayer.on(flvjs.Events.ERROR, (errType, errDetail) => {
                        setStatus('Stream error: ' + errType + ' — retrying in 3s…', 'var(--red)');
                        retryTimer = setTimeout(() => startFlvPlayer(streamKey), 3000);
                    });
                    flvPlayer.on(flvjs.Events.LOADING_COMPLETE, () => setStatus('Stream ended.', 'var(--amber)'));
                    videoElement.addEventListener('playing', () => setStatus('Live ●', 'var(--green)'), { once: true });

                    flvPlayer.attachMediaElement(videoElement);
                    flvPlayer.load();
                    flvPlayer.play().catch(() => setStatus('Autoplay blocked — click the video to start playback.', 'var(--amber)'));
                }

                document.getElementById('loadStreamBtn').addEventListener('click', () => {
                    const key = document.getElementById('streamKeyInput').value.trim() || (selectedDroneId || 'drone-1');
                    startFlvPlayer(key);
                });
                document.getElementById('videoElement').addEventListener('click', () => { if (flvPlayer) flvPlayer.play().catch(() => {}); });
            </script>
        </body>
        </html>
    `);
});

// ==========================================
// 6. CONTROL — basic per-drone flight & gimbal commands
// ==========================================
app.get('/control', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>Control</title>
            <script src="/socket.io/socket.io.js"></script>
            <style>
                ${sharedStyles()}

                .layout { padding: 20px 28px 40px; max-width: 1100px; margin: 0 auto; }

                .drone-tabs { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 20px; }
                .drone-tab {
                    font-family: 'JetBrains Mono', monospace; font-size: 12px;
                    padding: 8px 16px; border-radius: 20px; border: 1px solid var(--border);
                    background: var(--panel); color: var(--text-dim); display: flex; align-items: center; gap: 8px;
                }
                .drone-tab.active { border-color: var(--cyan); color: var(--text); background: var(--panel-raised); }
                .drone-tabs-empty { color: var(--text-faint); font-size: 13px; }

                .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; align-items: start; }
                @media (max-width: 860px) { .grid-2 { grid-template-columns: 1fr; } }

                .control-body { padding: 20px; display: flex; flex-direction: column; gap: 18px; }

                .arm-row { display: flex; gap: 10px; }
                .btn {
                    flex: 1; padding: 14px; border-radius: 8px; border: 1px solid var(--border);
                    background: var(--panel-raised); color: var(--text); font-size: 13px; font-weight: 600;
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

                .btn.stop { background: var(--red); border-color: var(--red); color: #1a0505; font-weight: 700; padding: 16px; }
                .btn.stop:hover { background: #f16569; }

                .slider-block label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-faint); font-weight: 600; }
                .slider-row { display: flex; align-items: center; gap: 14px; margin-top: 8px; }
                input[type="range"] { flex: 1; accent-color: var(--cyan); }
                .slider-value { font-family: 'JetBrains Mono', monospace; font-size: 13px; width: 54px; text-align: right; color: var(--cyan); }

                .yaw-nudges { display: flex; gap: 10px; margin-top: 8px; }
                .btn.nudge { flex: 1; padding: 10px; font-size: 16px; }

                .log-panel { margin-top: 20px; }
                .log-body { max-height: 260px; overflow-y: auto; font-family: 'JetBrains Mono', monospace; font-size: 12px; }
                .log-row { padding: 10px 18px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; gap: 12px; color: var(--text-dim); }
                .log-row:last-child { border-bottom: none; }
                .log-row .cmd { color: var(--text); }
                .log-empty { padding: 24px 18px; color: var(--text-faint); font-size: 13px; }

                .disabled-note { font-size: 12px; color: var(--text-faint); text-align: center; padding: 8px 0 0; }
            </style>
        </head>
        <body>
            ${topBar('control', 0)}
            <div class="layout">
                <div class="drone-tabs" id="droneTabs">
                    <span class="drone-tabs-empty">No drones reporting in yet — controls will unlock once telemetry arrives.</span>
                </div>

                <div class="grid-2">
                    <div class="panel">
                        <div class="panel-header"><h3>Flight Command</h3></div>
                        <div class="control-body">
                            <div class="arm-row">
                                <button class="btn arm" id="armBtn" disabled>Arm</button>
                                <button class="btn disarm" id="disarmBtn" disabled>Disarm</button>
                            </div>
                            <div class="mode-grid">
                                <button class="btn mode" id="takeoffBtn" disabled>Takeoff</button>
                                <button class="btn mode" id="landBtn" disabled>Land</button>
                                <button class="btn mode" id="rtlBtn" disabled style="grid-column: span 2;">Return to Launch</button>
                            </div>
                            <button class="btn stop" id="stopBtn" disabled>Emergency Stop</button>
                            <div class="disabled-note" id="disabledNote">Select a drone above to enable controls.</div>
                        </div>
                    </div>

                    <div class="panel">
                        <div class="panel-header"><h3>Gimbal &amp; Heading</h3></div>
                        <div class="control-body">
                            <div class="slider-block">
                                <label>Gimbal Pitch</label>
                                <div class="slider-row">
                                    <input type="range" id="pitchSlider" min="-90" max="30" value="0" disabled />
                                    <span class="slider-value" id="pitchValue">0°</span>
                                </div>
                            </div>
                            <div class="slider-block">
                                <label>Yaw Nudge</label>
                                <div class="yaw-nudges">
                                    <button class="btn nudge" id="yawLeftBtn" disabled>&#8630; 15°</button>
                                    <button class="btn nudge" id="yawRightBtn" disabled>15° &#8631;</button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="panel log-panel">
                    <div class="panel-header"><h3>Command Log</h3></div>
                    <div class="log-body scrollbar" id="logBody">
                        <div class="log-empty">Commands you send will appear here.</div>
                    </div>
                </div>
            </div>

            <script>
                const socket = io();
                let selectedDroneId = null;
                const knownDrones = {};

                const controlButtons = ['armBtn','disarmBtn','takeoffBtn','landBtn','rtlBtn','stopBtn','pitchSlider','yawLeftBtn','yawRightBtn'];

                function setControlsEnabled(enabled) {
                    controlButtons.forEach((id) => { document.getElementById(id).disabled = !enabled; });
                    document.getElementById('disabledNote').style.display = enabled ? 'none' : 'block';
                }

                function renderTabs() {
                    const ids = Object.keys(knownDrones);
                    const wrap = document.getElementById('droneTabs');
                    if (ids.length === 0) {
                        wrap.innerHTML = '<span class="drone-tabs-empty">No drones reporting in yet — controls will unlock once telemetry arrives.</span>';
                        return;
                    }
                    wrap.innerHTML = ids.map((id) => {
                        const d = knownDrones[id];
                        const online = (Date.now() - (d.lastUpdate || 0)) < 15000;
                        return '<div class="drone-tab ' + (id === selectedDroneId ? 'active' : '') + '" data-id="' + id + '">' +
                            '<span class="dot ' + (online ? (d.armed ? 'armed' : 'online') : 'offline') + '"></span>' + (d.name || id) + '</div>';
                    }).join('');
                    wrap.querySelectorAll('.drone-tab').forEach((el) => {
                        el.addEventListener('click', () => {
                            selectedDroneId = el.getAttribute('data-id');
                            renderTabs();
                            setControlsEnabled(true);
                        });
                    });
                }

                function noteDrone(id, data) {
                    knownDrones[id] = data;
                    if (!selectedDroneId) { selectedDroneId = id; setControlsEnabled(true); }
                    renderTabs();
                }

                fetch('/api/drones').then(r => r.json()).then((list) => {
                    list.forEach((d) => noteDrone(d.droneId, d));
                }).catch(() => {});

                socket.on('telemetry_update', (data) => noteDrone(data.droneId || 'drone-1', data));

                function sendCommand(command, params) {
                    if (!selectedDroneId) return;
                    fetch('/api/control/' + selectedDroneId, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ command, params })
                    }).catch(() => appendLog({ droneId: selectedDroneId, command, params, updatedAt: Date.now(), failed: true }));
                }

                function appendLog(entry) {
                    const body = document.getElementById('logBody');
                    if (body.querySelector('.log-empty')) body.innerHTML = '';
                    const row = document.createElement('div');
                    row.className = 'log-row';
                    const time = new Date(entry.updatedAt || Date.now()).toLocaleTimeString();
                    row.innerHTML = '<span>' + time + ' &middot; ' + entry.droneId + '</span>' +
                        '<span class="cmd">' + entry.command + (entry.params ? ' ' + JSON.stringify(entry.params) : '') + (entry.failed ? ' (failed to send)' : '') + '</span>';
                    body.prepend(row);
                }

                socket.on('control_command', appendLog);

                document.getElementById('armBtn').addEventListener('click', () => sendCommand('ARM'));
                document.getElementById('disarmBtn').addEventListener('click', () => sendCommand('DISARM'));
                document.getElementById('takeoffBtn').addEventListener('click', () => sendCommand('TAKEOFF'));
                document.getElementById('landBtn').addEventListener('click', () => sendCommand('LAND'));
                document.getElementById('rtlBtn').addEventListener('click', () => sendCommand('RTL'));
                document.getElementById('stopBtn').addEventListener('click', () => sendCommand('EMERGENCY_STOP'));
                document.getElementById('yawLeftBtn').addEventListener('click', () => sendCommand('SET_YAW', { deltaDeg: -15 }));
                document.getElementById('yawRightBtn').addEventListener('click', () => sendCommand('SET_YAW', { deltaDeg: 15 }));

                const pitchSlider = document.getElementById('pitchSlider');
                const pitchValue = document.getElementById('pitchValue');
                pitchSlider.addEventListener('input', () => { pitchValue.innerText = pitchSlider.value + '°'; });
                pitchSlider.addEventListener('change', () => sendCommand('SET_GIMBAL', { pitchDeg: Number(pitchSlider.value) }));
            </script>
        </body>
        </html>
    `);
});

// ==========================================
// 7. YAW & STATUS LOGS
// ==========================================
app.get('/control', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>FFly Control Panel</title>
            <style>
                .yaw-nudges { display: flex; gap: 8px; margin-top: 8px; }
                .btn.nudge { font-family: 'JetBrains Mono', monospace; font-size: 11px; padding: 10px; }

                .status-box {
                    background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
                    padding: 12px 14px; font-family: 'JetBrains Mono', monospace; font-size: 12px;
                    color: var(--text-dim); min-height: 54px; display: flex; align-items: center;
                }
            </style>
        </head>
        <body>
            ${topBar('control', 0)}
            <div class="layout">
                <div class="drone-tabs" id="droneTabs">
                    <span class="drone-tabs-empty">Waiting for active drones in the fleet…</span>
                </div>

                <div class="grid-2">
                    <div class="panel">
                        <div class="panel-header"><h3>Flight Actions</h3><span class="mono" id="targetDroneLabel" style="font-size:11px;color:var(--text-faint);">no target</span></div>
                        <div class="control-body">
                            <div class="arm-row">
                                <button class="btn arm" id="btnArm">ARM</button>
                                <button class="btn disarm" id="btnDisarm">DISARM</button>
                            </div>
                            <div class="mode-grid">
                                <button class="btn mode" id="btnTakeoff">TAKEOFF</button>
                                <button class="btn mode" id="btnLand">LAND</button>
                                <button class="btn mode" id="btnRtl">RTL</button>
                                <button class="btn mode" id="btnHover">HOLD</button>
                            </div>
                            <button class="btn stop" id="btnEmergency">EMERGENCY STOP</button>
                        </div>
                    </div>

                    <div class="panel">
                        <div class="panel-header"><h3>Payload & Gimbal</h3></div>
                        <div class="control-body">
                            <div class="slider-block">
                                <label for="gimbalPitch">Gimbal Pitch (Tilt)</label>
                                <div class="slider-row">
                                    <input type="range" id="gimbalPitch" min="-90" max="0" value="0" />
                                    <span class="slider-value" id="gimbalPitchVal">0°</span>
                                </div>
                            </div>

                            <div class="slider-block">
                                <label>Heading Adjustment (Yaw)</label>
                                <div class="yaw-nudges">
                                    <button class="btn nudge" id="btnYawLeft">&larr; -15°</button>
                                    <button class="btn nudge" id="btnYawReset">Reset (0°)</button>
                                    <button class="btn nudge" id="btnYawRight">+15° &rarr;</button>
                                </div>
                            </div>

                            <div class="slider-block">
                                <label>Command Status</label>
                                <div class="status-box" id="commandStatus">Select a drone to begin issuing commands.</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <script>
                const socket = io();
                const knownDrones = {};
                let targetDroneId = null;

                function renderTabs() {
                    const container = document.getElementById('droneTabs');
                    const ids = Object.keys(knownDrones);
                    if (ids.length === 0) {
                        container.innerHTML = '<span class="drone-tabs-empty">Waiting for active drones in the fleet…</span>';
                        return;
                    }
                    container.innerHTML = ids.map((id) => {
                        const d = knownDrones[id];
                        const active = id === targetDroneId ? 'active' : '';
                        const online = (Date.now() - (d.lastUpdate || 0)) < 15000;
                        return '<div class="drone-tab ' + active + '" data-id="' + id + '">' +
                            '<span class="dot ' + (online ? (d.armed ? 'armed' : 'online') : 'offline') + '"></span>' +
                            (d.name || id) +
                        '</div>';
                    }).join('');

                    container.querySelectorAll('.drone-tab').forEach((el) => {
                        el.addEventListener('click', () => setTargetDrone(el.getAttribute('data-id')));
                    });
                }

                function setTargetDrone(id) {
                    targetDroneId = id;
                    document.getElementById('targetDroneLabel').innerText = 'target: ' + id;
                    renderTabs();
                }

                function sendCommand(command, params) {
                    if (!targetDroneId) {
                        setStatus('Error: No drone selected to target.', 'var(--red)');
                        return;
                    }
                    setStatus('Sending ' + command + ' to ' + targetDroneId + '…', 'var(--cyan)');
                    fetch('/api/control/' + encodeURIComponent(targetDroneId), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ command, params })
                    })
                    .then(r => r.json())
                    .then(res => {
                        if (res.message && res.message.includes('Unknown')) {
                            setStatus(res.message, 'var(--red)');
                        } else {
                            setStatus('Command ' + command + ' dispatched (id: ' + res.commandId + ')', 'var(--green)');
                        }
                    })
                    .catch(err => setStatus('Failed to send command: ' + err.message, 'var(--red)'));
                }

                function setStatus(msg, color) {
                    const box = document.getElementById('commandStatus');
                    box.innerText = msg;
                    if (color) box.style.color = color;
                }

                // UI bindings
                document.getElementById('btnArm').addEventListener('click', () => sendCommand('ARM'));
                document.getElementById('btnDisarm').addEventListener('click', () => sendCommand('DISARM'));
                document.getElementById('btnTakeoff').addEventListener('click', () => sendCommand('TAKEOFF', { altitude: 10 }));
                document.getElementById('btnLand').addEventListener('click', () => sendCommand('LAND'));
                document.getElementById('btnRtl').addEventListener('click', () => sendCommand('RTL'));
                document.getElementById('btnHover').addEventListener('click', () => sendCommand('HOLD'));
                document.getElementById('btnEmergency').addEventListener('click', () => {
                    if (confirm('WARNING: Trigger Emergency Stop?')) sendCommand('EMERGENCY_STOP');
                });

                const gimbalSlider = document.getElementById('gimbalPitch');
                const gimbalVal = document.getElementById('gimbalPitchVal');
                gimbalSlider.addEventListener('input', (e) => {
                    gimbalVal.innerText = e.target.value + '°';
                });
                gimbalSlider.addEventListener('change', (e) => {
                    sendCommand('SET_GIMBAL', { pitch: parseInt(e.target.value, 10) });
                });

                document.getElementById('btnYawLeft').addEventListener('click', () => sendCommand('SET_YAW', { relative: -15 }));
                document.getElementById('btnYawReset').addEventListener('click', () => sendCommand('SET_YAW', { absolute: 0 }));
                document.getElementById('btnYawRight').addEventListener('click', () => sendCommand('SET_YAW', { relative: 15 }));

                // Initial fetch & real-time updates
                fetch('/api/drones').then(r => r.json()).then(list => {
                    list.forEach(d => {
                        knownDrones[d.droneId] = d;
                        if (!targetDroneId) setTargetDrone(d.droneId);
                    });
                    renderTabs();
                }).catch(() => {});

                socket.on('telemetry_update', (data) => {
                    const id = data.droneId || 'drone-1';
                    knownDrones[id] = data;
                    if (!targetDroneId) setTargetDrone(id);
                    renderTabs();
                });
            </script>
        </body>
        </html>
    `);
});
// ==========================================
// 8. SERVER INITIALIZATION
// ==========================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`============================================`);
    console.log(`FFly Ground Control Server active on port ${PORT}`);
    console.log(`- RTMP ingest: rtmp://localhost:1935/live/<droneId>`);
    console.log(`- HTTP-FLV stream: http://localhost:8000/live/<droneId>.flv`);
    console.log(`- Dashboard: http://localhost:3000/dashboard`);
    console.log(`- Control Panel: http://localhost:3000/control`);
    console.log(`============================================`);
});
