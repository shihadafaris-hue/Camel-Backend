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
// 1. DATA STORAGE & LOGGING
// ==========================================
const LOG_FILE = path.join(__dirname, 'drone_log.txt');

// Store latest states in memory so pages refresh with data instantly
let latestTelemetry = {
    drone: { lat: 0, lng: 0, yaw: 0, pitch: 0, alt: 0 },
    camels: []
};

// Endpoint for your iOS app to push data
app.post('/api/telemetry', (req, res) => {
    latestTelemetry = req.body;
    
    // Log data to local file with a timestamp
    const logEntry = `[${new Date().toISOString()}] ${JSON.stringify(latestTelemetry)}\n`;
    fs.appendFile(LOG_FILE, logEntry, (err) => {
        if (err) console.error("Failed to write to log file:", err);
    });

    // Send data to all active web map/dashboard tabs instantly
    io.emit('telemetry_update', latestTelemetry);
    
    res.status(200).json({ message: "Data received and logged" });
});

// ==========================================
// 2. LIVE TRACKING MAP (With FOV & Camels)
// ==========================================
app.get('/map', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Live Tracking Map</title>
            <script src="/socket.io/socket.io.js"></script>
            <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css" />
            <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
            <style>#map { height: 100vh; width: 100%; margin: 0; padding: 0; } body { margin: 0; }</style>
        </head>
        <body>
            <div id="map"></div>
            <script>
                const socket = io();
                
                // Initialize map (Defaults near Makkah Province)
                const map = L.map('map').setView([22.3098, 39.1065], 18);
                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 22 }).addTo(map);

                let droneMarker = L.marker([0, 0]).addTo(map).bindPopup("Drone");
                let fovPolygon = null;
                let camelMarkers = [];

                // Math function mirroring your Swift offset calculation
                function calculateOffset(lat, lng, bearingDeg, distanceMeters) {
                    const R = 6378137.0; 
                    const lat1 = lat * Math.PI / 180;
                    const lon1 = lng * Math.PI / 180;
                    const brng = bearingDeg * Math.PI / 180;
                    
                    const lat2 = Math.asin(Math.sin(lat1) * Math.cos(distanceMeters/R) + Math.cos(lat1) * Math.sin(distanceMeters/R) * Math.cos(brng));
                    const lon2 = lon1 + Math.atan2(Math.sin(brng) * Math.sin(distanceMeters/R) * Math.cos(lat1), Math.cos(distanceMeters/R) - Math.sin(lat1) * Math.sin(lat2));
                    
                    return [lat2 * 180 / Math.PI, lon2 * 180 / Math.PI];
                }

                function processData(data) {
                    if (data.drone && data.drone.lat && data.drone.lng) {
                        const droneLat = data.drone.lat;
                        const droneLng = data.drone.lng;
                        const droneYaw = data.drone.yaw || 0;
                        const newLatLng = new L.LatLng(droneLat, droneLng);
                        
                        droneMarker.setLatLng(newLatLng);
                        
                        if (!map.getBounds().contains(newLatLng)) {
                            map.panTo(newLatLng);
                        }

                        if (fovPolygon) map.removeLayer(fovPolygon);
                        
                        // Draw FOV Cone (25 meters, 30 degree spread)
                        const leftPoint = calculateOffset(droneLat, droneLng, droneYaw - 30, 25);
                        const rightPoint = calculateOffset(droneLat, droneLng, droneYaw + 30, 25);
                        
                        fovPolygon = L.polygon([[droneLat, droneLng], leftPoint, rightPoint], {
                            color: '#007AFF',
                            fillColor: '#007AFF',
                            fillOpacity: 0.25,
                            weight: 1
                        }).addTo(map);
                    }

                    if (data.camels && Array.isArray(data.camels)) {
                        camelMarkers.forEach(m => map.removeLayer(m));
                        camelMarkers = [];
                        
                        data.camels.forEach((camel, index) => {
                            if (camel.lat && camel.lng) {
                                let m = L.circleMarker([camel.lat, camel.lng], {
                                    color: '#FF3B30',
                                    fillColor: '#FF3B30',
                                    fillOpacity: 1,
                                    radius: 5,
                                    weight: 1
                                }).addTo(map).bindPopup("Camel " + (index + 1));
                                camelMarkers.push(m);
                            }
                        });
                    }
                }

                // Listen for updates or grab current snapshot on build
                socket.on('telemetry_update', (data) => processData(data));
                
                // Initialize with last cached position if page reloads
                const initialData = ${JSON.stringify(latestTelemetry)};
                if(initialData.drone.lat !== 0) processData(initialData);
            </script>
        </body>
        </html>
    `);
});

// ==========================================
// 3. STATS TELEMETRY DASHBOARD
// ==========================================
app.get('/dashboard', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Drone Dashboard</title>
            <script src="/socket.io/socket.io.js"></script>
            <style>
                body { font-family: -apple-system, sans-serif; padding: 20px; background: #f4f4f9; color: #333;}
                .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-top: 20px;}
                .card { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e1e1e8;}
                h3 { margin-top: 0; color: #666; font-size: 14px; text-transform: uppercase;}
                .value { font-size: 28px; font-weight: bold; color: #111;}
            </style>
        </head>
        <body>
            <h1>Drone Control Telemetry</h1>
            <div class="grid">
                <div class="card"><h3>📍 Latitude</h3><div id="lat" class="value">0.0</div></div>
                <div class="card"><h3>📍 Longitude</h3><div id="lng" class="value">0.0</div></div>
                <div class="card"><h3>⛰️ Altitude</h3><div id="alt" class="value">0.0 m</div></div>
                <div class="card"><h3>🧭 Yaw (Heading)</h3><div id="yaw" class="value">0.0°</div></div>
                <div class="card"><h3>📸 Gimbal Pitch</h3><div id="pitch" class="value">0.0°</div></div>
                <div class="card"><h3>🐫 Tracked Camels</h3><div id="camels" class="value">0</div></div>
            </div>
            <script>
                const socket = io();
                socket.on('telemetry_update', (data) => {
                    if (data.drone) {
                        document.getElementById('lat').innerText = data.drone.lat?.toFixed(6) ?? '0.0';
                        document.getElementById('lng').innerText = data.drone.lng?.toFixed(6) ?? '0.0';
                        document.getElementById('alt').innerText = (data.drone.alt?.toFixed(1) ?? '0.0') + ' m';
                        document.getElementById('yaw').innerText = (data.drone.yaw?.toFixed(1) ?? '0.0') + '°';
                        document.getElementById('pitch').innerText = (data.drone.pitch?.toFixed(1) ?? '0.0') + '°';
                    }
                    if (data.camels) {
                        document.getElementById('camels').innerText = data.camels.length;
                    }
                });
            </script>
        </body>
        </html>
    `);
});

// ==========================================
// 4. RUN HTTP AND RTMP SERVICES TOGETHER
// ==========================================
const HTTP_PORT = process.env.PORT || 3000;
server.listen(HTTP_PORT, () => {
    console.log(`\n======================================================`);
    console.log(`🚀 SERVERS STARTED SUCCESSFULLY`);
    console.log(`======================================================`);
    console.log(`📊 Dashboard Link:  http://localhost:${HTTP_PORT}/dashboard`);
    console.log(`🗺️  Live Map Link:   http://localhost:${HTTP_PORT}/map`);
    console.log(`📡 Telemetry API:  http://localhost:${HTTP_PORT}/api/telemetry`);
    console.log(`------------------------------------------------------`);
});

// Configure the Node Media Server for Video Routing
const nmsConfig = {
    rtmp: {
        port: 1935,
        chunk_size: 60000,
        gop_cache: true,
        ping: 30,
        ping_timeout: 60
    },
    http: {
        port: 8000, // Live video stream player endpoint
        allow_origin: '*'
    }
};

const nms = new NodeMediaServer(nmsConfig);
nms.run();
