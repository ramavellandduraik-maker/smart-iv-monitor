# Smart IV Drip Monitor

A real-time, software-based IoT monitoring system for intravenous (IV) drip management.  
Built for the Medical IoT PBL project.

**Team:** Sharon Thomas · Sasi Sornamalan · Sudharman · Ramavellanddurai

---

## Project Structure

```
smart-iv-monitor/
├── app.py                   # Flask backend + MQTT subscriber + WebSocket server
├── requirements.txt
├── run.bat                  # Windows one-click start
├── run.sh                   # Linux/Mac one-click start
├── simulator/
│   └── sensor_sim.py        # Simulated sensor data publisher (MQTT)
├── backend/
│   ├── db.py                # SQLite database layer
│   └── alert.py             # Alert & anomaly detection engine
└── frontend/
    └── index.html           # Real-time dashboard (Chart.js + Socket.IO)
```

---

## Quick Start

### Windows
```
double-click run.bat
```

### Linux / Mac
```bash
chmod +x run.sh
./run.sh
```

### Manual (two terminals)
```bash
# Terminal 1 — Backend
pip install -r requirements.txt
python app.py

# Terminal 2 — Simulator
python simulator/sensor_sim.py
```

Open **http://localhost:5000** in your browser.

---

## How It Works

| Layer | Component | Tech |
|---|---|---|
| Sensor | `sensor_sim.py` | Python, paho-mqtt |
| Messaging | MQTT | broker.hivemq.com (public) |
| Backend | `app.py` | Flask, Flask-SocketIO |
| Database | `db.py` | SQLite (built-in) |
| Alerts | `alert.py` | Rule-based engine |
| Dashboard | `index.html` | Chart.js, Socket.IO |

---

## REST API

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/patients` | Latest reading per patient |
| GET | `/api/live` | In-memory live snapshot |
| GET | `/api/history/<id>` | Session history for a patient |
| GET | `/api/alerts/active` | Unacknowledged alerts |
| GET | `/api/alerts/all` | All alerts |
| POST | `/api/alerts/<id>/acknowledge` | Acknowledge an alert |

---

## Alert Conditions

| Status | Trigger | Severity |
|---|---|---|
| LOW_FLUID | Fluid < 10% | Warning |
| BLOCKAGE | Drip rate = 0 | Critical |
| FREE_FLOW | Drip rate > 80 drops/min | Critical |
| AIR_BUBBLE | Air in line | Critical |
| EMPTY | Fluid = 0% | Critical |

---

## Deploying to the Cloud

See deployment options below. The simplest free option is **Render**.

1. Push this folder to a GitHub repository.
2. Connect the repo to Render → new Web Service.
3. Build command: `pip install -r requirements.txt`
4. Start command: `python app.py`
5. Done — share the public URL.
