"""
Smart IV Drip Monitor — Self-Contained Server
Includes Database, Alert Engine, Simulator, and Dashboard Serving.
Zero external local dependencies — works in flat or structured repos.
"""

import os
import sys
import json
import math
import random
import time
import threading
import sqlite3

# Eventlet monkey patching for WebSocket support
import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify, request, Response
from flask_socketio import SocketIO
from flask_cors import CORS
import paho.mqtt.client as mqtt

# ── App & SocketIO Setup ──────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "iv_monitor.db")
BROKER_HOST = "broker.hivemq.com"
BROKER_PORT = 1883
TOPIC_SUB = "smartiv/ward1/#"
latest_cache = {}

# ── Database Layer ────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS readings (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id   TEXT    NOT NULL,
                patient_name TEXT    NOT NULL,
                drip_rate    REAL    NOT NULL,
                fluid_pct    REAL    NOT NULL,
                remaining_ml REAL    NOT NULL,
                total_ml     REAL    NOT NULL,
                temperature  REAL    NOT NULL,
                status       TEXT    NOT NULL,
                timestamp    TEXT    NOT NULL,
                created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS alerts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id   TEXT    NOT NULL,
                patient_name TEXT    NOT NULL,
                alert_type   TEXT    NOT NULL,
                message      TEXT    NOT NULL,
                acknowledged INTEGER DEFAULT 0,
                created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
    print("[DB] Database initialised successfully.")

def insert_reading(data):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO readings
              (patient_id, patient_name, drip_rate, fluid_pct,
               remaining_ml, total_ml, temperature, status, timestamp)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            data["patient_id"], data["patient_name"],
            data["drip_rate"],  data["fluid_pct"],
            data["remaining_ml"], data["total_ml"],
            data["temperature"], data["status"], data["timestamp"],
        ))

def insert_alert(patient_id, patient_name, alert_type, message):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO alerts (patient_id, patient_name, alert_type, message)
            VALUES (?,?,?,?)
        """, (patient_id, patient_name, alert_type, message))

def acknowledge_alert(alert_id):
    with get_conn() as conn:
        conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (alert_id,))

def get_latest_readings():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM readings
            WHERE id IN (
                SELECT MAX(id) FROM readings GROUP BY patient_id
            )
            ORDER BY patient_id
        """).fetchall()
    return [dict(r) for r in rows]

def get_history(patient_id, limit=120):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM readings
            WHERE patient_id = ?
            ORDER BY id DESC LIMIT ?
        """, (patient_id, limit)).fetchall()
    return [dict(r) for r in reversed(rows)]

def get_active_alerts():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM alerts
            WHERE acknowledged = 0
            ORDER BY created_at DESC
        """).fetchall()
    return [dict(r) for r in rows]

def get_all_alerts(limit=50):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM alerts
            ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]

# ── Alert Engine ──────────────────────────────────────────────────────────────
_last_alert = {}

def evaluate(data):
    pid = data["patient_id"]
    name = data["patient_name"]
    status = data["status"]
    fired = []

    def _fire(alert_type, message):
        key = f"{pid}:{alert_type}"
        if _last_alert.get(key) == alert_type:
            return
        _last_alert[key] = alert_type
        insert_alert(pid, name, alert_type, message)
        alert_obj = {
            "patient_id": pid,
            "patient_name": name,
            "alert_type": alert_type,
            "message": message,
        }
        fired.append(alert_obj)
        socketio.emit("new_alert", alert_obj)
        print(f"[ALERT] {alert_type}: {message}")

    def _clear(alert_type):
        key = f"{pid}:{alert_type}"
        _last_alert.pop(key, None)

    if status == "BLOCKAGE":
        _fire("BLOCKAGE", f"{name} — IV flow stopped. Tube blockage detected.")
    else:
        _clear("BLOCKAGE")

    if status == "FREE_FLOW":
        _fire("FREE_FLOW", f"{name} — High drip rate ({data['drip_rate']} drops/min). Free-flow risk!")
    else:
        _clear("FREE_FLOW")

    if status == "LOW_FLUID":
        _fire("LOW_FLUID", f"{name} — Fluid level at {data['fluid_pct']}%. Replace IV bag.")
    else:
        _clear("LOW_FLUID")

    if status == "EMPTY":
        _fire("EMPTY", f"{name} — IV bag is EMPTY. Replace immediately!")

    if status == "AIR_BUBBLE":
        _fire("AIR_BUBBLE", f"{name} — Air bubble detected in IV tube!")
    else:
        _clear("AIR_BUBBLE")

    return fired

# ── MQTT Subscriber ───────────────────────────────────────────────────────────
def on_mqtt_connect(client, _ud, _flags, rc):
    if rc == 0:
        client.subscribe(TOPIC_SUB, qos=1)
        print(f"[MQTT] Connected & subscribed to {TOPIC_SUB}")
    else:
        print(f"[MQTT] Connect failed with code {rc}")

def on_mqtt_message(_client, _ud, msg):
    try:
        data = json.loads(msg.payload.decode())
    except Exception:
        return
    pid = data.get("patient_id", "unknown")
    insert_reading(data)
    latest_cache[pid] = data
    evaluate(data)
    socketio.emit("reading", data)

def start_mqtt_subscriber():
    client = mqtt.Client(client_id=f"iv_backend_{int(time.time())}")
    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message
    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_forever()
    except Exception as e:
        print(f"[MQTT] Error: {e}")

# ── Built-in Simulator ────────────────────────────────────────────────────────
PATIENTS = [
    {"id": "P001", "name": "Alice Johnson", "bag_ml": 500, "normal_rate": (30, 50)},
    {"id": "P002", "name": "Bob Fernandez", "bag_ml": 250, "normal_rate": (20, 40)},
    {"id": "P003", "name": "Carol Mehta",   "bag_ml": 500, "normal_rate": (35, 55)},
]
patient_state = {
    p["id"]: {
        "remaining_ml": float(p["bag_ml"]),
        "total_ml": float(p["bag_ml"]),
        "tick": random.uniform(0, 6.28),
        "anomaly_countdown": random.randint(25, 60),
        "current_anomaly": None,
    }
    for p in PATIENTS
}

def compute_packet(patient):
    pid = patient["id"]
    st = patient_state[pid]
    lo, hi = patient["normal_rate"]
    st["tick"] += 0.18
    base_rate = (lo + hi) / 2
    variation = (hi - lo) / 4
    drip_rate = round(base_rate + variation * math.sin(st["tick"]) + random.uniform(-1.5, 1.5), 1)

    status = "NORMAL"
    st["anomaly_countdown"] -= 1
    if st["anomaly_countdown"] <= 0:
        st["current_anomaly"] = random.choice(["BLOCKAGE", "FREE_FLOW", "AIR_BUBBLE", None])
        st["anomaly_countdown"] = random.randint(30, 70)

    if st["current_anomaly"] == "BLOCKAGE":
        drip_rate = 0.0
        status = "BLOCKAGE"
        if random.random() < 0.2:
            st["current_anomaly"] = None
    elif st["current_anomaly"] == "FREE_FLOW":
        drip_rate = round(hi * 2.2 + random.uniform(0, 10), 1)
        status = "FREE_FLOW"
        if random.random() < 0.25:
            st["current_anomaly"] = None
    elif st["current_anomaly"] == "AIR_BUBBLE":
        status = "AIR_BUBBLE"
        if random.random() < 0.4:
            st["current_anomaly"] = None

    drip_rate = max(0.0, drip_rate)
    consumed = (drip_rate / 60) * 1.5 * 0.05 * 60
    st["remaining_ml"] = max(0.0, st["remaining_ml"] - consumed)
    fluid_pct = round((st["remaining_ml"] / st["total_ml"]) * 100, 1)

    if fluid_pct <= 0:
        status = "EMPTY"
    elif status == "NORMAL" and fluid_pct < 10:
        status = "LOW_FLUID"

    return {
        "patient_id": pid,
        "patient_name": patient["name"],
        "drip_rate": drip_rate,
        "fluid_pct": fluid_pct,
        "remaining_ml": round(st["remaining_ml"], 1),
        "total_ml": patient["bag_ml"],
        "temperature": round(random.uniform(36.2, 37.4), 1),
        "status": status,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

def start_built_in_simulator():
    sim_client = mqtt.Client(client_id=f"iv_builtin_sim_{int(time.time())}")
    try:
        sim_client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        sim_client.loop_start()
        time.sleep(1.5)

        def _loop(p):
            while True:
                pkt = compute_packet(p)
                if pkt["status"] == "EMPTY":
                    patient_state[p["id"]]["remaining_ml"] = float(p["bag_ml"])
                sim_client.publish(f"smartiv/ward1/{p['id']}", json.dumps(pkt), qos=1)
                time.sleep(1.5)

        for p in PATIENTS:
            threading.Thread(target=_loop, args=(p,), daemon=True).start()
            time.sleep(0.3)
        print("[SIM] Built-in simulator active for all patients.")
    except Exception as e:
        print(f"[SIM] Error: {e}")

# ── Web Routes ────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    # Look for index.html in current directory, or in frontend/
    candidates = [
        os.path.join(BASE_DIR, "index.html"),
        os.path.join(BASE_DIR, "frontend", "index.html"),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return Response(f.read(), mimetype="text/html")
    return Response("<h1>Smart IV Drip Monitor</h1><p>index.html not found.</p>", mimetype="text/html")

@app.route("/api/patients", methods=["GET"])
def api_patients():
    return jsonify(get_latest_readings())

@app.route("/api/live", methods=["GET"])
def api_live():
    return jsonify(list(latest_cache.values()))

@app.route("/api/history/<patient_id>", methods=["GET"])
def api_history(patient_id):
    limit = int(request.args.get("limit", 120))
    return jsonify(get_history(patient_id, limit))

@app.route("/api/alerts/active", methods=["GET"])
def api_alerts_active():
    return jsonify(get_active_alerts())

@app.route("/api/alerts/all", methods=["GET"])
def api_alerts_all():
    return jsonify(get_all_alerts())

@app.route("/api/alerts/<int:alert_id>/acknowledge", methods=["POST"])
def api_acknowledge(alert_id):
    acknowledge_alert(alert_id)
    socketio.emit("alert_acknowledged", {"alert_id": alert_id})
    return jsonify({"ok": True})

@socketio.on("connect")
def on_ws_connect():
    socketio.emit("snapshot", list(latest_cache.values()), to=request.sid)

# ── Bootstrap ─────────────────────────────────────────────────────────────────
init_db()
threading.Thread(target=start_mqtt_subscriber, daemon=True).start()

if os.environ.get("SIMULATE", "true").lower() == "true":
    threading.Thread(target=start_built_in_simulator, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n=======================================================")
    print(f"  Smart IV Drip Monitor")
    print(f"  Live Server on port {port}")
    print(f"=======================================================\n")
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
