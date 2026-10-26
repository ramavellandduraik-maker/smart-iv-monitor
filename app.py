"""
app.py — Smart IV Drip Monitor Backend
Flask + Flask-SocketIO server that:
  - Subscribes to MQTT topics for all patients
  - Stores every reading in SQLite
  - Runs the alert engine on every packet
  - Serves a REST API consumed by the dashboard
  - Pushes real-time updates to the browser via WebSocket (Socket.IO)

Run:  python app.py
URL:  http://localhost:5000
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import threading
import time

import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO
from flask_cors import CORS
import paho.mqtt.client as mqtt

from backend.db     import (init_db, insert_reading, get_latest_readings,
                             get_history, get_active_alerts, get_all_alerts,
                             acknowledge_alert)
from backend.alert  import evaluate

# ── App setup ─────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONT_DIR = os.path.join(BASE_DIR, "frontend")

app       = Flask(__name__, static_folder=os.path.join(FRONT_DIR, "static"))
CORS(app)
socketio  = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ── MQTT config ───────────────────────────────────────────────────────────────
BROKER_HOST  = "broker.hivemq.com"
BROKER_PORT  = 1883
TOPIC_SUB    = "smartiv/ward1/#"    # subscribe to all patient topics

# In-memory cache of the latest reading per patient
latest_cache: dict[str, dict] = {}

# ── MQTT callbacks ─────────────────────────────────────────────────────────────

def on_connect(client, _ud, _flags, rc):
    if rc == 0:
        client.subscribe(TOPIC_SUB, qos=1)
        print(f"[MQTT] Connected — subscribed to {TOPIC_SUB}")
    else:
        print(f"[MQTT] Connection failed rc={rc}")


def on_message(_client, _ud, msg):
    try:
        data = json.loads(msg.payload.decode())
    except Exception as e:
        print(f"[MQTT] Bad payload: {e}")
        return

    pid = data.get("patient_id", "unknown")

    # 1. Persist to DB
    insert_reading(data)

    # 2. Update in-memory cache
    latest_cache[pid] = data

    # 3. Run alert engine
    evaluate(data, socketio=socketio)

    # 4. Push live update to all connected browser clients
    socketio.emit("reading", data)


def start_mqtt():
    client = mqtt.Client(client_id=f"iv_backend_{int(time.time())}")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    print("[MQTT] Connecting to broker …")
    client.loop_forever()


# ── REST API ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(FRONT_DIR, "index.html")


@app.route("/api/patients", methods=["GET"])
def api_patients():
    """Latest reading for every patient."""
    rows = get_latest_readings()
    return jsonify(rows)


@app.route("/api/live", methods=["GET"])
def api_live():
    """Snapshot of the in-memory cache (fastest path)."""
    return jsonify(list(latest_cache.values()))


@app.route("/api/history/<patient_id>", methods=["GET"])
def api_history(patient_id):
    limit = int(request.args.get("limit", 120))
    rows  = get_history(patient_id, limit)
    return jsonify(rows)


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


# ── Socket.IO events ──────────────────────────────────────────────────────────

@socketio.on("connect")
def on_ws_connect():
    print(f"[WS] Client connected: {request.sid}")
    # Send current snapshot on connect
    socketio.emit("snapshot", list(latest_cache.values()), to=request.sid)


@socketio.on("disconnect")
def on_ws_disconnect():
    print(f"[WS] Client disconnected: {request.sid}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os as _os
    init_db()

    # Start MQTT subscriber in a background thread
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()

    # If SIMULATE=true env var is set, also run the built-in simulator
    # (useful for Render / cloud where you run a single service)
    if _os.environ.get("SIMULATE", "true").lower() == "true":
        import sys as _sys
        _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
        from simulator.sensor_sim import PATIENTS, compute_packet, patient_state, PUBLISH_INTERVAL
        import paho.mqtt.client as _mqtt_client
        import json as _json

        def _run_builtin_sim():
            import time as _time
            _client = _mqtt_client.Client(client_id=f"iv_builtin_sim_{int(_time.time())}")
            _client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
            _client.loop_start()
            _time.sleep(2)

            def _patient_loop(patient):
                while True:
                    pkt = compute_packet(patient)
                    if pkt["status"] == "EMPTY":
                        patient_state[patient["id"]]["remaining_ml"] = float(patient["bag_ml"])
                    _client.publish(f"smartiv/ward1/{patient['id']}",
                                    _json.dumps(pkt), qos=1)
                    _time.sleep(PUBLISH_INTERVAL)

            for _p in PATIENTS:
                threading.Thread(target=_patient_loop, args=(_p,), daemon=True).start()
                _time.sleep(0.3)

        sim_thread = threading.Thread(target=_run_builtin_sim, daemon=True)
        sim_thread.start()
        print("[APP] Built-in simulator started (SIMULATE=true)")

    port = int(_os.environ.get("PORT", 5000))   # Render sets PORT automatically

    print("\n" + "="*55)
    print("  Smart IV Drip Monitor — Backend Server")
    print(f"  Dashboard: http://localhost:{port}")
    print("="*55 + "\n")

    socketio.run(app, host="0.0.0.0", port=port, debug=False)
