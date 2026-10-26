#!/bin/bash
echo "Installing dependencies..."
pip install -r requirements.txt --quiet

echo "Starting backend server..."
python app.py &
BACKEND_PID=$!

sleep 2

echo "Starting sensor simulator..."
python simulator/sensor_sim.py &
SIM_PID=$!

echo ""
echo "========================================"
echo "  Dashboard: http://localhost:5000"
echo "  Press Ctrl+C to stop both processes"
echo "========================================"

trap "kill $BACKEND_PID $SIM_PID; exit" INT
wait
