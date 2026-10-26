@echo off
title Smart IV Drip Monitor

echo.
echo  =====================================================
echo   Smart IV Drip Monitor  ^|  Medical IoT PBL
echo  =====================================================
echo.
echo  Installing dependencies...
pip install -r requirements.txt --quiet

echo.
echo  Starting Backend Server (Flask + MQTT subscriber)...
start "IV Monitor Backend" cmd /k "python app.py"

timeout /t 3 /nobreak >nul

echo  Starting Sensor Simulator...
start "IV Sensor Simulator" cmd /k "python simulator\sensor_sim.py"

timeout /t 2 /nobreak >nul

echo.
echo  Opening Dashboard in browser...
start http://localhost:5000

echo.
echo  Both windows are running. Close them to stop the system.
pause
