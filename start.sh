#!/bin/bash

# Quick Start Script for HL7 Lab Machine Interface

echo "=========================================="
echo "HL7 Lab Machine Interface - Quick Start"
echo "=========================================="
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "⚠️  Creating .env file..."
    cp .env.example .env
    echo "✅ .env file created"
    echo ""
    echo "⚠️  IMPORTANT: Edit .env file and set your API_URL and API_KEY"
    echo ""
fi

set -a
source .env
set +a

HTTP_PORT=${PORT:-5001}
MLLP_LISTEN_PORT=${MLLP_PORT:-6000}
ASTM_LISTEN_PORT=${ASTM_PORT:-7000}

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Get Local IP
LOCAL_IP=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | awk '{print $2}' | head -n 1)

if [ -f "mllp_server.py" ]; then
    echo "🔌 Starting MLLP Listener (Port $MLLP_LISTEN_PORT)..."
    python mllp_server.py &
    MLLP_PID=$!
    echo "   - MLLP Server running on PID $MLLP_PID"
fi

if [ -f "astm_server.py" ]; then
    echo "🔌 Starting ASTM Listener (Port $ASTM_LISTEN_PORT)..."
    python astm_server.py &
    ASTM_PID=$!
    echo "   - ASTM Server running on PID $ASTM_PID"
fi

echo ""
echo "🚀 Starting Flask application..."
echo "   - Server will listen on http://0.0.0.0:$HTTP_PORT"
echo "   - Endpoint: http://localhost:$HTTP_PORT/hl7/receive"
echo "   - Health check: http://localhost:$HTTP_PORT/health"
echo ""
echo "📡 Lab Configuration:"
echo "   OPTION 1: HTTP Post -> http://$LOCAL_IP:$HTTP_PORT/hl7/receive"
echo "   OPTION 2: MLLP/TCP (HL7)  -> $LOCAL_IP Port $MLLP_LISTEN_PORT"
echo "   OPTION 3: ASTM/TCP        -> $LOCAL_IP Port $ASTM_LISTEN_PORT"
echo ""
echo "Press Ctrl+C to stop all servers"
echo "=========================================="
echo ""

# Function to kill background processes on exit
cleanup() {
    echo "Stopping servers..."
    kill $MLLP_PID 2>/dev/null
    kill $ASTM_PID 2>/dev/null
    exit
}
trap cleanup SIGINT

# Run the Flask app
python app.py
