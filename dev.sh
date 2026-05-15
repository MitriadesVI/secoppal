 source /Users/rodrigoortiz/Documents/secoppal/.venv/bin/activate
#!/usr/bin/env bash
set -e

PROJECT_DIR="/Users/rodrigoortiz/Documents/secoppal"

cd "$PROJECT_DIR"

echo "🚀 Iniciando SECOPPAL..."

# Activar entorno virtual
source .venv/bin/activate

# Cerrar procesos viejos
echo "🧹 Cerrando procesos anteriores..."
pkill -f "uvicorn app.main:app" 2>/dev/null || true
pkill -f "streamlit run streamlit_app.py" 2>/dev/null || true

cleanup() {
  echo ""
  echo "🛑 Cerrando SECOPPAL..."
  kill "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
  pkill -f "uvicorn app.main:app" 2>/dev/null || true
  pkill -f "streamlit run streamlit_app.py" 2>/dev/null || true
  exit 0
}

trap cleanup INT TERM

echo "⚙️ Iniciando backend..."
uvicorn app.main:app --reload &
BACKEND_PID=$!

sleep 2

echo "🎨 Iniciando Streamlit..."
streamlit run streamlit_app.py &
FRONTEND_PID=$!

echo ""
echo "✅ SECOPPAL iniciado"
echo "Backend:  http://127.0.0.1:8000"
echo "Frontend: http://localhost:8501"
echo ""
echo "Presiona Ctrl+C para cerrar backend y frontend."
echo ""

wait
