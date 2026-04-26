#!/bin/bash
# =====================================================
# setup_mac_linux.sh
# Run this once inside the smart_money_tracker folder:
#   chmod +x setup_mac_linux.sh
#   ./setup_mac_linux.sh
# =====================================================

echo ""
echo "[1/4] Creating virtual environment..."
python3 -m venv venv

echo "[2/4] Activating virtual environment..."
source venv/bin/activate

echo "[3/4] Installing dependencies..."
pip install -r requirements.txt

echo "[4/4] Creating data folder..."
mkdir -p data

echo ""
echo "[Done] Setup complete."
echo ""
echo "Next steps:"
echo "  1. In a SEPARATE terminal: ollama serve"
echo "  2. Back here: python data_pipeline.py"
echo "  3. Then:      streamlit run app.py"
echo ""
echo "Each time you reopen VSCode, re-activate with:"
echo "  source venv/bin/activate"
