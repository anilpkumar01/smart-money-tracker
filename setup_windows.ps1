# =====================================================
# setup_windows.ps1  —  Windows 11 / PowerShell
# =====================================================
# HOW TO RUN:
#   Option A — from VSCode terminal (Ctrl+`):
#       .\setup_windows.ps1
#
#   Option B — from Windows Explorer:
#       Right-click setup_windows.ps1 → Run with PowerShell
#
# If you see "running scripts is disabled":
#   Run this FIRST (one time only):
#       Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#   Then run the script again.
# =====================================================

$ErrorActionPreference = "Stop"

function Print-Step($msg) {
    Write-Host ""
    Write-Host "  $msg" -ForegroundColor Cyan
}

function Print-OK($msg) {
    Write-Host "  OK: $msg" -ForegroundColor Green
}

function Print-Error($msg) {
    Write-Host "  ERROR: $msg" -ForegroundColor Red
}

Write-Host ""
Write-Host "=============================================" -ForegroundColor DarkCyan
Write-Host "  Smart Money Tracker — Windows 11 Setup    " -ForegroundColor DarkCyan
Write-Host "=============================================" -ForegroundColor DarkCyan

# ── Check Python is installed ────────────────────────────────────────────────
Print-Step "[1/5] Checking Python..."
try {
    $pyVersion = python --version 2>&1
    Print-OK "Found: $pyVersion"
} catch {
    Print-Error "Python not found. Download from https://www.python.org/downloads/"
    Print-Error "Make sure to check 'Add Python to PATH' during install."
    exit 1
}

# ── Create virtual environment ───────────────────────────────────────────────
Print-Step "[2/5] Creating virtual environment (venv)..."
if (Test-Path "venv") {
    Write-Host "  venv already exists — skipping creation." -ForegroundColor Yellow
} else {
    python -m venv venv
    Print-OK "venv created at .\venv\"
}

# ── Activate virtual environment ─────────────────────────────────────────────
Print-Step "[3/5] Activating virtual environment..."
try {
    & "venv\Scripts\Activate.ps1"
    Print-OK "venv activated — you should see (venv) in your prompt"
} catch {
    Print-Error "Could not activate venv. Try running:"
    Write-Host "  Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser" -ForegroundColor Yellow
    exit 1
}

# ── Install dependencies ─────────────────────────────────────────────────────
Print-Step "[4/5] Installing dependencies from requirements.txt..."
pip install --upgrade pip --quiet
pip install -r requirements.txt
Print-OK "All packages installed"

# ── Create data folder ───────────────────────────────────────────────────────
Print-Step "[5/5] Creating data folder..."
if (-Not (Test-Path "data")) {
    New-Item -ItemType Directory -Name "data" | Out-Null
    Print-OK "data\ folder created"
} else {
    Print-OK "data\ folder already exists"
}

# ── Done ─────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=============================================" -ForegroundColor Green
Write-Host "  Setup complete!" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Run these commands next (in order):" -ForegroundColor White
Write-Host ""
Write-Host "  TERMINAL 1 (keep open — starts Ollama AI):" -ForegroundColor DarkYellow
Write-Host "    ollama serve" -ForegroundColor Yellow
Write-Host ""
Write-Host "  TERMINAL 2 (your venv terminal in VSCode):" -ForegroundColor DarkCyan
Write-Host "    python data_pipeline.py   <- fetches live data (~60 sec)" -ForegroundColor Cyan
Write-Host "    streamlit run app.py      <- launches the app" -ForegroundColor Cyan
Write-Host ""
Write-Host "  IMPORTANT — every time you reopen VSCode:" -ForegroundColor DarkYellow
Write-Host "    venv\Scripts\Activate.ps1" -ForegroundColor Yellow
Write-Host "  (VSCode may do this automatically via .vscode\settings.json)" -ForegroundColor DarkGray
Write-Host ""
