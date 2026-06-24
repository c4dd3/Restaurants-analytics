# =============================================================================
# teardown.ps1 - Equivalente a "make teardown" para Windows (PowerShell 5.1+)
#
# Baja Proyecto 1 y Proyecto 2 completamente, eliminando todos los volumenes.
#
# Uso:
#   .\teardown.ps1
#   .\teardown.ps1 -P1 "C:\ruta\al\Restaurants-e2"
# =============================================================================
param(
    [string]$P1 = "..\Restaurants-e2"
)

Write-Host "Bajando Proyecto 2 (con volumenes)..." -ForegroundColor Cyan
try {
    docker compose -f deployments/docker-compose.yml --env-file .env down -v 2>$null
    Write-Host "  OK: Proyecto 2 limpio" -ForegroundColor Green
} catch {
    Write-Host "  WARN: Nada que bajar en P2" -ForegroundColor Yellow
}

$P1Resolved = Resolve-Path $P1 -ErrorAction SilentlyContinue
if ($P1Resolved) {
    Write-Host "Bajando Proyecto 1 (con volumenes)..." -ForegroundColor Cyan
    try {
        Push-Location $P1Resolved.Path
        docker compose -f deployments/docker-compose.yml --profile postgres down -v 2>$null
        Pop-Location
        Write-Host "  OK: Proyecto 1 limpio" -ForegroundColor Green
    } catch {
        Write-Host "  WARN: Nada que bajar en P1" -ForegroundColor Yellow
        try { Pop-Location } catch { }
    }
} else {
    Write-Host "  WARN: Proyecto 1 no encontrado en '$P1' - solo se bajo P2" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Todo limpio." -ForegroundColor Green
