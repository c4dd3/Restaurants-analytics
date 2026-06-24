# =============================================================================
# setup.ps1 - Equivalente a "make setup" para Windows (PowerShell 5.1+)
#
# Uso:
#   .\setup.ps1                          # usa ruta por defecto ..\Restaurants-e2
#   .\setup.ps1 -P1 "C:\ruta\al\repo"   # ruta personalizada del Proyecto 1
#
# Requisitos:
#   - Docker Desktop for Windows (con WSL2 backend)
#   - Python 3  (en PATH como "python" o "python3")
#   - Go 1.21+  (en PATH)
#   - PowerShell 5.1+ o PowerShell 7+
# =============================================================================
param(
    [string]$P1 = "..\Restaurants-e2"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# -- Colores ------------------------------------------------------------------
function Write-Header([string]$msg) { Write-Host "" ; Write-Host $msg -ForegroundColor Cyan }
function Write-Step([string]$msg)   { Write-Host "  $msg" -ForegroundColor White }
function Write-Ok([string]$msg)     { Write-Host "  OK: $msg" -ForegroundColor Green }
function Write-Warn([string]$msg)   { Write-Host "  WARN: $msg" -ForegroundColor Yellow }
function Write-Fail([string]$msg)   { Write-Host "  ERROR: $msg" -ForegroundColor Red ; exit 1 }

# -- Detectar python ----------------------------------------------------------
function Get-Python {
    foreach ($cmd in @("python", "python3")) {
        if (Get-Command $cmd -ErrorAction SilentlyContinue) { return $cmd }
    }
    Write-Fail "Python no encontrado en PATH. Instala Python 3 desde https://python.org"
}

# -- Leer valor del .env ------------------------------------------------------
function Get-EnvValue([string]$key) {
    if (-not (Test-Path ".env")) { return "" }
    $line = Get-Content ".env" | Where-Object { $_ -match "^$key=" } | Select-Object -First 1
    if ($line) { return ($line -replace "^$key=", "").Trim() }
    return ""
}

# -- Esperar condicion con timeout --------------------------------------------
function Wait-For([string]$description, [int]$maxTries, [int]$sleepSec, [scriptblock]$check) {
    for ($i = 1; $i -le $maxTries; $i++) {
        try {
            $ok = & $check
            if ($ok) { Write-Ok "$description listo"; return }
        } catch { }
        Write-Step "... esperando $description ($i/$maxTries)"
        Start-Sleep -Seconds $sleepSec
    }
    Write-Fail "$description no respondio despues de $($maxTries * $sleepSec)s"
}

# =============================================================================
Write-Host ""
Write-Host "=============================================="  -ForegroundColor Cyan
Write-Host "  RESTAURANTS ANALYTICS - SETUP COMPLETO"       -ForegroundColor Cyan
Write-Host "=============================================="  -ForegroundColor Cyan

$P1Resolved = Resolve-Path $P1 -ErrorAction SilentlyContinue
if (-not $P1Resolved) { Write-Fail "No se encontro el Proyecto 1 en '$P1'. Usa -P1 para indicar la ruta." }
$P1 = $P1Resolved.Path
$PYTHON = Get-Python

# -- [1/9] Bajar Proyecto 2 ---------------------------------------------------
Write-Header "[1/9] Bajando Proyecto 2 (con volumenes)..."
try { docker compose -f deployments/docker-compose.yml --env-file .env down -v 2>$null } catch { Write-Warn "Nada que bajar en P2" }

# -- [2/9] Bajar Proyecto 1 ---------------------------------------------------
Write-Header "[2/9] Bajando Proyecto 1 (con volumenes)..."
try {
    Push-Location $P1
    docker compose -f deployments/docker-compose.yml --profile postgres down -v 2>$null
    Pop-Location
} catch {
    Write-Warn "Nada que bajar en P1"
    try { Pop-Location } catch { }
}

# -- [3/9] Levantar Proyecto 1 ------------------------------------------------
Write-Header "[3/9] Levantando Proyecto 1..."
Push-Location $P1
$env:DB_ENGINE = "postgres"
docker compose -f deployments/docker-compose.yml --profile postgres up --build -d
Pop-Location
Remove-Item Env:\DB_ENGINE -ErrorAction SilentlyContinue

Write-Step "Esperando que la API del Proyecto 1 este lista..."
Wait-For "API Proyecto 1" 24 5 {
    try {
        $r = Invoke-WebRequest -Uri "http://localhost/api/health" -UseBasicParsing -TimeoutSec 3
        return ($r.StatusCode -eq 200)
    } catch { return $false }
}

Write-Step "Asegurando contrasena de postgres en P1..."
$ErrorActionPreference = "Continue"
docker exec re2_postgres psql -U postgres -c "ALTER USER postgres PASSWORD 'postgres';" 2>&1 | Out-Null
$ErrorActionPreference = "Stop"
Write-Ok "Contrasena de postgres confirmada"

# -- [4/9] Seed base ----------------------------------------------------------
Write-Header "[4/9] Sembrando datos base en Proyecto 1..."
Push-Location $P1

# PowerShell no carga .env automaticamente — exportar variables de P1 al proceso
if (Test-Path ".env") {
    Get-Content ".env" | Where-Object { $_ -match "^[A-Za-z_][A-Za-z0-9_]*=" } | ForEach-Object {
        $parts = $_ -split "=", 2
        $k = $parts[0].Trim()
        $v = if ($parts.Length -gt 1) { $parts[1].Trim() } else { "" }
        [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
    }
    Write-Step "Variables de P1 cargadas desde .env"
}

go run ./scripts/seed -restaurants=10 -menus-per=2 -products-per=8 -users=20
if ($LASTEXITCODE -ne 0) { Pop-Location; Write-Fail "Seed base fallo" }
Pop-Location
Write-Ok "Seed base completado"

# -- [5/9] Seed transacciones -------------------------------------------------
Write-Header "[5/9] Sembrando transacciones (ordenes + reservaciones)..."
$ErrorActionPreference = "Continue"
& $PYTHON -m pip install psycopg2-binary --quiet 2>&1 | Out-Null
$ErrorActionPreference = "Stop"
& $PYTHON scripts/seed_transactions.py --orders 300 --reservations 150
if ($LASTEXITCODE -ne 0) { Write-Fail "seed_transactions.py fallo" }
Write-Ok "Transacciones sembradas"

# -- [6/9] Configurar .env y levantar stack -----------------------------------
Write-Header "[6/9] Configurando .env y levantando stack de analitica..."
$ErrorActionPreference = "Continue"
& $PYTHON -m pip install cryptography --quiet 2>&1 | Out-Null
$ErrorActionPreference = "Stop"
& $PYTHON scripts/configure_env.py
if ($LASTEXITCODE -ne 0) { Write-Fail "configure_env.py fallo" }

docker compose -f deployments/docker-compose.yml --env-file .env up --build -d

Write-Step "Esperando que analytics-db este lista..."
Wait-For "analytics-db" 20 5 {
    $ErrorActionPreference = "Continue"
    docker exec ra_analytics_db pg_isready -U analytics 2>&1 | Out-Null
    $ErrorActionPreference = "Stop"
    return ($LASTEXITCODE -eq 0)
}

Write-Step "Creando bases de datos requeridas (airflow, metabase)..."
foreach ($db in @("airflow", "metabase")) {
    try {
        docker exec ra_analytics_db psql -U analytics -d hive_metastore -c "CREATE DATABASE $db;" 2>$null
        Write-Ok "Base de datos '$db' creada"
    } catch {
        Write-Warn "Base de datos '$db' ya existe"
    }
}

Write-Step "Reiniciando Airflow..."
docker restart ra_airflow

Write-Step "Esperando que Airflow DB este lista..."
Wait-For "Airflow DB" 24 5 {
    $ErrorActionPreference = "Continue"
    docker exec ra_airflow airflow db check 2>&1 | Out-Null
    $ErrorActionPreference = "Stop"
    return ($LASTEXITCODE -eq 0)
}

Write-Step "Creando conexiones de Airflow..."
$connections = @(
    [PSCustomObject]@{
        id   = "postgres_proyecto1"
        args = @("--conn-type","postgres","--conn-host","re2_postgres",
                 "--conn-port","5432","--conn-schema","restaurants",
                 "--conn-login","postgres","--conn-password","postgres")
    },
    [PSCustomObject]@{
        id   = "spark_default"
        args = @("--conn-type","spark","--conn-host","spark://spark-master","--conn-port","7077")
    }
)
foreach ($conn in $connections) {
    $ErrorActionPreference = "Continue"
    docker exec ra_airflow airflow connections add $conn.id @($conn.args) 2>&1 | Out-Null
    $ErrorActionPreference = "Stop"
    if ($LASTEXITCODE -eq 0) {
        Write-Ok "Conexion '$($conn.id)' creada"
    } else {
        Write-Warn "Conexion '$($conn.id)' ya existe o fallo (exit $LASTEXITCODE)"
    }
}

# -- [7/9] Esquema Hive -------------------------------------------------------
Write-Header "[7/9] Creando esquema Hive (dimensiones, hechos, vistas OLAP)..."

Write-Step "Esperando que ra_hive_server este corriendo..."
Wait-For "ra_hive_server" 24 10 {
    $status = docker inspect --format="{{.State.Status}}" ra_hive_server 2>$null
    return ($status -eq "running")
}

Write-Step "Esperando que HiveServer2 acepte conexiones (puerto 10000)..."
Wait-For "HiveServer2" 24 10 {
    $ErrorActionPreference = "Continue"
    $out = docker exec ra_hive_server bash -c "echo > /dev/tcp/localhost/10000" 2>&1
    $ErrorActionPreference = "Stop"
    return ($LASTEXITCODE -eq 0)
}

foreach ($schema in @("01_dimensions", "02_facts", "03_olap_views")) {
    $file = "hive\schema\$schema.hql"
    Write-Step "Ejecutando $schema.hql..."
    $ErrorActionPreference = "Continue"
    Get-Content $file | docker exec -i ra_hive_server beeline `
        -u "jdbc:hive2://localhost:10000" -n root --silent=true 2>&1 | Out-Null
    $ErrorActionPreference = "Stop"
    if ($LASTEXITCODE -ne 0) { Write-Fail "Error ejecutando $schema.hql" }
    Write-Ok "$schema.hql ejecutado"
}

# -- [8/9] ETL (Airflow DAG) --------------------------------------------------
Write-Header "[8/9] Ejecutando ETL (Airflow DAG)..."

Write-Step "Esperando que Airflow este healthy..."
Wait-For "Airflow" 40 15 {
    $ErrorActionPreference = "Continue"
    $status = docker inspect --format="{{.State.Health.Status}}" ra_airflow 2>&1
    $ErrorActionPreference = "Stop"
    return ($status -eq "healthy")
}

docker exec ra_airflow airflow dags unpause restaurants_etl
docker exec ra_airflow airflow dags trigger restaurants_etl
Write-Step "ETL en ejecucion - podés monitorear en http://localhost:8085"

Write-Step "Esperando que el DAG termine (max 20 min)..."
$dagDone = $false
for ($i = 1; $i -le 80; $i++) {
    $rawState = docker exec ra_airflow airflow dags list-runs -d restaurants_etl --output plain 2>$null |
                Select-Object -Skip 1 -First 1 |
                ForEach-Object { ($_ -split '\s+')[2] }
    $state = if ($rawState) { $rawState } else { "running" }

    if ($state -eq "success") {
        Write-Ok "DAG completado exitosamente"
        $dagDone = $true
        break
    } elseif ($state -eq "failed") {
        Write-Fail "DAG fallo - revisa http://localhost:8085"
    }
    Write-Step "... DAG estado: $state ($i/80)"
    Start-Sleep -Seconds 15
}
if (-not $dagDone) { Write-Warn "Timeout esperando el DAG - revisa el estado en Airflow UI" }

Write-Step "Verificando datos en Hive..."
$ErrorActionPreference = "Continue"
docker exec ra_hive_server beeline -u "jdbc:hive2://localhost:10000" `
    -n root --silent=true `
    -e "SELECT 'fact_items_pedido', COUNT(*) FROM restaurants_dw.fact_items_pedido UNION ALL SELECT 'dim_tiempo', COUNT(*) FROM restaurants_dw.dim_tiempo;" `
    2>&1 | Where-Object { $_ -notmatch "SLF4J" }
$ErrorActionPreference = "Stop"

# -- [9/9] Neo4J + Metabase ---------------------------------------------------
Write-Header "[9/9] Cargando grafo Neo4J y configurando Metabase..."

Write-Step "Ejecutando Spark analytics job..."
docker exec ra_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.driver.host=spark-master `
    /opt/spark-apps/jobs/restaurants_spark_analytics.py --source postgres
if ($LASTEXITCODE -ne 0) { Write-Fail "Spark analytics job fallo" }

$neo4jPass = Get-EnvValue "NEO4J_PASSWORD"
if (-not $neo4jPass) { $neo4jPass = "Analytics2024!" }

Write-Step "Cargando constraints y grafo en Neo4J..."
Get-Content "neo4j\queries\00_constraints.cypher", "neo4j\queries\01_load_graph.cypher" |
    docker exec -i ra_neo4j cypher-shell -u neo4j -p $neo4jPass
if ($LASTEXITCODE -ne 0) { Write-Fail "Carga del grafo Neo4J fallo" }
Write-Ok "Grafo cargado"

Write-Step "Configurando dashboards de Metabase..."
$ErrorActionPreference = "Continue"
& $PYTHON -m pip install requests --quiet 2>&1 | Out-Null
$ErrorActionPreference = "Stop"
& $PYTHON dashboards/metabase/setup_metabase.py
if ($LASTEXITCODE -ne 0) { Write-Warn "setup_metabase.py tuvo errores - revisa manualmente" }

# -- FIN ----------------------------------------------------------------------
Write-Host ""
Write-Host "=============================================="  -ForegroundColor Green
Write-Host "  SETUP COMPLETO"                               -ForegroundColor Green
Write-Host ""
Write-Host "  Airflow:  http://localhost:8085  (admin / admin)"
Write-Host "  Spark:    http://localhost:8090"
Write-Host "  Neo4J:    http://localhost:7474  (neo4j / $neo4jPass)"
Write-Host "  Metabase: http://localhost:3000  (admin@restaurants.local / Admin1234!)"
Write-Host "=============================================="  -ForegroundColor Green
Write-Host ""
