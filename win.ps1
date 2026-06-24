# =============================================================================
# win.ps1 - Equivalente al Makefile completo para Windows (PowerShell 5.1+)
#
# Uso:
#   .\win.ps1 <comando> [-P1 "C:\ruta\al\repo"]
#
# Comandos disponibles:
#   setup          Levanta todo desde cero (igual que make setup)
#   teardown       Baja P1 y P2 con volumenes
#   up             Levanta solo el stack de analitica
#   down           Baja el stack conservando volumenes
#   down-v         Baja el stack y elimina volumenes
#   logs           Sigue los logs en tiempo real
#   ps             Estado de todos los contenedores
#   beeline        Abre Beeline conectado a HiveServer2
#   neo4j-shell    Abre Cypher Shell en Neo4J
#   spark-shell    Abre Spark Shell en el master
#   neo4j-load     Recarga constraints y grafo desde los CSVs
#   neo4j-analysis Ejecuta las queries de analisis
#   spark-sample   Corre el job de Spark con datos de muestra
# =============================================================================
param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("setup","teardown","up","down","down-v","logs","ps",
                 "beeline","neo4j-shell","spark-shell",
                 "neo4j-load","neo4j-analysis","spark-sample")]
    [string]$Command,

    [string]$P1 = "..\Restaurants-e2"
)

$COMPOSE = @("docker","compose","-f","deployments/docker-compose.yml","--env-file",".env")

function Get-Neo4jPassword {
    if (Test-Path ".env") {
        $line = Get-Content ".env" | Where-Object { $_ -match "^NEO4J_PASSWORD=" } | Select-Object -First 1
        if ($line) { return ($line -replace "^NEO4J_PASSWORD=","").Trim() }
    }
    return "Analytics2024!"
}

switch ($Command) {

    "setup" {
        & "$PSScriptRoot\setup.ps1" -P1 $P1
    }

    "teardown" {
        & "$PSScriptRoot\teardown.ps1" -P1 $P1
    }

    "up" {
        docker compose -f deployments/docker-compose.yml --env-file .env up --build -d
    }

    "down" {
        docker compose -f deployments/docker-compose.yml --env-file .env down
    }

    "down-v" {
        docker compose -f deployments/docker-compose.yml --env-file .env down -v
    }

    "logs" {
        docker compose -f deployments/docker-compose.yml --env-file .env logs -f
    }

    "ps" {
        docker compose -f deployments/docker-compose.yml --env-file .env ps
    }

    "beeline" {
        docker exec -it ra_hive_server beeline -u jdbc:hive2://localhost:10000
    }

    "neo4j-shell" {
        $pass = Get-Neo4jPassword
        docker exec -it ra_neo4j cypher-shell -u neo4j -p $pass
    }

    "spark-shell" {
        docker exec -it ra_spark_master /opt/spark/bin/spark-shell `
            --master spark://spark-master:7077
    }

    "neo4j-load" {
        $pass = Get-Neo4jPassword
        Get-Content "neo4j\queries\00_constraints.cypher" |
            docker exec -i ra_neo4j cypher-shell -u neo4j -p $pass
        Get-Content "neo4j\queries\01_load_graph.cypher" |
            docker exec -i ra_neo4j cypher-shell -u neo4j -p $pass
        Write-Host "OK: Grafo cargado" -ForegroundColor Green
    }

    "neo4j-analysis" {
        $pass = Get-Neo4jPassword
        Get-Content "neo4j\queries\02_analysis_queries.cypher" |
            docker exec -i ra_neo4j cypher-shell -u neo4j -p $pass
    }

    "spark-sample" {
        docker exec -it ra_spark_master /opt/spark/bin/spark-submit `
            --master spark://spark-master:7077 `
            /opt/spark-apps/jobs/restaurants_spark_analytics.py `
            --source sample `
            --output-base /tmp/restaurants-output `
            --neo4j-output /opt/neo4j-import `
            --couriers 2
    }
}
