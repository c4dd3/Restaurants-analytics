COMPOSE = docker compose -f deployments/docker-compose.yml --env-file .env

# =============================================================================
# setup / demo-reset: inicialización completa desde cero.
#
# Uso:
#   make setup                        # usa ruta por defecto ../Restaurants-e2
#   make setup P1=~/otro/path/repo    # ruta personalizada del Proyecto 1
#   make demo-reset                   # alias de make setup
# =============================================================================
P1 ?= ../Restaurants-e2

.PHONY: setup
setup:
	@echo ""
	@echo "══════════════════════════════════════════════"
	@echo "  RESTAURANTS ANALYTICS — SETUP COMPLETO"
	@echo "══════════════════════════════════════════════"

	@echo ""
	@echo "▶ [1/9] Bajando Proyecto 2 (con volúmenes)..."
	$(COMPOSE) down -v 2>/dev/null || true

	@echo ""
	@echo "▶ [2/9] Bajando Proyecto 1 (con volúmenes)..."
	cd $(P1) && docker compose -f deployments/docker-compose.yml --profile postgres down -v 2>/dev/null || true

	@echo ""
	@echo "▶ [3/9] Levantando Proyecto 1..."
	cd $(P1) && DB_ENGINE=postgres docker compose -f deployments/docker-compose.yml --profile postgres up --build -d
	@echo "   Esperando que la API del Proyecto 1 esté lista..."
	@for i in $$(seq 1 24); do \
		curl -sf http://localhost/api/health > /dev/null 2>&1 && echo "   ✓ Proyecto 1 API healthy" && break; \
		echo "   ... esperando API P1 ($$i/24)"; sleep 5; \
	done

	@echo ""
	@echo "▶ [4/9] Sembrando datos base (Proyecto 1 — requiere Go y ANTHROPIC_API_KEY)..."
	cd $(P1) && go run ./scripts/seed -restaurants=10 -menus-per=2 -products-per=8 -users=20
	@echo "   ✓ Seed base completado"

	@echo ""
	@echo "▶ [5/9] Sembrando transacciones (órdenes + reservaciones)..."
	pip3 install psycopg2-binary -q 2>/dev/null || true
	python3 scripts/seed_transactions.py --orders 300 --reservations 150

	@echo ""
	@echo "▶ [6/9] Configurando .env y levantando stack de analítica (Proyecto 2)..."
	pip3 install cryptography -q 2>/dev/null || true
	python3 scripts/configure_env.py
	$(COMPOSE) up --build -d
	@echo "   Esperando que analytics-db esté lista..."
	@for i in $$(seq 1 20); do \
		docker exec ra_analytics_db pg_isready -U analytics > /dev/null 2>&1 && break; \
		echo "   ... esperando analytics-db ($$i/20)"; sleep 5; \
	done
	@echo "   Creando bases de datos requeridas (airflow, metabase)..."
	@docker exec ra_analytics_db psql -U analytics -d hive_metastore -c "CREATE DATABASE airflow;"  2>/dev/null || echo "   (airflow ya existe)"
	@docker exec ra_analytics_db psql -U analytics -d hive_metastore -c "CREATE DATABASE metabase;" 2>/dev/null || echo "   (metabase ya existe)"
	@echo "   Reiniciando Airflow con las bases listas..."
	docker restart ra_airflow
	@echo "   Esperando que Airflow arranque..."
	@for i in $$(seq 1 24); do \
		docker exec ra_airflow airflow db check > /dev/null 2>&1 && echo "   ✓ Airflow DB lista" && break; \
		echo "   ... esperando Airflow DB ($$i/24)"; sleep 5; \
	done
	@echo "   Creando conexiones de Airflow (postgres_proyecto1, spark_default)..."
	@docker exec ra_airflow airflow connections add postgres_proyecto1 \
		--conn-type postgres \
		--conn-host re2_postgres \
		--conn-port 5432 \
		--conn-schema restaurants \
		--conn-login postgres \
		--conn-password postgres 2>/dev/null || echo "   (postgres_proyecto1 ya existe)"
	@docker exec ra_airflow airflow connections add spark_default \
		--conn-type spark \
		--conn-host spark://spark-master \
		--conn-port 7077 2>/dev/null || echo "   (spark_default ya existe)"
	@echo "   ✓ Conexiones configuradas"

	@echo ""
	@echo "▶ [7/9] Creando esquema Hive (dimensiones, hechos, vistas OLAP)..."
	@for schema in 01_dimensions 02_facts 03_olap_views; do \
		docker exec -i ra_hive_server beeline \
			-u "jdbc:hive2://localhost:10000" -n root --silent=true 2>&1 \
			< hive/schema/$${schema}.hql && echo "   ✓ $${schema}.hql ejecutado"; \
	done

	@echo ""
	@echo "▶ [8/9] Ejecutando ETL (Airflow DAG)..."
	@echo "   Esperando que Airflow esté healthy..."
	@for i in $$(seq 1 40); do \
		STATUS=$$(docker inspect --format='{{.State.Health.Status}}' ra_airflow 2>/dev/null); \
		if [ "$$STATUS" = "healthy" ]; then \
			echo "   ✓ Airflow healthy"; break; \
		fi; \
		echo "   ... intento $$i/40 (estado: $$STATUS) — esperando 15s"; \
		sleep 15; \
	done
	docker exec ra_airflow airflow dags unpause restaurants_etl
	docker exec ra_airflow airflow dags trigger restaurants_etl
	@echo "   ETL en ejecución — monitorea en http://localhost:8085"
	@echo "   Esperando que el DAG termine (máx 20 min)..."
	@for i in $$(seq 1 80); do \
		STATE=$$(docker exec ra_airflow airflow dags list-runs -d restaurants_etl --output plain 2>/dev/null | awk 'NR==2{print $$3}'); \
		if [ "$$STATE" = "success" ]; then \
			echo "   ✓ DAG completado exitosamente"; break; \
		elif [ "$$STATE" = "failed" ]; then \
			echo "   ✗ DAG falló — revisa http://localhost:8085"; exit 1; \
		fi; \
		echo "   ... DAG estado: $${STATE:-running} ($$i/80)"; sleep 15; \
	done
	@echo "   Verificando datos en Hive..."
	@docker exec ra_hive_server beeline -u "jdbc:hive2://localhost:10000" \
		-n root --silent=true \
		-e "SELECT 'fact_items_pedido', COUNT(*) FROM restaurants_dw.fact_items_pedido UNION ALL SELECT 'dim_tiempo', COUNT(*) FROM restaurants_dw.dim_tiempo;" \
		2>&1 | grep -v SLF4J || true

	@echo ""
	@echo "▶ [9/9] Cargando grafo Neo4J y configurando Metabase..."
	docker exec ra_spark_master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.driver.host=spark-master \
		/opt/spark-apps/jobs/restaurants_spark_analytics.py --source postgres
	cat neo4j/queries/00_constraints.cypher neo4j/queries/01_load_graph.cypher | \
		docker exec -i ra_neo4j cypher-shell -u neo4j -p $$(grep NEO4J_PASSWORD .env | cut -d= -f2)
	python3 dashboards/metabase/setup_metabase.py

	@echo ""
	@echo "══════════════════════════════════════════════"
	@echo "  ✅  SETUP COMPLETO"
	@echo ""
	@echo "  Airflow:  http://localhost:8085  (admin / admin)"
	@echo "  Spark:    http://localhost:8090"
	@echo "  Neo4J:    http://localhost:7474  (neo4j / Analytics2024!)"
	@echo "  Metabase: http://localhost:3000  (admin@restaurants.local / Admin1234!)"
	@echo "══════════════════════════════════════════════"
	@echo ""

.PHONY: demo-reset
demo-reset: setup

# =============================================================================
# teardown: baja todo (P1 + P2) con volúmenes limpiamente.
# =============================================================================
.PHONY: teardown
teardown:
	@echo "▶ Bajando Proyecto 2 (con volúmenes)..."
	$(COMPOSE) down -v 2>/dev/null || true
	@echo "▶ Bajando Proyecto 1 (con volúmenes)..."
	cd $(P1) && docker compose -f deployments/docker-compose.yml --profile postgres down -v 2>/dev/null || true
	@echo "✓ Todo limpio"

# =============================================================================
# up / down / logs / ps
# =============================================================================
.PHONY: up
up:
	$(COMPOSE) up --build -d

.PHONY: down
down:
	$(COMPOSE) down

.PHONY: down-v
down-v:
	$(COMPOSE) down -v

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: ps
ps:
	$(COMPOSE) ps

# =============================================================================
# Atajos por servicio
# =============================================================================
.PHONY: beeline
beeline:
	docker exec -it ra_hive_server beeline -u jdbc:hive2://localhost:10000

.PHONY: neo4j-shell
neo4j-shell:
	docker exec -it ra_neo4j cypher-shell -u neo4j -p $$(grep NEO4J_PASSWORD .env | cut -d= -f2)

.PHONY: spark-shell
spark-shell:
	docker exec -it ra_spark_master /opt/spark/bin/spark-shell \
		--master spark://spark-master:7077

.PHONY: spark-job-sample
spark-job-sample:
	docker exec -it ra_spark_master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark-apps/jobs/restaurants_spark_analytics.py \
		--source sample \
		--output-base /tmp/restaurants-output \
		--neo4j-output /opt/neo4j-import \
		--couriers 2

.PHONY: neo4j-load
neo4j-load:
	docker exec -i ra_neo4j cypher-shell -u neo4j -p $$(grep NEO4J_PASSWORD .env | cut -d= -f2) < neo4j/queries/00_constraints.cypher
	docker exec -i ra_neo4j cypher-shell -u neo4j -p $$(grep NEO4J_PASSWORD .env | cut -d= -f2) < neo4j/queries/01_load_graph.cypher

.PHONY: neo4j-analysis
neo4j-analysis:
	docker exec -i ra_neo4j cypher-shell -u neo4j -p $$(grep NEO4J_PASSWORD .env | cut -d= -f2) < neo4j/queries/02_analysis_queries.cypher
