COMPOSE = docker compose -f deployments/docker-compose.yml

# =============================================================================
# setup: copia .env.example a .env y detecta el nombre real de la red re2.
# =============================================================================
.PHONY: setup
setup:
	@if [ ! -f .env ]; then cp .env.example .env; echo "✓ .env creado desde .env.example"; fi
	@echo "Buscando red del Proyecto 1..."
	@NETWORK=$$(docker network ls --format '{{.Name}}' | grep re2 | head -1); \
	if [ -z "$$NETWORK" ]; then \
		echo "⚠️  No se encontró la red re2. Asegurate de tener el Proyecto 1 levantado."; \
	else \
		sed -i.bak "s|^RE2_NETWORK_NAME=.*|RE2_NETWORK_NAME=$$NETWORK|" .env && rm -f .env.bak; \
		echo "✓ RE2_NETWORK_NAME=$$NETWORK actualizado en .env"; \
	fi
	@echo ""
	@echo "Recordatorio: completar AIRFLOW_FERNET_KEY y AIRFLOW_SECRET_KEY en .env"
	@echo "  Fernet key: python3 -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
	@echo "  Secret key: python3 -c \"import secrets; print(secrets.token_hex(32))\""

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
	docker exec -it ra_spark_master /opt/bitnami/spark/bin/spark-shell \
		--master spark://spark-master:7077
