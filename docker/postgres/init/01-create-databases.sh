#!/usr/bin/env bash
# Runs once, on first start of an empty data volume (docker-entrypoint-initdb.d).
# Creates the application role/database and the Keycloak role/database.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
    CREATE ROLE ${APP_DB_USER} LOGIN PASSWORD '${APP_DB_PASSWORD}';
    CREATE DATABASE ${APP_DB_NAME} OWNER ${APP_DB_USER};
    CREATE ROLE ${KEYCLOAK_DB_USER} LOGIN PASSWORD '${KEYCLOAK_DB_PASSWORD}';
    CREATE DATABASE ${KEYCLOAK_DB_NAME} OWNER ${KEYCLOAK_DB_USER};
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${APP_DB_NAME}" <<-SQL
    ALTER SCHEMA public OWNER TO ${APP_DB_USER};
SQL
