#!/bin/bash
set -euo pipefail

: "${API_DB_USER:?API_DB_USER not set}"
: "${API_DB_PASSWORD:?API_DB_PASSWORD not set}"

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v u="$API_DB_USER" \
     -v p="$API_DB_PASSWORD" \
     -v db="$POSTGRES_DB" \
     -v owner="$POSTGRES_USER" <<'EOSQL'
-- create the role only if missing; \gexec runs the generated statement
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'u', :'p')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'u');
\gexec

GRANT CONNECT ON DATABASE :"db" TO :"u";
GRANT USAGE ON SCHEMA public TO :"u";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"u";
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO :"u";

ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public
  GRANT SELECT ON TABLES TO :"u";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public
  GRANT SELECT ON SEQUENCES TO :"u";
EOSQL