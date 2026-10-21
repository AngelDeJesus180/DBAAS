#!/bin/bash
# Conecta db-replica al db-primario usando replicación con GTID.
# En un DBaaS esto es "un clic" (crear réplica de lectura / activar HA).
set -e
export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"

echo "Esperando a que el primario y la réplica acepten conexiones..."
for i in $(seq 1 60); do
  if mysql -h db-primario -uroot -e "SELECT 1 FROM logistica.envios LIMIT 1" >/dev/null 2>&1 \
     && mysql -h db-replica -uroot -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Si la réplica ya está replicando bien, no hacemos nada (permite repetir 'up').
ESTADO=$(mysql -h db-replica -uroot -N -e "SHOW REPLICA STATUS\G" | grep -E "Replica_IO_Running:|Replica_SQL_Running:" | awk '{print $2}' | tr '\n' ' ')
if [ "$ESTADO" = "Yes Yes " ]; then
  echo "La réplica ya estaba replicando. Nada que hacer."
  exit 0
fi

echo "Configurando la réplica..."
mysql -h db-replica -uroot -e "STOP REPLICA" >/dev/null 2>&1 || true
mysql -h db-replica -uroot <<SQL
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='db-primario',
  SOURCE_PORT=3306,
  SOURCE_USER='repl',
  SOURCE_PASSWORD='${REPL_PASSWORD}',
  SOURCE_AUTO_POSITION=1,
  GET_SOURCE_PUBLIC_KEY=1,
  SOURCE_CONNECT_RETRY=5;
START REPLICA;
SET PERSIST read_only = ON;
SET PERSIST super_read_only = ON;
SQL

# Esperar a que la réplica alcance al primario
for i in $(seq 1 30); do
  if mysql -h db-replica -uroot -N -e "SELECT COUNT(*) FROM logistica.estados_envio" 2>/dev/null | grep -q "8"; then
    echo "OK: la réplica ya tiene el esquema y los datos del primario."
    mysql -h db-replica -uroot -e "SHOW REPLICA STATUS\G" | grep -E "Replica_IO_Running:|Replica_SQL_Running:|Seconds_Behind_Source:|Last_.*Error:"
    exit 0
  fi
  sleep 2
done

echo "ERROR: la réplica no alcanzó al primario. Estado:"
mysql -h db-replica -uroot -e "SHOW REPLICA STATUS\G"
exit 1
