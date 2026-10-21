#!/bin/bash
# =====================================================================
# PASO 2: Respaldo y restauración (se ejecuta DENTRO del contenedor primario)
#   docker compose exec db-primario bash /scripts/paso2_backup_restore.sh
#
# 1. Saca un respaldo lógico consistente (mysqldump --single-transaction).
# 2. Lo restaura en una base NUEVA (logistica_restaurada).
# 3. Compara filas y CHECKSUM de cada tabla entre original y restaurada.
# 4. Mide los tiempos (insumo para el RTO del prototipo).
# En DBaaS esto equivale a: snapshot automático + "restaurar a una
# instancia nueva" o PITR (restaurar a un minuto exacto).
# =====================================================================
set -e
export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"
ARCHIVO="/backups/logistica_$(date +%Y%m%d_%H%M%S).sql"
TABLAS="clientes centros_distribucion estados_envio transiciones_validas envios eventos_envio eventos_procesados"

ms() { date +%s%3N; }

echo "== 1) Respaldo =="
T0=$(ms)
mysqldump -uroot --single-transaction --routines --triggers \
  --set-gtid-purged=OFF --no-tablespaces logistica > "$ARCHIVO"
T1=$(ms)
echo "Archivo: $ARCHIVO  ($(du -h "$ARCHIVO" | cut -f1))"
echo "Tiempo de respaldo: $((T1 - T0)) ms"

echo "== 2) Restauración en logistica_restaurada =="
mysql -uroot -e "DROP DATABASE IF EXISTS logistica_restaurada; CREATE DATABASE logistica_restaurada CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
T2=$(ms)
mysql -uroot logistica_restaurada < "$ARCHIVO"
T3=$(ms)
echo "Tiempo de restauración: $((T3 - T2)) ms"

echo "== 3) Validación de integridad (original vs restaurada) =="
ERRORES=0
printf "%-22s %12s %12s %14s %14s %s\n" TABLA FILAS_ORIG FILAS_REST CHECKSUM_ORIG CHECKSUM_REST OK
for t in $TABLAS; do
  F1=$(mysql -uroot -N -e "SELECT COUNT(*) FROM logistica.$t")
  F2=$(mysql -uroot -N -e "SELECT COUNT(*) FROM logistica_restaurada.$t")
  C1=$(mysql -uroot -N -e "CHECKSUM TABLE logistica.$t" | awk '{print $2}')
  C2=$(mysql -uroot -N -e "CHECKSUM TABLE logistica_restaurada.$t" | awk '{print $2}')
  if [ "$F1" = "$F2" ] && [ "$C1" = "$C2" ]; then OK="SI"; else OK="NO"; ERRORES=$((ERRORES+1)); fi
  printf "%-22s %12s %12s %14s %14s %s\n" "$t" "$F1" "$F2" "$C1" "$C2" "$OK"
done

echo "== Resumen =="
echo "Respaldo: $((T1 - T0)) ms | Restauración: $((T3 - T2)) ms | Tablas con diferencias: $ERRORES"
if [ "$ERRORES" -eq 0 ]; then
  echo "RESULTADO: RESTAURACIÓN VERIFICADA"
else
  echo "RESULTADO: HAY DIFERENCIAS"
  exit 1
fi
