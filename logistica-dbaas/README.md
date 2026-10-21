# Prototipo DBaaS: empresa de logística

Prototipo académico del **caso 9 (Cloud / DBaaS)**. Modela envíos, clientes, centros de distribución y estados de entrega en **MySQL 8.0**, el mismo motor que ofrecen como servicio administrado **Amazon RDS for MySQL** y **Google Cloud SQL for MySQL**.

Como no usamos una cuenta de nube, el prototipo **simula con Docker** lo que el proveedor DBaaS haría por nosotros:

| En el prototipo (Docker) | En DBaaS real |
|---|---|
| `db-primario` (recibe escrituras) | Instancia primaria en la zona A |
| `db-replica` (replicación GTID, solo lectura) | Réplica de lectura o standby en la zona B |
| `replica-setup` (conecta la réplica) | Botón "crear réplica" o "activar HA" |
| `paso2_backup_restore.sh` | Backups automáticos, snapshots y PITR |
| `paso3_failover.py` (promoción manual) | Failover automático (60–120 s según la documentación) |

> ⚠️ Todo corre en **una sola máquina**: el prototipo demuestra replicación, caída de un nodo y restauración, pero **no tolera la pérdida del PC**.

## Requisitos
- **Docker Desktop** (Windows/Mac) o Docker Engine con Compose v2. Nada más: Python y MySQL van dentro de los contenedores.
- Puertos libres: 3307, 3308 y 8888.

## 1. Levantar el entorno
En PowerShell, dentro de esta carpeta:

```powershell
docker compose up -d --build
docker compose ps          # db-primario y db-replica deben estar "healthy"
docker compose logs replica-setup   # debe decir: OK: la réplica ya tiene el esquema...
```

- **Jupyter:** http://localhost:8888 (token: `logistica`), notebook `notebooks/pruebas.ipynb`.
- Para conectarse con MySQL Workbench o DBeaver:
  - **Primario:** `localhost:3307`
  - **Réplica:** `localhost:3308`
  - **Usuario de la app:** `app_logistica` / `AppDemo#2026`

Las contraseñas son **solo de demostración**. Para cambiarlas, copie `.env.example` como `.env` antes del primer `up`.

## 2. Ejecutar las pruebas (en este orden)

```powershell
# PASO 1: carga, concurrencia, idempotencia, atomicidad, reglas, consultas, replicación, retención
docker compose exec jupyter python pruebas/paso1_pruebas.py

# PASO 2: respaldo y restauración con validación de checksums
docker compose exec db-primario bash /scripts/paso2_backup_restore.sh | Tee-Object evidencias/paso2_backup_restore.txt

# PASO 3: caída del primario y promoción de la réplica
docker stop logistica-db-primario
docker compose exec jupyter python pruebas/paso3_failover.py
```

Los resultados quedan en la carpeta `evidencias/`: `paso1_resultados.txt`, `paso1_resultados.json`, `pre_failover.json`, `paso2_backup_restore.txt` y `paso3_failover.txt`. **Tome capturas de pantalla** de cada paso para el informe.

Opcional: para cambiar el tamaño de la carga, use `docker compose exec -e CARGA_ENVIOS=5000 -e CARGA_HILOS=16 jupyter python pruebas/paso1_pruebas.py`.

## 2B. Failover AUTOMÁTICO (opcional, reemplaza el paso 3)
Un servicio `vigilante` revisa el primario cada segundo. Si falla **3 veces seguidas**, promueve la réplica él solo y anota el nuevo primario en `estado/primario_actual.txt`. La aplicación (`src/db.py → conexion_app`) siempre se conecta al nodo que acepta escrituras: es nuestra versión del endpoint de RDS, que cambia solo.

```powershell
docker compose down -v                       # empezar limpio
docker compose --profile auto up -d --build  # levanta también el vigilante
docker compose exec jupyter python pruebas/paso1_pruebas.py
docker compose exec db-primario bash /scripts/paso2_backup_restore.sh

# Ventana 1: escribe eventos sin parar durante 90 s
docker compose exec jupyter python pruebas/paso3b_failover_automatico.py
# Ventana 2: cuando la ventana 1 lo pida, simular una caída abrupta
docker kill logistica-db-primario

docker compose logs vigilante                # detección y promoción con hora exacta
```

El script mide cuántos segundos no se pudo escribir y verifica que ningún evento confirmado se perdió ni se duplicó. Lo logra reintentando con la misma llave de idempotencia.

**Prueba de "split-brain":** `docker start logistica-db-primario`. El vigilante detecta que el primario viejo volvió y lo deja en solo lectura (*fencing*), para que no haya dos primarios. Reincorporarlo como réplica es manual; lo más simple es volver a empezar desde cero.

> Limitación: es un vigilante sencillo, no un sistema de consenso. Si el propio vigilante se cae, no hay failover, y si la red se corta solo entre el vigilante y el primario, podría promover sin necesidad. Los servicios DBaaS (RDS Multi-AZ, Cloud SQL HA) y MySQL InnoDB Cluster (Group Replication con 3 nodos + MySQL Router) resuelven esto con monitoreo redundante, consenso y copia síncrona.

## 3. Volver a empezar desde cero
```powershell
docker compose down -v      # borra contenedores Y datos
docker compose up -d --build
```

## Estructura
```
docker-compose.yml        primario + réplica + configurador + jupyter
Dockerfile                imagen de la app (Python 3.11 + Jupyter)
sql/primario/             01_esquema.sql, 02_datos_base.sql, 03_usuarios.sql
scripts/                  configurar_replica.sh, paso2_backup_restore.sh
src/db.py                 conexiones y la transacción registrar_evento()
pruebas/                  paso1_pruebas.py, paso3_failover.py
notebooks/pruebas.ipynb   lo mismo, para mostrar en clase
evidencias/               salidas de las pruebas
backups/                  respaldos generados
```

## Problemas comunes
| Síntoma | Solución |
|---|---|
| `port is already allocated` | Otro MySQL usa el 3307/3308. Detenga ese servicio o cambie el puerto en `docker-compose.yml`. |
| `replica-setup` termina con ERROR | Ejecute `docker compose down -v` y luego `docker compose up -d --build`. |
| `paso3` dice "El primario SIGUE ARRIBA" | Primero ejecute `docker stop logistica-db-primario`. |
| Quiere repetir el paso 1 después del paso 3 | Reinicie desde cero (sección 3). |
| `bash\r: No such file` | Los `.sh` quedaron con fin de línea de Windows. En VS Code cambie CRLF a LF. |
