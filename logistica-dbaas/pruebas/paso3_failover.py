"""
PASO 3 - Caída del primario y promoción de la réplica (failover manual).

Antes de correrlo, TUMBAR el primario desde PowerShell / terminal:
    docker stop logistica-db-primario
Luego:
    docker compose exec jupyter python pruebas/paso3_failover.py

Qué hace (lo mismo que RDS Multi-AZ / Cloud SQL HA hacen solos en ~60-120 s):
  1. Confirma que el primario no responde.
  2. Verifica que la réplica aplicó todo lo que recibió (RPO).
  3. Promueve la réplica: deja de replicar y acepta escrituras.
  4. La aplicación registra un envío y un evento en el nuevo primario.
  5. Compara con la foto tomada en el paso 1 (evidencias/pre_failover.json).
Resultados en evidencias/paso3_failover.txt
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import pymysql  # noqa: E402
import db  # noqa: E402

EVID = os.path.join(os.path.dirname(__file__), "..", "evidencias")
_log = open(os.path.join(EVID, "paso3_failover.txt"), "w", encoding="utf-8")


def p(*a):
    t = " ".join(str(x) for x in a)
    print(t)
    _log.write(t + "\n")
    _log.flush()


p(f"Fecha: {datetime.now().isoformat(timespec='seconds')}")
T0 = time.perf_counter()

# 1) ¿Está caído el primario?
p("\n1) Probando conexión al primario (db-primario)...")
try:
    db.conexion_admin(db.HOST_PRIMARIO, timeout=3).close()
    p("   El primario SIGUE ARRIBA. Primero ejecute: docker stop logistica-db-primario")
    sys.exit(1)
except pymysql.err.OperationalError as e:
    p(f"   Primario NO responde (esperado): {e.args}")
t_deteccion = time.perf_counter()

# 2) Estado de la réplica
rep = db.conexion_admin(db.HOST_REPLICA)
estado = db.consultar(rep, "SHOW REPLICA STATUS")[0]
p("\n2) Estado de la réplica al detectar la caída:")
for k in ["Replica_IO_Running", "Replica_SQL_Running", "Last_IO_Error",
          "Retrieved_Gtid_Set", "Executed_Gtid_Set"]:
    p(f"   {k}: {estado.get(k)}")
recibido = (estado.get("Retrieved_Gtid_Set") or "").replace("\n", "")
if recibido:
    r = db.consultar(rep, "SELECT WAIT_FOR_EXECUTED_GTID_SET(%s, 30) AS r", (recibido,))[0]["r"]
    p(f"   ¿Aplicó todo lo recibido? {'SÍ' if r == 0 else 'NO (timeout)'}")

foto = {}
ruta_foto = os.path.join(EVID, "pre_failover.json")
if os.path.exists(ruta_foto):
    with open(ruta_foto, encoding="utf-8") as f:
        foto = json.load(f)
    gtid_viejo = foto.get("gtid_primario", "").replace("\n", "")
    falta = db.consultar(rep, "SELECT GTID_SUBTRACT(%s, @@GLOBAL.gtid_executed) AS f", (gtid_viejo,))[0]["f"]
    p(f"   Transacciones del primario (foto del paso 1) que NO están en la réplica: "
      f"{falta if falta else 'ninguna -> RPO = 0 para lo confirmado hasta la foto'}")

# 3) Promoción
p("\n3) Promoviendo la réplica a primario...")
t_promo = time.perf_counter()
with rep.cursor() as c:
    c.execute("STOP REPLICA")
    c.execute("RESET REPLICA ALL")
    c.execute("SET PERSIST super_read_only = OFF")
    c.execute("SET PERSIST read_only = OFF")
rep.commit()
p(f"   Promoción en {(time.perf_counter() - t_promo) * 1000:.0f} ms")

# 4) La aplicación escribe en el nuevo primario
p("\n4) La aplicación escribe en el NUEVO primario (db-replica)...")
db.HOST_PRIMARIO = db.HOST_REPLICA   # en DBaaS el endpoint/DNS cambia solo
app = db.conexion_app()
cli = db.consultar(app, "SELECT cliente_id FROM clientes LIMIT 1")[0]["cliente_id"]
centros = [r["centro_id"] for r in db.consultar(app, "SELECT centro_id FROM centros_distribucion LIMIT 2")]
guia = f"FO{uuid.uuid4().hex[:10]}".upper()
envio_id = db.crear_envio(app, guia, cli, centros[0], centros[1])
llave = f"failover-{uuid.uuid4()}"
r1 = db.registrar_evento(app, envio_id, "recogido", centros[0], llave)
r2 = db.registrar_evento(app, envio_id, "recogido", centros[0], llave)  # reintento del cliente
T_FIN = time.perf_counter()
p(f"   Envío {guia} creado. Evento: {r1}. Reintento con misma llave: {r2}")

# 5) Comparación
p("\n5) Conteos en el nuevo primario vs foto del paso 1:")
for t in ["envios", "eventos_envio", "eventos_procesados", "clientes"]:
    ahora = db.consultar(rep, f"SELECT COUNT(*) n FROM {t}")[0]["n"]
    antes = foto.get(t, {}).get("primario", "¿?")
    p(f"   {t:20s} antes(primario)={antes:>8}  ahora(nuevo primario)={ahora:>8}")

p("\nRESUMEN")
p(f"   Detección de la caída: {(t_deteccion - T0):.1f} s (timeout de conexión de 3 s)")
p(f"   Desde la detección hasta la primera escritura OK: {(T_FIN - t_deteccion):.2f} s")
p("   Nota: en el prototipo la decisión de promover la toma una persona/script;")
p("   en RDS Multi-AZ o Cloud SQL HA el proveedor la automatiza (60-120 s documentados).")
p("   Limitación: esta réplica es ASÍNCRONA; transacciones no enviadas antes de la caída")
p("   podrían perderse. RDS Multi-AZ / Cloud SQL HA usan copia SÍNCRONA (RPO≈0).")
p("\nPara volver a empezar desde cero:  docker compose down -v  y  docker compose up -d --build")
rep.close()
app.close()
_log.close()
