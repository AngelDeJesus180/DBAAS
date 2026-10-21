"""
PASO 1 - Pruebas funcionales, de carga, concurrencia, integridad y replicación.

Ejecutar (con los contenedores arriba):
    docker compose exec jupyter python pruebas/paso1_pruebas.py

Resultados: se imprimen en pantalla y se guardan en
    evidencias/paso1_resultados.txt   (texto para el informe)
    evidencias/paso1_resultados.json  (números)
    evidencias/pre_failover.json      (conteos antes de tumbar el primario)
Variables opcionales: CARGA_ENVIOS (por defecto 2000), CARGA_HILOS (por defecto 8)
"""
import json
import os
import random
import statistics
import sys
import threading
import time
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import pymysql  # noqa: E402
from db import (actualizar_con_version, conexion_admin, conexion_app,  # noqa: E402
                conexion_reportes, consultar, crear_envio, registrar_evento)

RAIZ = os.path.join(os.path.dirname(__file__), "..")
EVID = os.path.join(RAIZ, "evidencias")
os.makedirs(EVID, exist_ok=True)
N_ENVIOS = int(os.getenv("CARGA_ENVIOS", "2000"))
N_HILOS = int(os.getenv("CARGA_HILOS", "8"))
CICLO = ["recogido", "en_transito", "en_centro", "en_reparto", "entregado"]

random.seed(42)
_log = open(os.path.join(EVID, "paso1_resultados.txt"), "w", encoding="utf-8")
resultados = {"fecha_ejecucion": datetime.now().isoformat(timespec="seconds")}
aprobadas, fallidas = [], []


def p(*args):
    texto = " ".join(str(a) for a in args)
    print(texto)
    _log.write(texto + "\n")
    _log.flush()


def titulo(t):
    p("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70)


def verificar(nombre, condicion, detalle=""):
    (aprobadas if condicion else fallidas).append(nombre)
    p(f"  [{'PASA' if condicion else 'FALLA'}] {nombre} {detalle}")


def percentiles(lista_ms):
    if len(lista_ms) < 2:
        return {"p50": lista_ms[0] if lista_ms else None, "p95": None, "p99": None, "max": None}
    q = statistics.quantiles(lista_ms, n=100)
    return {"p50": round(q[49], 2), "p95": round(q[94], 2), "p99": round(q[98], 2),
            "max": round(max(lista_ms), 2)}


def esperar_en_replica(sql, params, timeout=10):
    """Repite una consulta en la réplica hasta que devuelve filas. Devuelve ms o None."""
    rep = conexion_reportes()
    t0 = time.perf_counter()
    try:
        while time.perf_counter() - t0 < timeout:
            if consultar(rep, sql, params):
                return (time.perf_counter() - t0) * 1000
            time.sleep(0.005)
        return None
    finally:
        rep.close()


def envio_nuevo(conn, prefijo):
    o, d = random.sample(CENTROS, 2)
    return crear_envio(conn, f"{prefijo}{uuid.uuid4().hex[:10]}".upper()[:20],
                       random.choice(CLIENTES), o, d)


def lanzar_a_la_vez(n, funcion):
    """Lanza n hilos que arrancan EXACTAMENTE al mismo tiempo (Barrier)."""
    barrera = threading.Barrier(n)
    salida = [None] * n

    def trabajo(i):
        conn = conexion_app()
        try:
            barrera.wait()
            salida[i] = funcion(conn, i)
        except Exception as e:  # se registra como resultado
            salida[i] = f"ERROR:{e}"
        finally:
            conn.close()

    hilos = [threading.Thread(target=trabajo, args=(i,)) for i in range(n)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    return salida


# ======================================================================
titulo("0) PREPARACIÓN: limpiar y cargar datos sintéticos")
adm = conexion_admin()
with adm.cursor() as c:
    c.execute("SELECT VERSION() v")
    p("  MySQL primario versión:", c.fetchone()["v"])
    c.execute("TRUNCATE TABLE eventos_envio")
    c.execute("TRUNCATE TABLE eventos_procesados")
    c.execute("DELETE FROM envios")
    c.execute("DELETE FROM clientes WHERE documento LIKE 'SYN%'")
    clientes = [(f"SYN{i:06d}", f"Cliente Sintético {i}", f"cliente{i}@sintetico.test")
                for i in range(1, 201)]
    c.executemany("INSERT INTO clientes (documento, nombre, email) VALUES (%s,%s,%s)", clientes)
adm.commit()
CLIENTES = [r["cliente_id"] for r in consultar(adm, "SELECT cliente_id FROM clientes")]
CENTROS = [r["centro_id"] for r in consultar(adm, "SELECT centro_id FROM centros_distribucion")]

filas = []
for i in range(1, N_ENVIOS + 1):
    o, d = random.sample(CENTROS, 2)
    filas.append((f"LG{i:010d}", random.choice(CLIENTES), o, d))
with adm.cursor() as c:
    c.executemany("""INSERT INTO envios (guia, cliente_id, centro_origen_id, centro_destino_id)
                     VALUES (%s,%s,%s,%s)""", filas)
adm.commit()
ENVIOS = [(r["envio_id"], r["centro_origen_id"], r["centro_destino_id"]) for r in consultar(
    adm, "SELECT envio_id, centro_origen_id, centro_destino_id FROM envios ORDER BY envio_id")]
p(f"  Clientes: {len(CLIENTES)} | Centros: {len(CENTROS)} | Envíos creados: {len(ENVIOS)}")

# ======================================================================
titulo(f"1) CARGA: {len(ENVIOS)} envíos x {len(CICLO)} eventos con {N_HILOS} hilos")
latencias, conteo, lock = [], {}, threading.Lock()


def trabajador(lote):
    conn = conexion_app()
    mis_lat, mis_res = [], {}
    try:
        for envio_id, origen, destino in lote:
            for estado in CICLO:
                centro = destino if estado in ("en_centro", "en_reparto", "entregado") else origen
                t = time.perf_counter()
                r = registrar_evento(conn, envio_id, estado, centro)
                mis_lat.append((time.perf_counter() - t) * 1000)
                mis_res[r] = mis_res.get(r, 0) + 1
    finally:
        conn.close()
    with lock:
        latencias.extend(mis_lat)
        for k, v in mis_res.items():
            conteo[k] = conteo.get(k, 0) + v


lotes = [ENVIOS[i::N_HILOS] for i in range(N_HILOS)]
t0 = time.perf_counter()
hilos = [threading.Thread(target=trabajador, args=(lote,)) for lote in lotes]
for h in hilos:
    h.start()
for h in hilos:
    h.join()
duracion = time.perf_counter() - t0
total_ev = sum(conteo.values())
carga = {"eventos": total_ev, "segundos": round(duracion, 2),
         "eventos_por_segundo": round(total_ev / duracion, 1),
         "latencia_ms": percentiles(latencias), "resultados": conteo, "hilos": N_HILOS}
resultados["carga"] = carga
p(f"  Eventos: {total_ev} en {duracion:.2f} s -> {carga['eventos_por_segundo']} eventos/s")
p(f"  Latencia por evento (ms): {carga['latencia_ms']}")
p(f"  Resultados: {conteo}")
p("  Referencia del caso: 10 M/día = 116 eventos/s promedio; pico supuesto x10 = 1.160 eventos/s")
verificar("Todos los eventos de la carga quedaron OK", conteo.get("OK", 0) == total_ev)
fila = consultar(adm, """SELECT (SELECT COUNT(*) FROM eventos_envio) ev,
                                (SELECT COUNT(*) FROM envios WHERE estado_actual='entregado') ent""")[0]
verificar("Historial y estado actual coinciden",
          fila["ev"] == total_ev and fila["ent"] == len(ENVIOS),
          f"(eventos={fila['ev']}, envíos entregados={fila['ent']})")

# ======================================================================
titulo("2) IDEMPOTENCIA CONCURRENTE: 20 hilos envían EL MISMO evento a la vez")
app = conexion_app()
e_id = envio_nuevo(app, "IDEM")
llave = f"scan-{uuid.uuid4()}"
res = lanzar_a_la_vez(20, lambda conn, i: registrar_evento(conn, e_id, "recogido", CENTROS[0], llave))
p("  Respuestas:", {r: res.count(r) for r in set(res)})
n_ev = consultar(adm, "SELECT COUNT(*) n FROM eventos_envio WHERE idempotencia_key=%s", (llave,))[0]["n"]
verificar("Solo 1 OK y 19 DUPLICADO", res.count("OK") == 1 and res.count("DUPLICADO") == 19)
verificar("En la base quedó exactamente 1 evento con esa llave", n_ev == 1, f"(filas={n_ev})")
resultados["idempotencia"] = {r: res.count(r) for r in set(res)}

# ======================================================================
titulo("3) CONCURRENCIA DE ESTADO: 20 hilos intentan marcar 'entregado' a la vez")
e_id = envio_nuevo(app, "CONC")
for est in ["recogido", "en_transito", "en_centro", "en_reparto"]:
    registrar_evento(app, e_id, est, CENTROS[0])
res = lanzar_a_la_vez(20, lambda conn, i: registrar_evento(conn, e_id, "entregado", CENTROS[1]))
p("  Respuestas:", {r: res.count(r) for r in set(res)})
fila = consultar(adm, "SELECT estado_actual, version FROM envios WHERE envio_id=%s", (e_id,))[0]
n_ent = consultar(adm, """SELECT COUNT(*) n FROM eventos_envio
                          WHERE envio_id=%s AND estado_nuevo='entregado'""", (e_id,))[0]["n"]
verificar("Solo 1 hilo logra entregar; los demás reciben TRANSICION_INVALIDA",
          res.count("OK") == 1 and res.count("TRANSICION_INVALIDA") == 19)
verificar("Un solo evento 'entregado' y version=5", n_ent == 1 and fila["version"] == 5,
          f"(eventos entregado={n_ent}, version={fila['version']})")
resultados["concurrencia_estado"] = {r: res.count(r) for r in set(res)}

# ======================================================================
titulo("4) CONCURRENCIA OPTIMISTA: 10 hilos leen la misma versión y actualizan")
e_id = envio_nuevo(app, "OPT")
base = consultar(app, "SELECT version, centro_origen_id FROM envios WHERE envio_id=%s", (e_id,))[0]
destinos = [c for c in CENTROS if c != base["centro_origen_id"]]
res = lanzar_a_la_vez(10, lambda conn, i: actualizar_con_version(
    conn, e_id, base["version"], destinos[i % len(destinos)]))
p("  Respuestas (True=actualizó, False=conflicto):", {str(r): res.count(r) for r in set(res)})
verificar("Exactamente 1 actualización gana; 9 detectan conflicto",
          res.count(True) == 1 and res.count(False) == 9)

# ======================================================================
titulo("5) ATOMICIDAD: falla a mitad de la transacción")
e_id = envio_nuevo(app, "ATOM")
llave = f"scan-{uuid.uuid4()}"
try:
    registrar_evento(app, e_id, "recogido", CENTROS[0], llave, fallar_a_mitad=True)
    hubo_error = False
except RuntimeError as e:
    hubo_error = True
    p("  Error lanzado:", e)
fila = consultar(adm, "SELECT estado_actual, version FROM envios WHERE envio_id=%s", (e_id,))[0]
n1 = consultar(adm, "SELECT COUNT(*) n FROM eventos_envio WHERE idempotencia_key=%s", (llave,))[0]["n"]
n2 = consultar(adm, "SELECT COUNT(*) n FROM eventos_procesados WHERE idempotencia_key=%s", (llave,))[0]["n"]
verificar("No quedó nada a medias (sin evento, sin llave, estado sin cambio)",
          hubo_error and n1 == 0 and n2 == 0 and fila["estado_actual"] == "creado",
          f"(eventos={n1}, llaves={n2}, estado={fila['estado_actual']})")
r = registrar_evento(app, e_id, "recogido", CENTROS[0], llave)
verificar("Al reintentar con la misma llave sí se registra", r == "OK", f"(respuesta={r})")

# ======================================================================
titulo("6) REGLAS DE INTEGRIDAD")
e_id = envio_nuevo(app, "REG")
verificar("Transición inválida creado->entregado rechazada",
          registrar_evento(app, e_id, "entregado", CENTROS[0]) == "TRANSICION_INVALIDA")
verificar("Evento para envío inexistente rechazado",
          registrar_evento(app, 999999999, "recogido", CENTROS[0]) == "NO_EXISTE")


def espera_error(nombre, sql, params, codigo, conn=app):
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        verificar(nombre, False, "(¡se permitió!)")
    except pymysql.err.MySQLError as e:
        conn.rollback()
        verificar(nombre, e.args[0] == codigo, f"(error {e.args[0]})")


espera_error("CHECK: origen = destino rechazado",
             "INSERT INTO envios (guia, cliente_id, centro_origen_id, centro_destino_id) VALUES (%s,%s,%s,%s)",
             ("REGCHK1", CLIENTES[0], CENTROS[0], CENTROS[0]), 3819)
espera_error("FK: cliente inexistente rechazado",
             "INSERT INTO envios (guia, cliente_id, centro_origen_id, centro_destino_id) VALUES (%s,%s,%s,%s)",
             ("REGFK1", 99999999, CENTROS[0], CENTROS[1]), 1452)
espera_error("UNIQUE: guía duplicada rechazada",
             "INSERT INTO envios (guia, cliente_id, centro_origen_id, centro_destino_id) VALUES (%s,%s,%s,%s)",
             ("LG0000000001", CLIENTES[0], CENTROS[0], CENTROS[1]), 1062)
espera_error("FK: estado inexistente rechazado",
             "UPDATE envios SET estado_actual='perdido_en_el_mar' WHERE envio_id=%s", (e_id,), 1452)
espera_error("Permisos mínimos: la app NO puede borrar",
             "DELETE FROM envios WHERE envio_id=%s", (e_id,), 1142)
espera_error("Permisos mínimos: la app NO puede borrar tablas",
             "DROP TABLE eventos_procesados", (), 1142)

# ======================================================================
titulo("7) CONSULTAS CRÍTICAS (tiempos y plan de ejecución)")
guia = consultar(adm, "SELECT guia FROM envios WHERE guia LIKE 'LG%%' ORDER BY RAND() LIMIT 1")[0]["guia"]
hoy = datetime.now().strftime("%Y-%m-%d")
consultas = [
    ("Q1 Rastreo por guía (primario)", app,
     "SELECT envio_id, estado_actual, fecha_actualizacion FROM envios WHERE guia=%s", (guia,)),
    ("Q2 Historial de un envío (réplica)", None,
     """SELECT e.fecha_evento, e.estado_nuevo, c.nombre FROM eventos_envio e
        JOIN envios v ON v.envio_id=e.envio_id JOIN centros_distribucion c ON c.centro_id=e.centro_id
        WHERE v.guia=%s ORDER BY e.fecha_evento""", (guia,)),
    ("Q3 Envíos en reparto por centro destino (réplica)", None,
     """SELECT centro_destino_id, COUNT(*) n FROM envios
        WHERE estado_actual='en_reparto' GROUP BY centro_destino_id""", ()),
    ("Q4 Eventos por centro en un día (réplica, usa partición)", None,
     """SELECT centro_id, estado_nuevo, COUNT(*) n FROM eventos_envio
        WHERE fecha_evento >= %s AND fecha_evento < %s + INTERVAL 1 DAY
        GROUP BY centro_id, estado_nuevo""", (hoy, hoy)),
]
time.sleep(1)  # dar tiempo a que la réplica aplique la carga
rep = conexion_reportes()
resultados["consultas"] = {}
for nombre, conn, sql, params in consultas:
    conn = conn or rep
    tiempos = []
    for _ in range(20):
        t = time.perf_counter()
        filas_q = consultar(conn, sql, params)
        tiempos.append((time.perf_counter() - t) * 1000)
    plan = consultar(conn, "EXPLAIN " + sql, params)
    resumen_plan = "; ".join(f"{x['table']}:{x['type']}/{x.get('key')}"
                             + (f"/part={x['partitions']}" if x.get("partitions") else "")
                             for x in plan)
    m = percentiles(tiempos)
    resultados["consultas"][nombre] = {"filas": len(filas_q), **m, "plan": resumen_plan}
    p(f"  {nombre}: filas={len(filas_q)} p50={m['p50']} ms p95={m['p95']} ms")
    p(f"      plan: {resumen_plan}")

# ======================================================================
titulo("8) REPLICACIÓN: retraso, solo lectura y consistencia")
retrasos = []
for i in range(20):
    g = f"LAG{uuid.uuid4().hex[:12]}".upper()
    o, d = random.sample(CENTROS, 2)
    crear_envio(app, g, CLIENTES[0], o, d)
    ms = esperar_en_replica("SELECT 1 FROM envios WHERE guia=%s", (g,))
    if ms is not None:
        retrasos.append(ms)
m = percentiles(retrasos)
resultados["replicacion_retraso_ms"] = m
p(f"  Retraso primario->réplica (ms), 20 muestras: {m}")
verificar("Las 20 escrituras llegaron a la réplica", len(retrasos) == 20)

adm_rep = conexion_admin(os.getenv("DB_REPLICA_HOST", "localhost"))
espera_error("La réplica rechaza escrituras (super_read_only), incluso de root",
             "INSERT INTO estados_envio (codigo, orden) VALUES ('x', 99)", (), 1290, conn=adm_rep)
estado_rep = consultar(adm_rep, "SHOW REPLICA STATUS")[0]
p(f"  Replica_IO_Running={estado_rep['Replica_IO_Running']} "
  f"Replica_SQL_Running={estado_rep['Replica_SQL_Running']} "
  f"Seconds_Behind_Source={estado_rep['Seconds_Behind_Source']}")
time.sleep(1)
tablas = ["envios", "eventos_envio", "eventos_procesados", "clientes"]
iguales = True
for t in tablas:
    a = consultar(adm, f"SELECT COUNT(*) n FROM {t}")[0]["n"]
    b = consultar(adm_rep, f"SELECT COUNT(*) n FROM {t}")[0]["n"]
    iguales &= a == b
    p(f"  {t:20s} primario={a:>8} réplica={b:>8}")
verificar("Primario y réplica tienen los mismos conteos", iguales)

# ======================================================================
titulo("9) RETENCIÓN POR PARTICIONES (crecimiento del 40 % anual)")
p("  Se insertan 5.000 eventos 'históricos' de agosto 2026 y se purga ese mes completo.")
viejos = [(ENVIOS[i % len(ENVIOS)][0], "en_transito", "en_centro", CENTROS[0],
           f"2026-08-{(i % 28) + 1:02d} 10:00:00", f"hist-{i}") for i in range(5000)]
with adm.cursor() as c:
    c.executemany("""INSERT INTO eventos_envio (envio_id, estado_anterior, estado_nuevo, centro_id,
                     fecha_evento, idempotencia_key) VALUES (%s,%s,%s,%s,%s,%s)""", viejos)
adm.commit()
antes = consultar(adm, "SELECT COUNT(*) n FROM eventos_envio PARTITION (p2026_08)")[0]["n"]
t = time.perf_counter()
with adm.cursor() as c:
    c.execute("ALTER TABLE eventos_envio TRUNCATE PARTITION p2026_08")
ms = (time.perf_counter() - t) * 1000
despues = consultar(adm, "SELECT COUNT(*) n FROM eventos_envio PARTITION (p2026_08)")[0]["n"]
p(f"  Eventos de agosto antes={antes}, después={despues}, tiempo={ms:.1f} ms")
verificar("Purga de un mes completo sin DELETE fila a fila", antes == 5000 and despues == 0)
resultados["retencion"] = {"filas_purgadas": antes, "ms": round(ms, 1)}

# ======================================================================
titulo("10) FOTO ANTES DEL FALLO (para comparar en el paso 3)")
time.sleep(1)
foto = {"fecha": datetime.now().isoformat(timespec="seconds")}
for t in tablas:
    foto[t] = {"primario": consultar(adm, f"SELECT COUNT(*) n FROM {t}")[0]["n"],
               "replica": consultar(adm_rep, f"SELECT COUNT(*) n FROM {t}")[0]["n"]}
foto["gtid_primario"] = consultar(adm, "SELECT @@GLOBAL.gtid_executed g")[0]["g"]
with open(os.path.join(EVID, "pre_failover.json"), "w", encoding="utf-8") as f:
    json.dump(foto, f, indent=2, ensure_ascii=False)
p(json.dumps(foto, indent=2, ensure_ascii=False))

# ======================================================================
titulo("RESUMEN")
p(f"  Pruebas que pasan: {len(aprobadas)} | que fallan: {len(fallidas)}")
for x in fallidas:
    p("   - FALLA:", x)
resultados["pasan"], resultados["fallan"] = aprobadas, fallidas
with open(os.path.join(EVID, "paso1_resultados.json"), "w", encoding="utf-8") as f:
    json.dump(resultados, f, indent=2, ensure_ascii=False, default=str)
for c in (adm, adm_rep, app, rep):
    c.close()
_log.close()
