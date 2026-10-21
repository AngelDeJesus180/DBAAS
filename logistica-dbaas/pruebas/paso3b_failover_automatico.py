"""
PASO 3B - Failover AUTOMÁTICO con escrituras continuas.

Requiere el vigilante:   docker compose --profile auto up -d --build
Ventana 1:   docker compose exec jupyter python pruebas/paso3b_failover_automatico.py
Ventana 2 (cuando el script lo pida):   docker kill logistica-db-primario

Qué demuestra:
  * La aplicación escribe eventos sin parar (≈20 por segundo).
  * Se mata el primario; el vigilante promueve la réplica SOLO.
  * La app reintenta con la MISMA llave de idempotencia y se reconecta sola.
  * Se mide cuánto tiempo no se pudo escribir (RTO observado).
  * Se verifica que ningún evento confirmado se perdió (RPO) y que no hay duplicados.
Resultados en evidencias/paso3b_failover_automatico.txt
"""
import os
import random
import sys
import time
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import pymysql  # noqa: E402
import db  # noqa: E402

DURACION_S = int(os.getenv("DURACION_S", "90"))
AVISO_S = 10
EVID = os.path.join(os.path.dirname(__file__), "..", "evidencias")
os.makedirs(EVID, exist_ok=True)
_log = open(os.path.join(EVID, "paso3b_failover_automatico.txt"), "w", encoding="utf-8")
CICLO = ["recogido", "en_transito", "en_centro", "en_reparto", "entregado"]


def p(*a):
    t = f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} " + " ".join(str(x) for x in a)
    print(t, flush=True)
    _log.write(t + "\n")
    _log.flush()


def conectar_con_espera(limite_s=180):
    t0 = time.perf_counter()
    while True:
        try:
            return db.conexion_app(timeout=1)
        except pymysql.err.MySQLError:
            if time.perf_counter() - t0 > limite_s:
                raise
            time.sleep(0.2)


p(f"Preparando envíos... (prueba de {DURACION_S} s)")
conn = conectar_con_espera()
p(f"Conectado al nodo de escritura: {conn.get_host_info()}")
clientes = [r["cliente_id"] for r in db.consultar(conn, "SELECT cliente_id FROM clientes LIMIT 50")]
centros = [r["centro_id"] for r in db.consultar(conn, "SELECT centro_id FROM centros_distribucion")]
envios = []
for _ in range(600):
    o, d = random.sample(centros, 2)
    envios.append([db.crear_envio(conn, f"AUT{uuid.uuid4().hex[:12]}".upper(),
                                  random.choice(clientes), o, d), 0])

confirmados = []          # llaves que la app recibió como OK o DUPLICADO
fallos_seguidos = 0
inicio_caida = None
caidas = []
nodo_actual = None
t_inicio = time.perf_counter()
aviso_dado = False
i = 0
p(">>> Escribiendo eventos continuamente.")

while time.perf_counter() - t_inicio < DURACION_S:
    if not aviso_dado and time.perf_counter() - t_inicio > AVISO_S:
        p(">>> AHORA, en otra ventana:   docker kill logistica-db-primario")
        aviso_dado = True
    envio = envios[i % len(envios)]
    i += 1
    if envio[1] >= len(CICLO):
        continue
    llave = f"auto-{uuid.uuid4()}"
    estado = CICLO[envio[1]]
    while True:   # reintentar ESTE evento con la MISMA llave hasta lograrlo
        try:
            r = db.registrar_evento(conn, envio[0], estado, centros[0], llave, reintentos=1)
            break
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError, AttributeError) as e:
            if inicio_caida is None:
                inicio_caida = time.perf_counter()
                p(f"!!! Falla la escritura: {str(e)[:90]}")
            fallos_seguidos += 1
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(0.2)
            conn = conectar_con_espera()
    if inicio_caida is not None:
        dur = time.perf_counter() - inicio_caida
        caidas.append(dur)
        p(f"<<< Escritura restablecida tras {dur:.2f} s y {fallos_seguidos} reintentos. "
          f"Respuesta al reintento: {r}")
        inicio_caida, fallos_seguidos = None, 0
    if r in ("OK", "DUPLICADO"):
        confirmados.append(llave)
        envio[1] += 1
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT @@hostname h")
            h = cur.fetchone()["h"]
        conn.commit()
    except Exception:
        h = nodo_actual   # se cayó justo aquí; el siguiente evento lo detecta
    if h != nodo_actual:
        p(f"--- Escribiendo en el nodo {h}")
        nodo_actual = h
    time.sleep(0.05)

p("\nVerificación final en el primario actual...")
total_conf = len(confirmados)
encontrados = 0
for k in range(0, total_conf, 500):
    lote = confirmados[k:k + 500]
    marcas = ",".join(["%s"] * len(lote))
    encontrados += db.consultar(conn, f"SELECT COUNT(*) n FROM eventos_envio "
                                      f"WHERE idempotencia_key IN ({marcas})", lote)[0]["n"]
dup = db.consultar(conn, """SELECT COUNT(*) n FROM (SELECT idempotencia_key FROM eventos_envio
                            WHERE idempotencia_key LIKE 'auto-%%'
                            GROUP BY idempotencia_key HAVING COUNT(*) > 1) x""")[0]["n"]
p(f"Eventos confirmados a la app: {total_conf}")
p(f"Encontrados en la base:       {encontrados}  -> perdidos = {total_conf - encontrados}")
p(f"Llaves duplicadas:            {dup}")
p(f"Interrupciones de escritura:  {[round(x, 2) for x in caidas]} s")
p("PASA" if encontrados == total_conf and dup == 0 and caidas else
  "REVISAR (¿se mató el primario durante la prueba?)")
p("Ver también evidencias/vigilante.log (detección y promoción con hora exacta).")
conn.close()
_log.close()
