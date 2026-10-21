"""
VIGILANTE: failover AUTOMÁTICO del prototipo (versión casera de lo que hacen
RDS Multi-AZ / Cloud SQL HA).

Cada segundo revisa el primario. Si falla 3 veces seguidas:
  1. Espera a que la réplica aplique todo lo que ya recibió (no perder datos).
  2. Promueve la réplica (STOP REPLICA, RESET REPLICA ALL, read_only=OFF).
  3. Anota el nuevo primario en estado/primario_actual.txt (la app lo lee).
Si el primario viejo vuelve a encender, lo AÍSLA (super_read_only=ON) para
que no haya dos primarios a la vez (split-brain). Reincorporarlo como réplica
es un paso manual (ver README).

Se activa con:  docker compose --profile auto up -d --build
Bitácora:       evidencias/vigilante.log
"""
import os
import time
from datetime import datetime

import pymysql

import db

INTERVALO_S = 1.0          # cada cuánto revisa
FALLOS_PARA_PROMOVER = 3   # evita promover por un simple parpadeo de red
RAIZ = os.path.join(os.path.dirname(__file__), "..")
LOG = os.path.join(RAIZ, "evidencias", "vigilante.log")


def log(msg):
    linea = f"{datetime.now().isoformat(timespec='milliseconds')} {msg}"
    print(linea, flush=True)
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(linea + "\n")


def anotar_primario(host):
    os.makedirs(os.path.dirname(db.ARCHIVO_PRIMARIO), exist_ok=True)
    tmp = db.ARCHIVO_PRIMARIO + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(host)
    os.replace(tmp, db.ARCHIVO_PRIMARIO)   # escritura atómica


def vivo(host):
    try:
        c = db.conexion_admin(host, timeout=1)
        with c.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.read_only AS ro")
            ro = cur.fetchone()["ro"]
        c.close()
        return True, ro
    except pymysql.err.MySQLError:
        return False, None


def promover(host):
    t0 = time.perf_counter()
    c = db.conexion_admin(host, timeout=3)
    with c.cursor() as cur:
        cur.execute("SHOW REPLICA STATUS")
        st = cur.fetchone()
        recibido = ((st or {}).get("Retrieved_Gtid_Set") or "").replace("\n", "")
        if recibido:
            cur.execute("SELECT WAIT_FOR_EXECUTED_GTID_SET(%s, 15) AS r", (recibido,))
            r = cur.fetchone()["r"]
            log(f"  réplica aplicó todo lo recibido: {'SÍ' if r == 0 else 'NO (timeout 15 s)'}")
        cur.execute("STOP REPLICA")
        cur.execute("RESET REPLICA ALL")
        cur.execute("SET PERSIST super_read_only = OFF")
        cur.execute("SET PERSIST read_only = OFF")
    c.commit()
    c.close()
    anotar_primario(host)
    log(f"  PROMOVIDO {host} a primario en {(time.perf_counter() - t0) * 1000:.0f} ms")


def aislar(host):
    c = db.conexion_admin(host, timeout=2)
    with c.cursor() as cur:
        cur.execute("SET PERSIST super_read_only = ON")
    c.commit()
    c.close()
    log(f"  FENCING: {host} volvió y quedó en solo lectura (no hay dos primarios). "
        f"Reincorporarlo como réplica es manual.")


def main():
    primario, reserva = db.HOST_PRIMARIO, db.HOST_REPLICA
    # Al arrancar: si el primario original está bien, ese es el primario.
    ok, ro = vivo(primario)
    if ok and ro == 0:
        anotar_primario(primario)
    elif db.primario_anotado() == reserva:
        primario, reserva = reserva, primario
    log(f"Vigilante iniciado. Primario={primario} Reserva={reserva}")

    fallos = 0
    promovido = False
    while True:
        ok, _ = vivo(primario)
        if ok:
            if fallos:
                log(f"Primario {primario} responde de nuevo tras {fallos} fallo(s); no se promueve.")
            fallos = 0
        else:
            fallos += 1
            log(f"Primario {primario} NO responde ({fallos}/{FALLOS_PARA_PROMOVER})")
            if fallos >= FALLOS_PARA_PROMOVER and not promovido:
                ok_r, ro_r = vivo(reserva)
                if ok_r:
                    log(f"FAILOVER AUTOMÁTICO: promoviendo {reserva}")
                    promover(reserva)
                    primario, reserva = reserva, primario
                    promovido, fallos = True, 0
                else:
                    log("La reserva tampoco responde: no hay a quién promover.")

        # Si el primario viejo vuelve con escrituras habilitadas, aislarlo
        if promovido:
            ok_v, ro_v = vivo(reserva)
            if ok_v and ro_v == 0:
                aislar(reserva)
        time.sleep(INTERVALO_S)


if __name__ == "__main__":
    main()
