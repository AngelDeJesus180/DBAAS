"""
Capa de acceso a datos de la empresa de logística.

- Conexiones separadas: escrituras al PRIMARIO, reportes a la RÉPLICA
  (en DBaaS serían el endpoint de escritura y el endpoint de lectura).
- registrar_evento(): la operación crítica del negocio. Hace TODO en una
  sola transacción: idempotencia + validación de transición + historial +
  estado actual. O se guarda todo, o no se guarda nada.
"""
import os
import time
import uuid
from datetime import datetime

import pymysql

HOST_PRIMARIO = os.getenv("DB_PRIMARIO_HOST", "localhost")
HOST_REPLICA = os.getenv("DB_REPLICA_HOST", "localhost")
PUERTO = int(os.getenv("DB_PORT", "3306"))
BASE = os.getenv("DB_NAME", "logistica")

# Errores de MySQL que vale la pena reintentar (bloqueo mutuo / espera agotada)
ERRORES_REINTENTABLES = {1213, 1205}


def _conectar(host, usuario, clave, base=BASE, timeout=5):
    return pymysql.connect(
        host=host, port=PUERTO, user=usuario, password=clave, database=base,
        connect_timeout=timeout, autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


# Archivo donde el vigilante (src/vigilante.py) anota cuál nodo es el primario.
# Es nuestra versión casera del "endpoint de escritura" de RDS, que cambia solo.
ARCHIVO_PRIMARIO = os.path.join(os.path.dirname(__file__), "..", "estado", "primario_actual.txt")


def primario_anotado():
    try:
        with open(ARCHIVO_PRIMARIO, encoding="utf-8") as f:
            return f.read().strip() or None
    except OSError:
        return None


def conexion_app(timeout=5):
    """
    Usuario de aplicación (SELECT/INSERT/UPDATE) contra el nodo que ACEPTA ESCRITURAS.
    Orden: 1) el que anotó el vigilante, 2) db-primario, 3) db-replica.
    Solo devuelve una conexión a un nodo con read_only = OFF; así nunca escribe en una réplica.
    """
    candidatos = []
    for h in (primario_anotado(), HOST_PRIMARIO, HOST_REPLICA):
        if h and h not in candidatos:
            candidatos.append(h)
    ultimo_error = None
    for host in candidatos:
        try:
            conn = _conectar(host, os.getenv("DB_APP_USER", "app_logistica"),
                             os.getenv("DB_APP_PASSWORD", "AppDemo#2026"), timeout=timeout)
        except pymysql.err.OperationalError as e:
            ultimo_error = e
            continue
        with conn.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.read_only AS ro")
            ro = cur.fetchone()["ro"]
        conn.commit()
        if ro == 0:
            return conn
        conn.close()
    raise pymysql.err.OperationalError(2003, f"Ningún nodo acepta escrituras ({ultimo_error})")


def conexion_reportes():
    """Usuario de solo lectura contra la RÉPLICA."""
    return _conectar(HOST_REPLICA, os.getenv("DB_REPORTES_USER", "app_reportes"),
                     os.getenv("DB_REPORTES_PASSWORD", "ReportesDemo#2026"))


def conexion_admin(host=None, timeout=5):
    """Root: solo para preparar datos de prueba y tareas de operación."""
    return _conectar(host or HOST_PRIMARIO, "root",
                     os.getenv("MYSQL_ROOT_PASSWORD", "RootDemo#2026"), timeout=timeout)


def crear_envio(conn, guia, cliente_id, origen_id, destino_id):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO envios (guia, cliente_id, centro_origen_id, centro_destino_id)
               VALUES (%s, %s, %s, %s)""",
            (guia, cliente_id, origen_id, destino_id))
        envio_id = cur.lastrowid
    conn.commit()
    return envio_id


def registrar_evento(conn, envio_id, estado_nuevo, centro_id,
                     idempotencia_key=None, fecha_evento=None,
                     fallar_a_mitad=False, reintentos=3):
    """
    Registra un cambio de estado de un envío. Devuelve un texto:
      'OK'                  -> se guardó el evento y se actualizó el envío
      'DUPLICADO'           -> esa idempotencia_key ya se había procesado (no hace nada)
      'NO_EXISTE'           -> el envío no existe
      'TRANSICION_INVALIDA' -> el cambio no está permitido (ej. entregado -> creado)
    Es seguro reintentarla: si se repite con la misma llave no duplica nada.
    """
    idempotencia_key = idempotencia_key or str(uuid.uuid4())
    fecha_evento = fecha_evento or datetime.now()

    for intento in range(1, reintentos + 1):
        try:
            with conn.cursor() as cur:
                conn.begin()
                # 1) Idempotencia: si la llave ya existe, la PK lo rechaza
                try:
                    cur.execute(
                        "INSERT INTO eventos_procesados (idempotencia_key, envio_id) VALUES (%s, %s)",
                        (idempotencia_key, envio_id))
                except pymysql.err.IntegrityError as e:
                    conn.rollback()
                    if e.args[0] == 1062:
                        return "DUPLICADO"
                    raise

                # 2) Bloquear la fila del envío (nadie más lo cambia mientras tanto)
                cur.execute("SELECT estado_actual FROM envios WHERE envio_id = %s FOR UPDATE",
                            (envio_id,))
                fila = cur.fetchone()
                if fila is None:
                    conn.rollback()
                    return "NO_EXISTE"
                estado_anterior = fila["estado_actual"]

                # 3) ¿Está permitida la transición?
                cur.execute("""SELECT 1 FROM transiciones_validas
                               WHERE estado_origen = %s AND estado_destino = %s""",
                            (estado_anterior, estado_nuevo))
                if cur.fetchone() is None:
                    conn.rollback()
                    return "TRANSICION_INVALIDA"

                # 4) Historial
                cur.execute("""INSERT INTO eventos_envio
                                 (envio_id, estado_anterior, estado_nuevo, centro_id,
                                  fecha_evento, idempotencia_key)
                               VALUES (%s, %s, %s, %s, %s, %s)""",
                            (envio_id, estado_anterior, estado_nuevo, centro_id,
                             fecha_evento, idempotencia_key))
                evento_id = cur.lastrowid

                if fallar_a_mitad:   # para la prueba de atomicidad
                    raise RuntimeError("Fallo simulado a mitad de la transacción")

                # 5) Estado actual + versión
                cur.execute("""UPDATE envios SET estado_actual = %s, version = version + 1
                               WHERE envio_id = %s""", (estado_nuevo, envio_id))
                cur.execute("UPDATE eventos_procesados SET evento_id = %s WHERE idempotencia_key = %s",
                            (evento_id, idempotencia_key))
            conn.commit()
            return "OK"
        except pymysql.err.OperationalError as e:
            conn.rollback()
            if e.args[0] in ERRORES_REINTENTABLES and intento < reintentos:
                time.sleep(0.05 * intento)   # espera corta y reintenta (es idempotente)
                continue
            raise
        except Exception:
            conn.rollback()
            raise


def actualizar_con_version(conn, envio_id, version_leida, nuevo_destino_id):
    """
    Concurrencia OPTIMISTA (para cambios desde pantallas, ej. corregir el destino):
    solo actualiza si nadie cambió el envío desde que lo leímos.
    Devuelve True si actualizó, False si hubo conflicto.
    """
    with conn.cursor() as cur:
        cur.execute("""UPDATE envios SET centro_destino_id = %s, version = version + 1
                       WHERE envio_id = %s AND version = %s""",
                    (nuevo_destino_id, envio_id, version_leida))
        ok = cur.rowcount == 1
    conn.commit()
    return ok


def consultar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        filas = cur.fetchall()
    conn.commit()
    return filas
