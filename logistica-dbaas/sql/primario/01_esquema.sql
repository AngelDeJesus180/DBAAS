-- =====================================================================
--  LOGÍSTICA - Modelo de datos (MySQL 8.0, compatible con Amazon RDS
--  for MySQL y Google Cloud SQL for MySQL)
--  Este script solo se ejecuta en el PRIMARIO. La réplica recibe todo
--  por replicación (binlog + GTID).
-- =====================================================================

CREATE DATABASE IF NOT EXISTS logistica
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE logistica;

-- ---------------------------------------------------------------------
-- Clientes
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clientes (
    cliente_id      INT AUTO_INCREMENT PRIMARY KEY,
    documento       VARCHAR(20)  NOT NULL,
    nombre          VARCHAR(100) NOT NULL,
    email           VARCHAR(150) NOT NULL,
    fecha_registro  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_clientes_documento UNIQUE (documento),
    CONSTRAINT uq_clientes_email     UNIQUE (email)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- Centros de distribución
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS centros_distribucion (
    centro_id  INT AUTO_INCREMENT PRIMARY KEY,
    codigo     VARCHAR(10)  NOT NULL,
    nombre     VARCHAR(100) NOT NULL,
    ciudad     VARCHAR(100) NOT NULL,
    CONSTRAINT uq_centros_codigo UNIQUE (codigo)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- Catálogo de estados y transiciones permitidas (máquina de estados)
-- Así la base de datos (no solo la app) sabe qué cambios son válidos.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS estados_envio (
    codigo    VARCHAR(20) PRIMARY KEY,
    orden     TINYINT     NOT NULL,
    es_final  BOOLEAN     NOT NULL DEFAULT FALSE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS transiciones_validas (
    estado_origen   VARCHAR(20) NOT NULL,
    estado_destino  VARCHAR(20) NOT NULL,
    PRIMARY KEY (estado_origen, estado_destino),
    FOREIGN KEY (estado_origen)  REFERENCES estados_envio(codigo),
    FOREIGN KEY (estado_destino) REFERENCES estados_envio(codigo)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- Envíos: UNA fila por envío con su estado ACTUAL (lectura rápida).
-- version = control de concurrencia optimista.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS envios (
    envio_id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    guia                VARCHAR(20) NOT NULL,            -- número de rastreo
    cliente_id          INT         NOT NULL,
    centro_origen_id    INT         NOT NULL,
    centro_destino_id   INT         NOT NULL,
    estado_actual       VARCHAR(20) NOT NULL DEFAULT 'creado',
    version             INT         NOT NULL DEFAULT 0,
    fecha_creacion      DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT uq_envios_guia UNIQUE (guia),
    CONSTRAINT fk_envios_cliente FOREIGN KEY (cliente_id)
        REFERENCES clientes(cliente_id),
    CONSTRAINT fk_envios_origen  FOREIGN KEY (centro_origen_id)
        REFERENCES centros_distribucion(centro_id),
    CONSTRAINT fk_envios_destino FOREIGN KEY (centro_destino_id)
        REFERENCES centros_distribucion(centro_id),
    CONSTRAINT fk_envios_estado  FOREIGN KEY (estado_actual)
        REFERENCES estados_envio(codigo),
    CONSTRAINT ck_envios_origen_destino CHECK (centro_origen_id <> centro_destino_id),
    CONSTRAINT ck_envios_version        CHECK (version >= 0),
    INDEX idx_envios_cliente_fecha (cliente_id, fecha_creacion),
    INDEX idx_envios_estado_destino (estado_actual, centro_destino_id)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- Eventos de envío: historial (10 M filas nuevas por día).
-- PARTICIONADA POR MES para que:
--   * las consultas por rango de fechas solo lean el mes necesario;
--   * la retención sea barata (DROP PARTITION en vez de DELETE masivo).
-- Limitación de MySQL: una tabla particionada NO admite llaves foráneas
-- y toda llave única debe incluir la columna de partición. Por eso:
--   * la integridad con envios la garantiza la transacción de registro
--     (src/db.py -> registrar_evento), que bloquea el envío con FOR UPDATE;
--   * la idempotencia vive en la tabla eventos_procesados (abajo).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS eventos_envio (
    evento_id         BIGINT      NOT NULL AUTO_INCREMENT,
    envio_id          BIGINT      NOT NULL,
    estado_anterior   VARCHAR(20) NOT NULL,
    estado_nuevo      VARCHAR(20) NOT NULL,
    centro_id         INT         NOT NULL,
    fecha_evento      DATETIME    NOT NULL,                     -- cuándo ocurrió (escáner)
    recibido_en       DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP, -- cuándo llegó
    idempotencia_key  VARCHAR(64) NOT NULL,
    PRIMARY KEY (evento_id, fecha_evento),
    INDEX idx_eventos_envio_fecha  (envio_id, fecha_evento),
    INDEX idx_eventos_centro_fecha (centro_id, fecha_evento)
) ENGINE=InnoDB
PARTITION BY RANGE COLUMNS (fecha_evento) (
    PARTITION p2026_08 VALUES LESS THAN ('2026-09-01'),
    PARTITION p2026_09 VALUES LESS THAN ('2026-10-01'),
    PARTITION p2026_10 VALUES LESS THAN ('2026-11-01'),
    PARTITION p2026_11 VALUES LESS THAN ('2026-12-01'),
    PARTITION p2026_12 VALUES LESS THAN ('2027-01-01'),
    PARTITION p2027_01 VALUES LESS THAN ('2027-02-01'),
    PARTITION pmax     VALUES LESS THAN (MAXVALUE)
);

-- ---------------------------------------------------------------------
-- Idempotencia: cada evento trae una llave única generada por quien lo
-- envía (escáner / app). Si llega dos veces (reintento), la PK lo frena.
-- Retención sugerida: 30 días (los reintentos ocurren en minutos/horas).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS eventos_procesados (
    idempotencia_key  VARCHAR(64) PRIMARY KEY,
    envio_id          BIGINT   NOT NULL,
    evento_id         BIGINT   NULL,
    recibido_en       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_procesados_fecha (recibido_en)
) ENGINE=InnoDB;
