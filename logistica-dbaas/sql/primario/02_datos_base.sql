USE logistica;

-- Estados del ciclo de vida de un envío
INSERT IGNORE INTO estados_envio (codigo, orden, es_final) VALUES
('creado',      1, FALSE),
('recogido',    2, FALSE),
('en_transito', 3, FALSE),
('en_centro',   4, FALSE),
('en_reparto',  5, FALSE),
('entregado',   6, TRUE),
('devuelto',    7, TRUE),
('cancelado',   8, TRUE);

-- Cambios de estado permitidos (todo lo demás se rechaza)
INSERT IGNORE INTO transiciones_validas (estado_origen, estado_destino) VALUES
('creado',      'recogido'),
('creado',      'cancelado'),
('recogido',    'en_transito'),
('en_transito', 'en_centro'),
('en_centro',   'en_transito'),   -- puede pasar por varios centros
('en_centro',   'en_reparto'),
('en_reparto',  'entregado'),
('en_reparto',  'en_centro'),     -- intento de entrega fallido, vuelve al centro
('en_centro',   'devuelto');

-- Centros de distribución (datos sintéticos)
INSERT IGNORE INTO centros_distribucion (codigo, nombre, ciudad) VALUES
('BOG01', 'Centro Bogotá Norte',  'Bogotá'),
('BOG02', 'Centro Bogotá Sur',    'Bogotá'),
('MED01', 'Centro Medellín',      'Medellín'),
('CAL01', 'Centro Cali',          'Cali'),
('BAQ01', 'Centro Barranquilla',  'Barranquilla'),
('BUC01', 'Centro Bucaramanga',   'Bucaramanga');

-- Algunos clientes (el script de pruebas crea más)
INSERT IGNORE INTO clientes (documento, nombre, email) VALUES
('900100001', 'Cliente Demo A', 'demo.a@ejemplo.com'),
('900100002', 'Cliente Demo B', 'demo.b@ejemplo.com'),
('900100003', 'Cliente Demo C', 'demo.c@ejemplo.com');
