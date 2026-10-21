-- =====================================================================
-- Usuarios con PERMISOS MÍNIMOS (nadie usa root desde la aplicación).
-- Contraseñas SOLO DE DEMOSTRACIÓN. En DBaaS real irían en un gestor de
-- secretos (AWS Secrets Manager / Google Secret Manager) o con
-- autenticación IAM, nunca escritas en el código.
-- =====================================================================

-- 1) Aplicación: lee y escribe datos de negocio. NO puede borrar ni
--    cambiar la estructura (sin DELETE, DROP, ALTER).
CREATE USER IF NOT EXISTS 'app_logistica'@'%' IDENTIFIED BY 'AppDemo#2026';
GRANT SELECT, INSERT, UPDATE ON logistica.* TO 'app_logistica'@'%';

-- 2) Reportes: solo lectura. Se usa contra la RÉPLICA.
CREATE USER IF NOT EXISTS 'app_reportes'@'%' IDENTIFIED BY 'ReportesDemo#2026';
GRANT SELECT ON logistica.* TO 'app_reportes'@'%';

-- 3) Replicación: solo puede leer el binlog del primario.
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'ReplDemo#2026';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';

FLUSH PRIVILEGES;
