-- Criação dos schemas de camadas do pipeline
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Garante que o usuário do pipeline tem acesso a todos os schemas
GRANT ALL PRIVILEGES ON SCHEMA raw     TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA marts   TO pipeline_user;