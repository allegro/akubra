CREATE TABLE consistency_record (
  request_id      CHARACTER(36) PRIMARY KEY ,
  object_id       CHARACTER VARYING(1024) NOT NULL,
  method          CHARACTER VARYING(8) NOT NULL,
  cluster_name    CHARACTER VARYING(64) NOT NULL,
  access_key      CHARACTER VARYING(128) NOT NULL,
  execution_date  TIMESTAMP NOT NULL,
  created_at      TIMESTAMP NOT NULL,
  updated_at      TIMESTAMP NOT NULL
);