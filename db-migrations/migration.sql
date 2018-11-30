CREATE TABLE consistency_record (
  request_id        CHARACTER(36) PRIMARY KEY ,
  object_id         CHARACTER VARYING(1024) NOT NULL,
  method            CHARACTER VARYING(8) NOT NULL,
  cluster_name      CHARACTER VARYING(64) NOT NULL,
  access_key        CHARACTER VARYING(128) NOT NULL,
  execution_date    TIMESTAMP NOT NULL,
  inserted_at       TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX consistency_record__cluster_name__object_id__inserted_at
  ON consistency_record
  USING btree (cluster_name, object_id, inserted_at);