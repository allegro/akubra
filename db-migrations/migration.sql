CREATE TABLE consistency_record (
  request_id        CHARACTER(36)           PRIMARY KEY ,
  object_id         CHARACTER VARYING(1024) NOT NULL,
  method            CHARACTER VARYING(8)    NOT NULL,
  domain            CHARACTER VARYING(254)  NOT NULL,
  access_key        CHARACTER VARYING(128)  NOT NULL,
  execution_delay   INTERVAL                NOT NULL,
  inserted_at       TIMESTAMP               NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMP               NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX consistency_record__domain__object_id__inserted_at
  ON consistency_record
  USING btree (domain, object_id, inserted_at);

CREATE INDEX consistency_record__request_id
  ON consistency_record
  USING btree (request_id);

CREATE INDEX consistency_record__inserted_at
  ON consistency_record
  USING btree (inserted_at DESC);