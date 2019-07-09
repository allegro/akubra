CREATE TABLE consistency_record
(
  object_version  BIGINT                  NOT NULL DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP at time zone 'utc') * 10^6,
  request_id      CHARACTER(36) PRIMARY KEY,
  object_id       CHARACTER VARYING(1024) NOT NULL,
  method          CHARACTER VARYING(8)    NOT NULL,
  domain          CHARACTER VARYING(254)  NOT NULL,
  access_key      CHARACTER VARYING(128)  NOT NULL,
  execution_delay INTERVAL                NOT NULL,
  inserted_at     TIMESTAMPTZ             NOT NULL DEFAULT (CURRENT_TIMESTAMP at time zone 'utc'),
  updated_at      TIMESTAMPTZ             NOT NULL DEFAULT (CURRENT_TIMESTAMP at time zone 'utc'),
  error           CHARACTER VARYING(1024)          DEFAULT ''
);

CREATE UNIQUE INDEX consistency_record__domain__object_id__inserted_at
  ON consistency_record
    USING btree (domain, object_id, object_version);

CREATE INDEX consistency_record__request_id
  ON consistency_record
    USING btree (request_id);

CREATE INDEX consistency_record__inserted_at
  ON consistency_record
    USING btree (object_version DESC);

