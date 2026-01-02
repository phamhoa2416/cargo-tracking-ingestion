CREATE TABLE device_events
(
    time            TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    device_id       UUID        NOT NULL,
    hardware_uid    UUID,

    event_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL,

    description     TEXT,
    metadata        JSONB,

    acknowledged    BOOLEAN     NOT NULL DEFAULT false,
    acknowledged_at TIMESTAMPTZ,

    PRIMARY KEY (device_id, time, event_type)
);

SELECT create_hypertable(
               'device_events',
               'time',
               chunk_time_interval => INTERVAL '7 days'
       );

CREATE INDEX ON device_events (device_id, time, event_type DESC);