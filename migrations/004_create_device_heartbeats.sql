CREATE TABLE device_heartbeats
(
    time            TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    device_id       UUID        NOT NULL,
    hardware_uid    UUID        NOT NULL,

    battery_level   INTEGER,
    signal_strength INTEGER,
    firmware_version TEXT,

    PRIMARY KEY (device_id, hardware_uid, time)
);

SELECT create_hypertable(
               'device_heartbeats',
               'time',
               chunk_time_interval => INTERVAL '1 day'
       );

CREATE INDEX ON device_heartbeats (device_id, time DESC);
SELECT add_retention_policy('device_heartbeats', INTERVAL '30 days');
