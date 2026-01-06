CREATE TABLE device_telemetry
(
    device_id       UUID        NOT NULL,
    time            TIMESTAMPTZ NOT NULL,
    hardware_uid    UUID        NOT NULL,

    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    co2             DOUBLE PRECISION,
    light           DOUBLE PRECISION,

    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    speed           DOUBLE PRECISION,
    accuracy        DOUBLE PRECISION,
    lean            DOUBLE PRECISION,

    battery_level   INTEGER,
    signal_strength INTEGER,

    is_moving       BOOLEAN,
    event_type      TEXT,

    raw_payload     JSONB,

    PRIMARY KEY (device_id, hardware_uid, time)
);

SELECT create_hypertable(
               'device_telemetry',
               'time',
               chunk_time_interval => INTERVAL '1 day'
       );

ALTER TABLE device_telemetry
    SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'device_id, hardware_uid'
    );

SELECT add_compression_policy('device_telemetry', INTERVAL '14 days');
SELECT add_retention_policy('device_telemetry', INTERVAL '90 days');

CREATE INDEX ON device_telemetry (device_id, time DESC);