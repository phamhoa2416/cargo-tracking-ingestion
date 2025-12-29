-- Description: This script initializes the database schema for storing IoT device telemetry data using TimescaleDB.
-- It creates a hypertable with appropriate columns, compression, and retention policies.

-- Enable TimescaleDB extension
CREATE TABLE device_telemetry
(
    device_id       UUID        NOT NULL,
    time            TIMESTAMPTZ NOT NULL,
    hardware_uid    UUID        NOT NULL,

    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    pressure        DOUBLE PRECISION,

    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    altitude        DOUBLE PRECISION,
    speed           DOUBLE PRECISION,
    heading         DOUBLE PRECISION,
    accuracy        DOUBLE PRECISION,

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
    acknowledged_by UUID,

    PRIMARY KEY (device_id, time, event_type)
);

SELECT create_hypertable(
               'device_events',
               'time',
               chunk_time_interval => INTERVAL '7 days'
       );

CREATE INDEX ON device_events (device_id, time, event_type DESC);

CREATE TABLE shipment_tracking
(
    time                    TIMESTAMPTZ NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    shipment_id             UUID        NOT NULL,
    device_id               UUID        NOT NULL,

    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,

    status                  TEXT,
    eta_minutes             INTEGER,
    distance_to_destination DOUBLE PRECISION,

    speed                   DOUBLE PRECISION,
    heading                 DOUBLE PRECISION,
    accuracy                DOUBLE PRECISION,

    PRIMARY KEY (shipment_id, device_id, time)
);

SELECT create_hypertable(
               'shipment_tracking',
               'time',
               chunk_time_interval => INTERVAL '1 day'
       );

CREATE INDEX ON shipment_tracking (shipment_id, time DESC);
CREATE INDEX ON shipment_tracking (device_id, time DESC);



