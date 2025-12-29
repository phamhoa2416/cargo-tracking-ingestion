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