-- Source of truth schema — PostgreSQL (schema.sql is applied by aggregator init)
-- Applied by docker compose init container AND the VM bootstrap.

CREATE TABLE IF NOT EXISTS station_reports (
    station_id         TEXT PRIMARY KEY,
    election_id        TEXT NOT NULL,
    region             TEXT NOT NULL,
    registered_voters  INTEGER NOT NULL CHECK (registered_voters > 0),
    ballots_cast       INTEGER NOT NULL CHECK (ballots_cast >= 0),
    candidates         JSONB   NOT NULL,
    published_at       TIMESTAMPTZ NOT NULL,
    polls_closed_at    TIMESTAMPTZ NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS anomalies_log (
    anomaly_id   TEXT PRIMARY KEY,
    election_id  TEXT NOT NULL,
    station_id   TEXT,
    region       TEXT,
    kind         TEXT NOT NULL,
    details      TEXT,
    report_id    TEXT,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Region totals straight from the latest station snapshots (the source of truth
-- the dashboard API serves).
CREATE OR REPLACE VIEW region_results AS
SELECT
    election_id,
    region,
    j.key AS candidate,
    SUM(j.value::BIGINT)::BIGINT AS votes,
    SUM(ballots_cast) AS ballots_cast
FROM station_reports s,
     LATERAL jsonb_each_text(s.candidates) AS j
GROUP BY s.election_id, s.region, j.key;

COMMENT ON VIEW region_results IS 'region totals computed from the latest reports by station';
