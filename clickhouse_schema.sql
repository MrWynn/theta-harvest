CREATE TABLE IF NOT EXISTS laevitas.thetadata_options_chain_1m
(
    `symbol` LowCardinality(String) CODEC(ZSTD(1)),
    `expiration` Date CODEC(Delta, ZSTD(1)),
    `strike` Float64 CODEC(Gorilla, ZSTD(1)),
    `right` Enum8('CALL' = 1, 'PUT' = 2) CODEC(ZSTD(1)),
    `timestamp` DateTime64(3, 'America/New_York') CODEC(DoubleDelta, ZSTD(1)),
    `data_date` Date CODEC(Delta, ZSTD(1)),
    `open` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `high` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `low` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `close` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `volume` Nullable(UInt64) CODEC(T64, ZSTD(1)),
    `count` Nullable(UInt32) CODEC(T64, ZSTD(1)),
    `vwap` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `bid_size` Nullable(UInt32) CODEC(T64, ZSTD(1)),
    `bid_exchange` Nullable(UInt16) CODEC(T64, ZSTD(1)),
    `bid` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `bid_condition` Nullable(UInt32) CODEC(T64, ZSTD(1)),
    `ask_size` Nullable(UInt32) CODEC(T64, ZSTD(1)),
    `ask_exchange` Nullable(UInt16) CODEC(T64, ZSTD(1)),
    `ask` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `ask_condition` Nullable(UInt32) CODEC(T64, ZSTD(1)),
    `delta` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `gamma` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `theta` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `vega` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `rho` Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    `underlying_time` Nullable(DateTime64(3, 'America/New_York')) CODEC(DoubleDelta, ZSTD(1)),
    `underlying_price` Nullable(Float64) CODEC(Gorilla, ZSTD(1))
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(data_date)
PRIMARY KEY (symbol, data_date, expiration)
ORDER BY (symbol, data_date, expiration, right, strike, timestamp)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS laevitas.thetadata_options_chain_1m_progress
(
    `symbol` LowCardinality(String) CODEC(ZSTD(1)),
    `data_date` Date CODEC(Delta, ZSTD(1)),
    `status` Enum8('complete' = 1, 'no_data' = 2) CODEC(ZSTD(1)),
    `row_count` UInt64 CODEC(T64, ZSTD(1)),
    `checked_expiration_count` UInt32 CODEC(T64, ZSTD(1)),
    `written_expiration_count` UInt32 CODEC(T64, ZSTD(1)),
    `schema_version` UInt16 DEFAULT 1 CODEC(T64, ZSTD(1)),
    `completed_at` DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(completed_at)
PARTITION BY toYYYY(data_date)
PRIMARY KEY (symbol, data_date)
ORDER BY (symbol, data_date)
SETTINGS index_granularity = 8192;
