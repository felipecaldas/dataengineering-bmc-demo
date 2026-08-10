CREATE DATABASE airflow;

\connect retail

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS ingress;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS meta.demo_config (
    key text PRIMARY KEY,
    value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO meta.demo_config(key, value) VALUES
    ('withhold_eod_count', '0'),
    ('asn_enabled', 'true'),
    ('asn_schema_variant', 'standard'),
    ('silver_delay_seconds', '0'),
    ('wms_mode', 'ack'),
    ('wms_ack_delay_seconds', '2')
ON CONFLICT (key) DO NOTHING;

CREATE TABLE IF NOT EXISTS ingress.kafka_events (
    event_id text PRIMARY KEY,
    topic text NOT NULL,
    event_key text NOT NULL,
    payload jsonb NOT NULL,
    source_timestamp timestamptz,
    ingested_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS kafka_events_topic_date_idx
    ON ingress.kafka_events(topic, ((payload->>'trading_date')));

CREATE TABLE IF NOT EXISTS bronze.pos_transactions (
    transaction_id text PRIMARY KEY,
    trading_date date NOT NULL,
    store_id integer NOT NULL,
    till_id integer NOT NULL,
    product_sku text NOT NULL,
    qty integer NOT NULL,
    unit_price_ex_gst numeric(12,2) NOT NULL,
    transaction_ts_local timestamptz NOT NULL,
    transaction_ts_utc timestamptz NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.store_eod (
    store_id integer NOT NULL,
    trading_date date NOT NULL,
    transaction_count integer NOT NULL,
    total_ex_gst numeric(14,2) NOT NULL,
    eod_ts_local timestamptz NOT NULL,
    eod_ts_utc timestamptz NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(store_id, trading_date)
);

CREATE TABLE IF NOT EXISTS bronze.asn_raw (
    trading_date date PRIMARY KEY,
    blob_name text NOT NULL,
    header jsonb NOT NULL,
    rows jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.dim_store (
    store_id integer PRIMARY KEY,
    state_code text NOT NULL,
    status text NOT NULL,
    open_date date NOT NULL,
    close_date date,
    timezone text NOT NULL,
    close_time_local time NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.trading_calendar (
    calendar_date date NOT NULL,
    state_code text NOT NULL,
    is_trading_day boolean NOT NULL,
    holiday_name text,
    trading_restriction text,
    PRIMARY KEY(calendar_date, state_code)
);

CREATE TABLE IF NOT EXISTS silver.product_master (
    product_sku text PRIMARY KEY,
    product_name text NOT NULL,
    category text NOT NULL,
    unit_cost numeric(12,2) NOT NULL,
    retail_price numeric(12,2) NOT NULL,
    lead_time_days integer NOT NULL,
    review_period_days integer NOT NULL,
    safety_stock_units integer NOT NULL,
    is_active_line boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.pos_transactions
    (LIKE bronze.pos_transactions INCLUDING ALL);
CREATE TABLE IF NOT EXISTS silver.store_eod
    (LIKE bronze.store_eod INCLUDING ALL);

CREATE TABLE IF NOT EXISTS silver.asn_inbound (
    asn_id text NOT NULL,
    trading_date date NOT NULL,
    product_sku text NOT NULL,
    expected_units integer NOT NULL,
    expected_arrival_date date NOT NULL,
    supplier_id text NOT NULL,
    PRIMARY KEY(asn_id, product_sku)
);

CREATE TABLE IF NOT EXISTS silver.stock_on_hand (
    store_id integer NOT NULL,
    product_sku text NOT NULL,
    on_hand_units integer NOT NULL,
    on_order_units integer NOT NULL,
    snapshot_date date NOT NULL,
    PRIMARY KEY(store_id, product_sku, snapshot_date)
);

CREATE TABLE IF NOT EXISTS silver.sales_history (
    sale_date date NOT NULL,
    store_id integer NOT NULL,
    product_sku text NOT NULL,
    units_sold integer NOT NULL,
    sales_ex_gst numeric(14,2) NOT NULL,
    PRIMARY KEY(sale_date, store_id, product_sku)
);

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id bigserial PRIMARY KEY,
    stage text NOT NULL,
    trading_date date NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'RUNNING',
    row_count integer,
    message text
);

CREATE TABLE IF NOT EXISTS meta.wms_deliveries (
    filename text PRIMARY KEY,
    received_at timestamptz NOT NULL DEFAULT now(),
    acknowledged_at timestamptz,
    status text NOT NULL DEFAULT 'RECEIVED'
);

