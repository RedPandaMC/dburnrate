-- SQLite schema for mocking Databricks system tables
-- This schema replicates the structure of system.* tables for testing

-- system.billing.usage equivalent
CREATE TABLE system_billing_usage (
    account_id TEXT,
    workspace_id TEXT,
    record_id TEXT PRIMARY KEY,
    sku_name TEXT,
    cloud TEXT DEFAULT 'AZURE',
    usage_start_time TEXT,
    usage_end_time TEXT,
    usage_date TEXT,
    custom_tags TEXT,
    usage_unit TEXT DEFAULT 'DBU',
    usage_quantity REAL,
    usage_metadata TEXT,
    identity_metadata TEXT,
    record_type TEXT DEFAULT 'ORIGINAL',
    ingestion_date TEXT,
    billing_origin_product TEXT,
    product_features TEXT,
    usage_type TEXT DEFAULT 'COMPUTE_TIME'
);

-- system.billing.list_prices equivalent
CREATE TABLE system_billing_list_prices (
    sku_name TEXT PRIMARY KEY,
    cloud TEXT,
    currency_code TEXT DEFAULT 'USD',
    price_usd REAL,
    price_start_time TEXT,
    price_end_time TEXT
);

-- system.query.history equivalent
CREATE TABLE system_query_history (
    statement_id TEXT PRIMARY KEY,
    statement_text TEXT,
    statement_type TEXT,
    start_time TEXT,
    end_time TEXT,
    execution_duration_ms INTEGER,
    compilation_duration_ms INTEGER,
    read_bytes INTEGER,
    read_rows INTEGER,
    produced_rows INTEGER,
    written_bytes INTEGER,
    total_task_duration_ms INTEGER,
    warehouse_id TEXT,
    cluster_id TEXT,
    status TEXT DEFAULT 'FINISHED',
    error_message TEXT
);

-- system.compute.node_types equivalent
CREATE TABLE system_compute_node_types (
    instance_type TEXT PRIMARY KEY,
    vcpus INTEGER,
    memory_gb INTEGER,
    category TEXT,
    dbu_per_hour REAL,
    photon_dbu_per_hour REAL
);

-- Mock tables for DESCRIBE DETAIL simulation
CREATE TABLE mock_tables (
    table_name TEXT PRIMARY KEY,
    location TEXT,
    size_in_bytes INTEGER,
    num_files INTEGER,
    num_records INTEGER,
    partition_columns TEXT
);

-- Benchmark test tables
CREATE TABLE benchmark_table_a (
    id INTEGER PRIMARY KEY,
    category TEXT,
    value REAL,
    created_at TEXT
);

CREATE TABLE benchmark_table_b (
    id INTEGER PRIMARY KEY,
    table_a_id INTEGER,
    amount REAL,
    status TEXT,
    FOREIGN KEY (table_a_id) REFERENCES benchmark_table_a(id)
);

CREATE TABLE benchmark_table_c (
    id INTEGER PRIMARY KEY,
    table_b_id INTEGER,
    description TEXT,
    FOREIGN KEY (table_b_id) REFERENCES benchmark_table_b(id)
);

CREATE TABLE benchmark_table_d (
    id INTEGER PRIMARY KEY,
    table_c_id INTEGER,
    score REAL,
    FOREIGN KEY (table_c_id) REFERENCES benchmark_table_c(id)
);

CREATE TABLE benchmark_table_e (
    id INTEGER PRIMARY KEY,
    table_d_id INTEGER,
    label TEXT,
    FOREIGN KEY (table_d_id) REFERENCES benchmark_table_d(id)
);

-- Create indexes for join performance
CREATE INDEX idx_table_b_a_id ON benchmark_table_b(table_a_id);
CREATE INDEX idx_table_c_b_id ON benchmark_table_c(table_b_id);
CREATE INDEX idx_table_d_c_id ON benchmark_table_d(table_c_id);
CREATE INDEX idx_table_e_d_id ON benchmark_table_e(table_d_id);
CREATE INDEX idx_table_a_category ON benchmark_table_a(category);
