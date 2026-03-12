-- Simple SELECT with LIMIT
-- Minimal complexity, baseline for cost comparison
-- Expected: Very low DBU consumption (< 0.1)

SELECT * FROM benchmark_table_a LIMIT 100