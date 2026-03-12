-- GROUP BY aggregation
-- Tests shuffle overhead for aggregation
-- Expected: Moderate DBU consumption (0.05-0.5)

SELECT 
    category,
    COUNT(*) as row_count,
    AVG(value) as avg_value,
    SUM(value) as total_value,
    MIN(value) as min_value,
    MAX(value) as max_value
FROM benchmark_table_a
GROUP BY category