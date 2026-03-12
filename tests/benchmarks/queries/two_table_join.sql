-- Two table join
-- Tests join overhead with broadcast or shuffle
-- Expected: Moderate DBU consumption (0.1-1.0)

SELECT 
    a.id,
    a.category,
    a.value,
    b.amount,
    b.status
FROM benchmark_table_a a
JOIN benchmark_table_b b ON a.id = b.table_a_id
WHERE a.category IN ('A', 'B', 'C')