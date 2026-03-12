-- Five table join (complex)
-- Tests multi-way join optimization and shuffle costs
-- Expected: Higher DBU consumption (0.5-5.0)

SELECT 
    a.category,
    COUNT(*) as row_count,
    SUM(b.amount) as total_amount,
    AVG(d.score) as avg_score,
    e.label
FROM benchmark_table_a a
JOIN benchmark_table_b b ON a.id = b.table_a_id
JOIN benchmark_table_c c ON b.id = c.table_b_id
JOIN benchmark_table_d d ON c.id = d.table_c_id
JOIN benchmark_table_e e ON d.id = e.table_d_id
WHERE a.value > 100
GROUP BY a.category, e.label