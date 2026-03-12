-- Single table with filter predicate
-- Tests filtering efficiency and scan costs
-- Expected: Low DBU consumption (0.02-0.2)

SELECT * FROM benchmark_table_a 
WHERE category = 'A' AND value > 500