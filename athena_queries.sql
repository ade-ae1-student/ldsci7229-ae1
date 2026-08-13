-- Task 2: Athena analytical queries
-- Database: ade_ae1_datalake_db

-- Q1: Verify the merge - record counts by source
SELECT source, COUNT(*) AS record_count
FROM cleaned_unified_events
GROUP BY source;

-- Q2: Total records (0 bytes scanned - answered from Parquet footers)
SELECT COUNT(*) FROM cleaned_unified_events;

-- Q3: Single partition count (0 bytes scanned)
SELECT COUNT(*) FROM cleaned_unified_events WHERE year = '2020';

-- Q4: Aggregation across all partitions - 753.70 KB scanned
SELECT region, COUNT(*) AS events, AVG(metric_value) AS avg_metric
FROM cleaned_unified_events
GROUP BY region
ORDER BY events DESC
LIMIT 10;

-- Q5: Same aggregation, partition pruned - 64.04 KB scanned (91.5% reduction)
SELECT region, COUNT(*) AS events, AVG(metric_value) AS avg_metric
FROM cleaned_unified_events
WHERE year = '2020'
GROUP BY region
ORDER BY events DESC
LIMIT 10;

-- Q6: Trend over time - 2.74 KB scanned
SELECT year, COUNT(*) AS collisions
FROM cleaned_unified_events
WHERE source = 'la_collisions'
GROUP BY year
ORDER BY year;

-- Q7: Same aggregation against raw CSV - 118.11 MB scanned
-- Demonstrates columnar vs row storage, and shows the age-99 sentinel
-- inflating the raw average by ~0.74 years
SELECT "area name" AS region, COUNT(*) AS events, AVG("victim age") AS avg_metric
FROM raw_batch_la_collisions
GROUP BY "area name"
ORDER BY events DESC
LIMIT 10;
