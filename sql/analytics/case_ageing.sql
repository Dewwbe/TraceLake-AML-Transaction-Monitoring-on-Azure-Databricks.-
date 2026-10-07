-- Case ageing and workload (case fields are SIMULATED in this repository).
SELECT priority, status,
       COUNT(*) AS cases,
       ROUND(AVG(DATEDIFF(COALESCE(closed_ts, current_timestamp()), opened_ts)), 1) AS avg_age_days
FROM ${catalog}.gold.fact_case
GROUP BY priority, status
ORDER BY priority, status;
