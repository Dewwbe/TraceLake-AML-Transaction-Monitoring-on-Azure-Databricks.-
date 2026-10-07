-- Customers whose risk rating changed, with the version history (SCD2).
SELECT d.customer_id, d.risk_rating, d.risk_score, d.valid_from, d.valid_to, d.is_current
FROM ${catalog}.gold.dim_customer d
JOIN (SELECT customer_id FROM ${catalog}.gold.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1) m
  USING (customer_id)
ORDER BY d.customer_id, d.valid_from;
