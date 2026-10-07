-- Latest rule performance per rule (Databricks SQL). Replace ${catalog}.
SELECT rule_id, alerted_accounts, true_positive_accounts, positive_accounts,
       ROUND(precision, 3) AS precision, ROUND(recall, 3) AS recall
FROM ${catalog}.gold.mart_rule_performance
WHERE run_id = (SELECT run_id FROM ${catalog}.gold.mart_rule_performance ORDER BY computed_at DESC LIMIT 1)
ORDER BY rule_id;
