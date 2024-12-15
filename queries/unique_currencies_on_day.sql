SELECT deposit_date, COUNT(DISTINCT currency_id) AS unique_currencies
FROM FactDeposits
GROUP BY deposit_date;
