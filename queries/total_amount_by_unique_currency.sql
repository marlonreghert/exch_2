SELECT deposit_date, currency_id, SUM(amount) AS total_amount
FROM FactDeposits
WHERE currency_id = 'USD'
GROUP BY deposit_date, currency_id;