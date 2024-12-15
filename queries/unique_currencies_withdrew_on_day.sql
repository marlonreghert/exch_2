SELECT withdrawal_date, COUNT(DISTINCT currency_id) AS unique_currencies
FROM FactWithdrawals
GROUP BY withdrawal_date;
