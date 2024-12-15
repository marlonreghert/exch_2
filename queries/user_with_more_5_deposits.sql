SELECT user_id
FROM FactDeposits
WHERE deposit_date <= '2024-01-01'
GROUP BY user_id
HAVING COUNT(*) > 5;