SELECT u.user_id
FROM DimUsers u
LEFT JOIN FactDeposits d ON u.user_id = d.user_id
WHERE d.user_id IS NULL;