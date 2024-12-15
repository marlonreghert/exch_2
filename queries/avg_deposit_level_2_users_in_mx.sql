SELECT AVG(d.amount) AS avg_deposit
FROM FactDeposits d
JOIN DimUsers u ON d.user_id = u.user_id
JOIN DimUserLevels l ON d.user_id = l.user_id
WHERE l.level = 2 AND u.jurisdiction = 'MX';