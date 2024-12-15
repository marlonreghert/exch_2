SELECT user_id, jurisdiction, MAX(level) AS latest_level
FROM DimUserLevels
GROUP BY user_id, jurisdiction;
