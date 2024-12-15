SELECT activity_date, COUNT(DISTINCT user_id) AS active_users
FROM FactUserActivity
WHERE activity_type IN ('deposit', 'withdrawal')
GROUP BY activity_date;
