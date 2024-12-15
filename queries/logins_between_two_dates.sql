SELECT user_id, COUNT(*) AS login_count
FROM FactUserActivity
WHERE activity_type = 'login' AND activity_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY user_id;
