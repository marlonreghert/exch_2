SELECT user_id, MAX(activity_date) AS last_login_date
FROM FactUserActivity
WHERE activity_type = 'login'
GROUP BY user_id;
