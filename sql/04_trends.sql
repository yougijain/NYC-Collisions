-- Monthly crash trends with injuries and fatalities
SELECT 
  strftime('%Y-%m', crash_datetime) AS month,
  COUNT(*) AS crash_count,
  SUM(number_of_persons_injured) AS total_injuries,
  SUM(number_of_persons_killed) AS total_fatalities,
  AVG(number_of_persons_injured) AS avg_injuries_per_crash
FROM collisions_clean
WHERE number_of_persons_injured > 0 
   OR number_of_persons_killed > 0
GROUP BY month
ORDER BY month;
