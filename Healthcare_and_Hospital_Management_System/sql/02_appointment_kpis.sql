SELECT COUNT(*) total_appointments,
100.0*SUM(CASE WHEN status='Completed' THEN 1 ELSE 0 END)/COUNT(*) completion_rate_pct,
100.0*SUM(CASE WHEN status='Cancelled' THEN 1 ELSE 0 END)/COUNT(*) cancellation_rate_pct,
100.0*SUM(CASE WHEN status='Scheduled' THEN 1 ELSE 0 END)/COUNT(*) scheduled_rate_pct,
100.0*SUM(CASE WHEN status='No-show' THEN 1 ELSE 0 END)/COUNT(*) no_show_rate_pct
FROM healthcare_silver.appointments;