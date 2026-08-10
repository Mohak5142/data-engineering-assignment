SELECT date_format(appointment_date,'yyyy-MM') month,COUNT(*) total_appointments,
SUM(CASE WHEN status='Completed' THEN 1 ELSE 0 END) completed,
SUM(CASE WHEN status='Cancelled' THEN 1 ELSE 0 END) cancelled,
SUM(CASE WHEN status='No-show' THEN 1 ELSE 0 END) no_show,
100.0*SUM(CASE WHEN status='No-show' THEN 1 ELSE 0 END)/COUNT(*) no_show_rate_pct
FROM healthcare_silver.appointments GROUP BY date_format(appointment_date,'yyyy-MM') ORDER BY month;