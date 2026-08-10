SELECT d.doctor_id,d.first_name,d.last_name,d.specialization,d.hospital_branch,
COUNT(a.appointment_id) appointment_count,
SUM(CASE WHEN a.status='Completed' THEN 1 ELSE 0 END) completed_count,
SUM(CASE WHEN a.status='No-show' THEN 1 ELSE 0 END) no_show_count,
100.0*SUM(CASE WHEN a.status='No-show' THEN 1 ELSE 0 END)/COUNT(a.appointment_id) no_show_rate_pct
FROM healthcare_silver.doctors d LEFT JOIN healthcare_silver.appointments a ON d.doctor_id=a.doctor_id
GROUP BY d.doctor_id,d.first_name,d.last_name,d.specialization,d.hospital_branch;