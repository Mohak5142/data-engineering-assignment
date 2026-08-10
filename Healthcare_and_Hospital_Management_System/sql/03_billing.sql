SELECT SUM(amount) total_billed_amount, AVG(amount) average_bill_amount,
100.0*SUM(CASE WHEN payment_status='Paid' THEN 1 ELSE 0 END)/COUNT(*) payment_success_rate_proxy_pct
FROM healthcare_silver.billing;