# Data Dictionary

## patients.csv
patient_id: string, PK
first_name: string, PII
last_name: string, PII
gender: categorical
date_of_birth: date, sensitive; hashed in Silver
contact_number: string, PII; hashed in Silver
address: string, PII; hashed in Silver
registration_date: date
insurance_provider: string
insurance_number: string, PII; hashed in Silver
email: string, PII; hashed in Silver

## appointments.csv
appointment_id: string, PK
patient_id: string, FK -> patients
doctor_id: string, FK -> doctors
appointment_date: date
appointment_time: time/string
reason_for_visit: string
status: Scheduled/Completed/Cancelled/No-show

## billing.csv
bill_id: string, PK
patient_id: string, FK -> patients
treatment_id: string, FK -> treatments
bill_date: date
amount: double
payment_method: string
payment_status: Paid/Pending/Failed

## doctors.csv
doctor_id: string, PK
first_name/last_name: PII
specialization: string
phone_number: PII; hashed in Silver
years_experience: integer
hospital_branch: string
email: PII; hashed in Silver

## treatments.csv
treatment_id: string, PK
appointment_id: string, FK -> appointments
treatment_type: string
description: string
cost: double
treatment_date: date
