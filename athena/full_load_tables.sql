SELECT * FROM full_appointments LIMIT 10;

SELECT * FROM full_billing LIMIT 10;

SELECT * FROM full_doctors; LIMIT 10;

SELECT * FROM full_patients LIMIT 10;

SELECT * FROM full_treatments LIMIT 10;


SELECT COUNT(*) AS total_records
FROM full_appointments;

SELECT *
FROM full_appointments
WHERE col0 = 'A001';


SELECT patient_id, SUM(amount) AS total_spent
FROM full_billing
GROUP BY patient_id
ORDER BY total_spent DESC;

SELECT specialization, COUNT(*) AS total
FROM full_doctors
GROUP BY specialization;

