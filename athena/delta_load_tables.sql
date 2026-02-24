SELECT * FROM delta_appointments LIMIT 10;

SELECT * FROM delta_billing LIMIT 10;

SELECT * FROM delta_doctors; LIMIT 10;

SELECT * FROM delta_patients LIMIT 10;

SELECT * FROM delta_treatments LIMIT 10;


SELECT *
FROM delta_billing
WHERE payment_status = 'PENDING';

SELECT COUNT(*) AS total_delta_patients
FROM delta_patients;


SELECT * 
FROM delta_patients
LIMIT 5;

DESCRIBE delta_patients;

SELECT *
FROM delta_patients
ORDER BY updated_date DESC
LIMIT 5;


SELECT insurance_provider, COUNT(*) AS total
FROM delta_patients
GROUP BY insurance_provider
ORDER BY total DESC;

SELECT patient_id, COUNT(*) AS total
FROM delta_patients
GROUP BY patient_id
HAVING COUNT(*) > 1;


SELECT patient_id,
       first_name,
       last_name,
       date_of_birth
FROM delta_patients
ORDER BY date_of_birth ASC
LIMIT 10;


SELECT *
FROM delta_patients
WHERE insurance_provider IS NULL
   OR insurance_provider = '';
   
   
   
SELECT COUNT(DISTINCT contact_number) AS unique_contacts
FROM delta_patients;


SELECT *
FROM delta_patients
ORDER BY updated_date DESC
LIMIT 10;

SELECT p.patient_id,
       COUNT(a.col0) AS total_visits
FROM delta_patients p
LEFT JOIN full_appointments a
ON p.patient_id = a.col1
GROUP BY p.patient_id
ORDER BY total_visits DESC;

