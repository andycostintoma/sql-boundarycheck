-- name: CTECrossBC :many
-- Cross-BC via CTE: cte_test context touches patients (patient) and appointments (appointment).
WITH patient_appts AS (
    SELECT a.appointment_id, p.patient_id
    FROM patients p
    JOIN appointments a ON a.patient_id = p.patient_id
)
SELECT * FROM patient_appts;
