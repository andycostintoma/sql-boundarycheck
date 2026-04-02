-- name: GetAppointment :one
SELECT appointment_id, clinic_id, patient_id
FROM appointments
WHERE appointment_id = $1;
