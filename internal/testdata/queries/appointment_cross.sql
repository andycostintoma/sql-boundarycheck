-- name: GetAppointmentWithClinic :one
-- Cross-BC: appointment_cross context touches "clinics" which belongs to clinic.
SELECT a.appointment_id, c.name
FROM appointments a
JOIN clinics c ON c.clinic_id = a.clinic_id
WHERE a.appointment_id = $1;
