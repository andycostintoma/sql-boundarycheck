-- name: GetAppointment :one
SELECT appointment_id FROM appointments WHERE appointment_id = $1;
