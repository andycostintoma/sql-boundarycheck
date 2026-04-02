-- name: GetClinic :one
SELECT clinic_id, name
FROM clinics
WHERE clinic_id = $1;

-- name: ListClinicServices :many
SELECT service_id, clinic_id, name
FROM clinic_services
WHERE clinic_id = $1;
