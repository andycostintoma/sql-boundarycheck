-- name: GetClinic :one
SELECT clinic_id FROM clinics WHERE clinic_id = $1;

-- name: ListClinicServices :many
SELECT service_id, clinic_id FROM clinic_services WHERE clinic_id = $1;
