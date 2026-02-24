-- name: LookupHospitalByName :one
SELECT hospital_id FROM ref.hospitals WHERE hospital_name = sqlc.arg(hospital_name) LIMIT 1;
