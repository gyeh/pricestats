-- name: ResolveHospital :one
INSERT INTO ref.hospitals (hospital_name, hospital_location, hospital_address, license_number, license_state)
VALUES (sqlc.arg(hospital_name), sqlc.arg(hospital_location), sqlc.arg(hospital_address), sqlc.arg(license_number), sqlc.arg(license_state))
ON CONFLICT DO NOTHING
RETURNING hospital_id;
