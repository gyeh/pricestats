-- resolve_hospital.sql
-- Upserts a hospital by name and returns the hospital_id.
-- $1 = hospital_name, $2 = hospital_location, $3 = hospital_address,
-- $4 = license_number, $5 = license_state
INSERT INTO ref.hospitals (hospital_name, hospital_location, hospital_address, license_number, license_state)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING
RETURNING hospital_id;
