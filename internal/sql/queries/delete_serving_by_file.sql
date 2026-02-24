-- name: DeleteServingByFile :exec
DELETE FROM mrf.prices_by_code WHERE mrf_file_id = sqlc.arg(mrf_file_id);
