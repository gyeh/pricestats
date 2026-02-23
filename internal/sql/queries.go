package sql

import (
	_ "embed"
)

//go:embed queries/resolve_hospital.sql
var ResolveHospital string

//go:embed queries/register_mrf_file.sql
var RegisterMRFFile string

//go:embed queries/update_mrf_status.sql
var UpdateMRFStatus string

//go:embed queries/upsert_payers.sql
var UpsertPayers string

//go:embed queries/upsert_plans.sql
var UpsertPlans string

//go:embed queries/transform_wide_to_long.sql
var TransformWideToLong string

//go:embed queries/delete_staging_batch.sql
var DeleteStagingBatch string

//go:embed queries/deactivate_older_versions.sql
var DeactivateOlderVersions string

//go:embed queries/activate_version.sql
var ActivateVersion string

//go:embed queries/analyze_partitions.sql
var AnalyzePartitions string
