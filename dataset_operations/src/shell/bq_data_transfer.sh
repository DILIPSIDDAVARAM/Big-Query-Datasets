# Reading command-line arguments
target_project_id=${1}
target_dataset_id=${2}
source_dataset_id=${3}
source_project_id=${4}

# Big Query Transfer Config -> date --date "+1 hour" -u +%Y-%m-%dT%H:%M:%SZ
bq mk --transfer_config --project_id=${target_project_id} --data_source=cross_region_copy --target_dataset=${target_dataset_id} --display_name='Data Movement' --params='{"source_dataset_id": "'"${source_dataset_id}"'", "source_project_id": "'"${source_project_id}"'", "overwrite_destination_table": "true"}' --schedule_end_time="$(date --date "+30 minute" -u +%Y-%m-%dT%H:%M:%SZ)"
