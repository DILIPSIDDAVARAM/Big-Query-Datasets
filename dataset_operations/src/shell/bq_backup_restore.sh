begin=$(date +%s)

echo "Big Query Transfer Config running..."
# Reading command-line arguments
project_id=${1}
target_dataset_id=${2}
source_dataset_id=${3}

# Big Query Transfer Config
bq mk --transfer_config --project_id=${project_id} --data_source=cross_region_copy --target_dataset=${target_dataset_id} --display_name='Data Movement' --params='{"source_dataset_id": "'"${source_dataset_id}"'", "source_project_id": "'"${project_id}"'", "overwrite_destination_table": "true"}'

end=$(date +%s)
tottime=$(expr $end - $begin)
echo "Dataset copy completed in $tottime seconds"
