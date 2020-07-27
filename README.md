# Project Title

Big-Query-Datasets

## Getting Started
The repository contains modules developed in Python environment collaborated with Google Cloud SDK to manage the Google BigQuery datasets.

### Prerequisites
1. Python 3.6 and higher
2. Python - PIP Installer
3. Google Cloud SDK
(Must Installed Packages)
```
google-cloud-bigquery              1.17.1
google-cloud-bigquery-datatransfer 1.0.0
google-cloud-storage               1.28.1
google-cloud-secret-manager        1.0.0
```
4. Google Cloud Project (s)
5. Store Service Account JSON Key with following permissions in Secret Manager.
```
BigQuery Editor
```
6. User Account with following permissions
```
BigQuery Admin (includes bigquery.transfers.update permission)
```
7. Compute Engine Service Account with following permissions
```
Big Query Admin
Storage Admin
Secret Manager Admin
```
8. Environment Variables
```
GOOGLE_APPLICATION_CREDENTIALS = /path/to/compute-engine-service-account/key.json
```

### Installing
1. A step by step guide from Google Cloud Documentation for [Cloud SDK Setup](https://cloud.google.com/sdk/docs/how-to). 
2. A step by step guide from Google Cloud Documentation for [Creating and managing service accounts](https://cloud.google.com/iam/docs/creating-managing-service-accounts).
3. A step by step guide from Google Cloud Documentation for [Secret Manager Setup](https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets#secretmanager-create-secret-web).

## Deployment
1. [bq_dataset_create](dataset_operations/src/python/bq_dataset_create.py)
```
Test-Case #1: Single dataset creation
$ python bq_dataset_create.py --project_id certain-region-281614 --dataset_list test_dataset --location EU --description 'Created From Console' --default_table_expiration_ms 3600000 --labels '{"mode":"console", "type":"test"}'
Test-Case #2: Multiple datasets creation
$ python bq_dataset_create.py --project_id certain-region-281614 --dataset_list 'test_dataset_one test_dataset_two' --location EU --description 'Created From Console' --default_table_expiration_ms 3600000 --labels '{"mode":"console", "type":"test"}'
```
2. [bq_dataset_update](dataset_operations/src/python/bq_dataset_update.py)
```
Test-Case #1: Single dataset updation
$ python bq_dataset_update.py --project_id certain-region-281614 --dataset_list test_dataset --access_controls '{"role":"READER","entity_type":"userByEmail","entity_id":"mytrial98@gmail.com"}'
Test-Case #2: Multiple datasets updation with similar properties
$ python bq_dataset_update.py --project_id certain-region-281614 --dataset_list 'test_dataset_one test_dataset_two' --description 'Updated From Console' --default_table_expiration_ms 4000000 --labels '{"mode":"shell"}'
```
3. [bq_dataset_delete](dataset_operations/src/python/bq_dataset_delete.py)
```
Test-Case #1: Datasets deletion with keyword and filter = all (by default)
$ python bq_dataset_delete.py --project_id certain-region-281614 --delete_contents true --keyword properties
Test-Case #2: Datasets deletion with keyword and filter = choose 
$ python bq_dataset_delete.py --project_id certain-region-281614 --delete_contents true --dataset_list 'test_dataset_one test_dataset_two' --keyword one --filter choose
Test-Case #3: Datasets deletion with keyword and filter = ignore 
$ python bq_dataset_delete.py --project_id certain-region-281614 --delete_contents true --dataset_list 'test_dataset test_dataset_two' --keyword two --filter ignore
Test-Case #4: Datasets deletion with no keyword and filter = all (by default) 
$ python bq_dataset_delete.py --project_id certain-region-281614 --delete_contents true
```
4. [bq_dataset_info](dataset_operations/src/python/bq_dataset_info.py)
```
Test-Case #1: Getting information for single dataset
$ python bq_dataset_info.py --project_id unique-atom-251817 --datasets properties_uy
Test-Case #2: Getting information for multiple datasets
$ python bq_dataset_info.py --project_id unique-atom-251817 --datasets 'properties_ar,properties_co'
```
5. [bq_data_movement](dataset_operations/src/python/bq_data_movement.py)
```
Test-Case #1: Migrating datasets
$ python bq_data_movement.py --config_file dataset_operations/config/bigquery_config.json --type datasets
Test-Case #2: Migrating tables
$ python bq_data_movement.py --config_file dataset_operations/config/bigquery_config.json --type tables
```
6. [bq_dataset_export](dataset_operations/src/python/bq_dataset_export.py)
```
Test-Case #1: Backup of datasets for more than one project from the configuration-file with daily retention and expiration set to False
$ python bq_dataset_export.py --project_id 'project_id_one project_id_two' --config_file dataset_operations/config/bigquery_config.json --retention daily --backup_type config --expiration False
Test-Case #2: Backup of all datasets for one project with weekly retention and expiration set to True
$ python bq_dataset_export.py --project_id project_id_one --config_file dataset_operations/config/bigquery_config.json --retention weekly --backup_type all --expiration True
```
7. [bq_table_import](dataset_operations/src/python/bq_table_import.py)
```
Test-Case #1: Restore of datasets for more than one project from the configuration-file with daily retention
$ python bq_table_import.py --project_id 'project_id_one project_id_two' --config_file dataset_operations/config/bigquery_config.json --retention daily --restore_type config --date 2020-07-26
Test-Case #2: Restore of all datasets for one project with weekly retention
$ python bq_table_import.py --project_id project_id_one --config_file dataset_operations/config/bigquery_config.json --retention weekly --restore_type all --date 2019-07-26
```

## Authors

* **Dilip Siddavaram** - *Initial work* - [DILIPSIDDAVARAM](https://github.com/DILIPSIDDAVARAM)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE) file for details

