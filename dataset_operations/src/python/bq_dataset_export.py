#!/usr/bin/env python
# coding: utf-8
"""Importing python libraries"""
import os
import json
import argparse
import concurrent.futures
from concurrent import futures
from datetime import datetime, timedelta
import gcsfs
import warnings

warnings.filterwarnings("ignore")
"""Importing google-cloud libraries"""
from google.cloud import bigquery
from google.cloud import storage


def cmd_args_parser():
    """Parsing command-line arguments"""
    parser = argparse.ArgumentParser(
        prog="DatasetExport", description="Exports datasets to google-cloud-storage",
    )
    parser.add_argument(
        "--project_id",
        type=str.split,
        action="store",
        dest="project_id",
        help="""
        Provide the project-id.
        For multiple, use whitespace as delimiter.
        """,
        required=True,
    )
    parser.add_argument(
        "--config_file",
        type=str,
        action="store",
        dest="config_file",
        help="Provide the backup configuration file path.",
        required=True,
    )
    parser.add_argument(
        "--retention",
        type=str.split,
        action="store",
        dest="retention",
        help="""
        Provide retention period for storing the backup.
        For multiple, use whitespace as delimiter.
        Follows lower case-sensitivity.
        Available options are daily, monthly, weekly, yearly.
        """,
        required=True,
    )
    parser.add_argument(
        "--backup_type",
        type=str.split,
        action="store",
        dest="backup_type",
        help="""
        Provide backup_type for storing the backup.
        For multiple, use whitespace as delimiter.
        Follows lower case-sensitivity.
        Available options are all, config.
        """,
        required=True,
    )
    parser.add_argument(
        "--expiration",
        type=str.split,
        action="store",
        dest="expiration",
        help="""
        Provide expiration for deleting the backup.
        For multiple, use whitespace as delimiter.
        Follows title case-sensitivity.
        Available options are True, False.
        """,
        required=True,
    )
    args = parser.parse_args()
    cmdargs = {}
    cmdargs["project_id"] = args.project_id
    cmdargs["config_file"] = args.config_file
    cmdargs["retention"] = args.retention
    cmdargs["backup_type"] = args.backup_type
    cmdargs["expiration"] = args.expiration
    return cmdargs


def list_all_datasets(client=None):
    """
    Returns a list of datasets in the project.
    :param client: Bigquery Client (type:google.cloud.bigquery.client.Client)
    :return datasets: List of Datasets(type:list)
    """
    datasets = []
    try:
        datasets_list = list(client.list_datasets())
        if datasets_list:
            for dataset in datasets_list:
                datasets.append(dataset.dataset_id)
    except Exception as error:
        print(
            "Exception occurred at function {}: {}".format("list_all_datasets", error)
        )
    finally:
        return datasets


def get_datasets_tables_dict(client=None, project_id=None, datasets=None):
    """
    Returns a dictionary of datasets and their tables.
    :param client: Bigquery Client (type:google.cloud.bigquery.client.Client)
    :param project_id: Google Cloud Project-Id (type:str)
    :param datasets: List of dataset-ids (type:list)
    :return dataset_tables_dict: {dataset_id:[tables_list]} (type:dict)
    """
    try:
        datasets_tables_dict = {}
        for dataset in datasets:
            tables_list = []
            dataset_id = "{}.{}".format(project_id, dataset)
            tables = client.list_tables(dataset_id)
            for table in tables:
                tables_list.append(table.table_id)
            datasets_tables_dict[dataset] = tables_list
    except Exception as error:
        print(
            "Exception occurred at function {}: {}".format(
                "get_datasets_tables_dict", error
            )
        )
    finally:
        return datasets_tables_dict


def main_process_function(project_id, config_file, retention, backup_type, expiration):
    """
    This is the main function for exporting the big-query datasets
    to google-cloud-storage.
    :param project_id: Google Cloud Project Id (type:str)
    :param config_file: Backup Configuration File Path (type:str)
    :param retention: Retention Type ["daily", "monthly", "weekly", "yearly"] (type:str)
    :param backup_type: Backup Type ["all", "config"] (type:str)
    :param expiration: True/False (type:bool/str)
    :return NoneType:
    """
    print("Running bigquery dataset export for project:{}".format(project_id))
    # Reading backup-parameters from json config
    with open(config_file) as f:
        master_config = json.load(f)
    backup_config = master_config["backup"]

    location = backup_config["location"]
    schema_path = backup_config["schema_uri"]
    table_path = backup_config["table_uri"]
    project_backup_config = backup_config["projects_dict"][project_id]
    mapped_list = []

    # Get timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d")

    # Creating Big Query Client
    client = bigquery.Client(project=project_id)

    # Getting mapped relation between datasets and their tables
    if backup_type == "all":
        # Get all datasets
        datasets = list_all_datasets(client=client)
        # Map dataset->[tables]
        dataset_tables_map = get_datasets_tables_dict(
            client=client, project_id=project_id, datasets=datasets
        )
        mapped_list.append(dataset_tables_map)
    elif backup_type == "config":
        # Extract the backup pattern from config
        backup_pattern = project_backup_config["backup_pattern"]
        for key, value in backup_pattern.items():
            dataset_tables_map = {}
            if value == "all":
                # Map dataset->[tables]
                dataset_tables_map = get_datasets_tables_dict(
                    client=client, project_id=project_id, datasets=[key]
                )
                mapped_list.append(dataset_tables_map)
            else:
                # Map dataset->[tables]
                dataset_tables_map[key] = value
                mapped_list.append(dataset_tables_map)
    else:
        print(
            "Please provide a valid backup_type option. Choose from ['all', 'config']"
        )
        return None

    # Performing dataset export to gcs (data, schema)
    if mapped_list:
        for datasets_tables_dict in mapped_list:
            for bq_dataset_name in datasets_tables_dict.keys():
                print("Backup Operation on dataset: {}".format(bq_dataset_name))
                for bq_table_name in datasets_tables_dict[bq_dataset_name]:
                    print("Backing up table: {}".format(bq_table_name))
                    try:
                        # Getting dataset and table objects
                        dataset_ref = bigquery.DatasetReference(
                            project_id, bq_dataset_name
                        )
                        table_ref = dataset_ref.table(bq_table_name)
                        table_obj = client.get_table(table_ref)

                        # Specifying extract-job parameters
                        gcs_table_path = table_path.format(
                            bucket_name=project_backup_config["bucket_name"],
                            retention=retention,
                            dataset_name=bq_dataset_name,
                            timestamp=timestamp,
                            table_file_name=bq_table_name + "-*.json",
                        )
                        job_config = bigquery.ExtractJobConfig()
                        job_config.compression = bigquery.Compression.GZIP
                        job_config.destination_format = (
                            bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
                        )

                        # Exporting table-data to gcs
                        extract_job = client.extract_table(
                            table_ref,
                            gcs_table_path,
                            job_config=job_config,
                            location=location,
                        )
                        extract_job.result()

                        # Extracting table-schema
                        table_schema = table_obj.schema
                        table_schema = [
                            {
                                "name": item.name,
                                "mode": item.mode,
                                "type": item.field_type,
                            }
                            for item in table_schema
                        ]
                        json_schema = json.dumps(table_schema)

                        # Defining schema-path
                        gcs_schema_path = schema_path.format(
                            bucket_name=project_backup_config["bucket_name"],
                            retention=retention,
                            dataset_name=bq_dataset_name,
                            timestamp=timestamp,
                            schema_file_name=bq_table_name + "-schema.json",
                        )

                        # Writing table-schema to gcs
                        sa_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                        fs = gcsfs.GCSFileSystem(
                            project=project_id, token=sa_credentials
                        )
                        with fs.open(
                            gcs_schema_path,
                            "w",
                            metadata={"Content-Type": "application/json"},
                        ) as f:
                            f.write(json_schema)
                    except Exception as error:
                        print(
                            "Exception occurred for project {} at function {} inside export-loop: {}".format(
                                project_id, "main_process_function", error
                            )
                        )
                # Deleting backup data based on the backup_data_policy
                backup_data_policy = {
                    "daily": 1,
                    "weekly": 7,
                    "monthly": 30,
                    "yearly": 365,
                }
                if str(expiration).title() == "True":
                    try:
                        bucket_name = project_backup_config["bucket_name"]
                        storage_client = storage.Client(project_id)
                        client_bucket = storage_client.get_bucket(bucket_name)
                        delete_date = (
                            datetime.now()
                            - timedelta(days=backup_data_policy[retention])
                        ).strftime("%Y-%m-%d")
                        delete_path = "{retention}/{dataset_name}/{timestamp}".format(
                            retention=retention,
                            dataset_name=bq_dataset_name,
                            timestamp=delete_date,
                        )
                        for file in client_bucket.list_blobs(prefix=delete_path):
                            file.delete()
                            print("Deleted '{}'".format(file.name))
                    except Exception as error:
                        print(
                            "Exception occurred at function {} inside expiration-loop: {}".format(
                                "main_process_function", error
                            )
                        )
                else:
                    pass
        return None
    else:
        print("The mapping between datasets and their tables is empty.")
        return None


if __name__ == "__main__":
    # Reading cmd args
    cmdargs = cmd_args_parser()

    # Preparing params for multi-processing

    project_id_list = cmdargs["project_id"]
    # Define number of workers
    num_workers = len(project_id_list)
    # Defining other positional params list
    config_file_param_list = [cmdargs["config_file"]] * num_workers
    if len(cmdargs["retention"]) == 1:
        retention_param_list = cmdargs["retention"] * num_workers
    else:
        retention_param_list = cmdargs["retention"]
    if len(cmdargs["backup_type"]) == 1:
        backup_type_param_list = cmdargs["backup_type"] * num_workers
    else:
        backup_type_param_list = cmdargs["backup_type"]
    if len(cmdargs["expiration"]) == 1:
        expiration_param_list = cmdargs["expiration"] * num_workers
    else:
        expiration_param_list = cmdargs["expiration"]

    # Starting Multi Processing
    process_call_count = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        try:
            for val in executor.map(
                main_process_function,
                project_id_list,
                config_file_param_list,
                retention_param_list,
                backup_type_param_list,
                expiration_param_list,
            ):
                process_call_count = process_call_count + 1
        except futures.process.BrokenProcessPool as e:
            print("Could not start new tasks: {}".format(e))

    print(
        "For clients: {}, total number of processes executed: {}".format(
            num_workers, process_call_count
        )
    )
