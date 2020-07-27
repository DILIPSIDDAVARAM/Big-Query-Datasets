#!/usr/bin/env python
# coding: utf-8
"""Importing python libraries"""
import json
import argparse
import concurrent.futures
from concurrent import futures
import gcsfs
import warnings

warnings.filterwarnings("ignore")

"""Importing google-cloud libraries"""
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def cmd_args_parser():
    """Parsing command-line arguments"""
    parser = argparse.ArgumentParser(
        prog="TableImport",
        description="Imports tables from google-cloud-storage to google-bigquery",
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
        help="Provide the restore configuration file path.",
        required=True,
    )
    parser.add_argument(
        "--retention",
        type=str.split,
        action="store",
        dest="retention",
        help="""
        Provide retention period for storing the restore.
        For multiple, use whitespace as delimiter.
        Follows lower case-sensitivity.
        Available options are daily, monthly, weekly, yearly.
        """,
        required=True,
    )
    parser.add_argument(
        "--date",
        type=str.split,
        action="store",
        dest="date",
        help="Provide date for importing table. Supported format: YYYY-mm-dd",
        required=True,
    )
    parser.add_argument(
        "--restore_type",
        type=str.split,
        action="store",
        dest="restore_type",
        help="""
        Provide restore_type for restoring the restore.
        For multiple, use whitespace as delimiter.
        Follows lower case-sensitivity.
        Available options are all, config.
        """,
        required=True,
    )
    args = parser.parse_args()
    cmdargs = {}
    cmdargs["project_id"] = args.project_id
    cmdargs["config_file"] = args.config_file
    cmdargs["retention"] = args.retention
    cmdargs["restore_type"] = args.restore_type
    cmdargs["date"] = args.date

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


def read_json_schema(gcs_schema_path):
    """Reads JSON content from GCS file and returns."""
    try:
        fs = gcsfs.GCSFileSystem()
        with fs.open(gcs_schema_path, "r") as f:
            schema = json.loads(f.read())
        return schema
    except Exception as error:
        print(
            "Exception occurred at function {} inside export-loop: {}".format(
                "read_json_schema", error
            )
        )
        return None


def load_table_from_gcs(job_params):
    """
    Loads bigquery table from google-cloud-storage.
    :param client: Big Query Client (type:google.cloud.bigquery.client.Client)
    :param full_table_id: Project_Id.Dataset_Id.Table_Id (type:str)
    :param gcs_table_path: Table Data Uri (type:str)
    :param json_schema: Table Json Schema (type:dict)
    :param location: Table-Data File Location (type:str)
    :return success_criteria: {0:success, 1:fail}
    """
    client = job_params.get("client")
    full_table_id = job_params.get("full_table_id")
    gcs_table_path = job_params.get("gcs_table_path")
    json_schema = job_params.get("json_schema", {})
    location = job_params.get("location")
    try:
        load_job_config = bigquery.LoadJobConfig()
        load_job_config.schema = [
            bigquery.SchemaField(
                schema.get("name"), schema.get("type"), schema.get("mode")
            )
            for schema in json_schema
        ]
        load_job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        load_job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        load_job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        load_job = client.load_table_from_uri(
            gcs_table_path, full_table_id, location=location, job_config=load_job_config
        )
        load_job.result()
        destination_table = client.get_table(full_table_id)
        print("Loaded {} rows successfully.".format(destination_table.num_rows))
        success_criteria = 0
    except Exception as error:
        print(
            "Exception occurred at function {} inside export-loop: {}".format(
                "load_table_from_gcs", error
            )
        )
        success_criteria = 1
    finally:
        return success_criteria


def main_process_function(project_id, config_file, retention, restore_type, date):
    """
    This is the main function for exporting the big-query datasets
    to google-cloud-storage.
    :param project_id: Google Cloud Project Id (type:str)
    :param config_file: Restore Configuration File Path (type:str)
    :param retention: Retention Type ["daily", "monthly", "weekly", "yearly"] (type:str)
    :param restore_type: Restore Type ["all", "config"] (type:str)
    :param date: restore Date. Supported format: YYYY-mm-dd (type:bool/str)
    :return NoneType:
    """
    print("Running bigquery dataset import for project:{}".format(project_id))
    # Reading restore-parameters from json config
    with open(config_file) as f:
        master_config = json.load(f)
    restore_config = master_config["restore"]

    location = restore_config["location"]
    schema_path = restore_config["schema_uri"]
    table_path = restore_config["table_uri"]
    project_restore_config = restore_config["projects_dict"][project_id]
    mapped_list = []
    timestamp = date

    # Creating Big Query Client
    client = bigquery.Client(project=project_id)

    # Getting mapped relation between datasets and their tables
    if restore_type == "all":
        # Get all datasets
        datasets = list_all_datasets(client=client)
        # Map dataset->[tables]
        dataset_tables_map = get_datasets_tables_dict(
            client=client, project_id=project_id, datasets=datasets
        )
        mapped_list.append(dataset_tables_map)
    elif restore_type == "config":
        # Extract the restore pattern from config
        restore_pattern = project_restore_config["restore_pattern"]
        for key, value in restore_pattern.items():
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
            "Please provide a valid restore_type option. Choose from ['all', 'config']"
        )
        return None

    # Performing dataset import to gcs (data, schema)
    if mapped_list:
        for datasets_tables_dict in mapped_list:
            for bq_dataset_name in datasets_tables_dict.keys():
                print("Restore Operation on dataset: {}".format(bq_dataset_name))
                for bq_table_name in datasets_tables_dict[bq_dataset_name]:
                    print("Restoring table: {}".format(bq_table_name))
                    try:
                        # Getting dataset and table objects
                        dataset_ref = bigquery.DatasetReference(
                            project_id, bq_dataset_name
                        )
                        table_ref = dataset_ref.table(bq_table_name)
                        # Check if table exists
                        try:
                            client.get_table(table_ref)
                            table_exists = True
                        except NotFound:
                            table_exists = False
                        # Defining Load Job Parameters
                        job_params = {}
                        gcs_schema_path = schema_path.format(
                            bucket_name=project_restore_config["bucket_name"],
                            retention=retention,
                            dataset_name=bq_dataset_name,
                            timestamp=timestamp,
                            schema_file_name=bq_table_name + "-schema.json",
                        )
                        json_schema = read_json_schema(gcs_schema_path)
                        if not table_exists and json_schema is None:
                            print(
                                "Schema and table doesn't exist for {}. Skipping load job...".format(
                                    table_ref
                                )
                            )
                            pass
                        else:
                            gcs_table_path = table_path.format(
                                bucket_name=project_restore_config["bucket_name"],
                                retention=retention,
                                dataset_name=bq_dataset_name,
                                timestamp=timestamp,
                                table_file_name=bq_table_name + "-*.json",
                            )
                            job_params["gcs_table_path"] = gcs_table_path
                            job_params["json_schema"] = json_schema
                            job_params["location"] = location
                            job_params["full_table_id"] = table_ref
                            job_params["client"] = client
                            success_criteria = load_table_from_gcs(job_params)
                            if success_criteria == 0:
                                print(
                                    "Table load from google cloud storage successful."
                                )
                            else:
                                print(
                                    "Table load from google cloud storage not successful."
                                )
                            pass
                    except Exception as error:
                        print(
                            "Exception occurred for project {} at function {} inside export-loop: {}".format(
                                project_id, "main_process_function", error
                            )
                        )
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
    if len(cmdargs["restore_type"]) == 1:
        restore_type_param_list = cmdargs["restore_type"] * num_workers
    else:
        restore_type_param_list = cmdargs["restore_type"]
    if len(cmdargs["date"]) == 1:
        date_param_list = cmdargs["date"] * num_workers
    else:
        date_param_list = cmdargs["date"]
    # Starting Multi Processing
    process_call_count = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        try:
            for val in executor.map(
                main_process_function,
                project_id_list,
                config_file_param_list,
                retention_param_list,
                restore_type_param_list,
                date_param_list,
            ):
                process_call_count = process_call_count + 1
        except futures.process.BrokenProcessPool as e:
            print("Could not start new tasks: {}".format(e))

    print(
        "For projects: {}, total number of processes executed: {}".format(
            num_workers, process_call_count
        )
    )
