#!/usr/bin/env python
# coding: utf-8
"""Importing python libraries"""
import argparse
import json
import os
import subprocess

"""Importing google-cloud packages"""
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from google.cloud import secretmanager


def cmd_args_parser():
    """Parsing command-line arguments"""
    parser = argparse.ArgumentParser(
        prog="DataMovement",
        description="Copies bigquery datasets/tables across regions",
    )
    parser.add_argument(
        "--config_file",
        type=str,
        action="store",
        dest="config_file",
        help="Provide the migration configuration file path.",
        required=True,
    )
    parser.add_argument(
        "--type",
        type=str.lower,
        action="store",
        choices=["datasets", "tables"],
        dest="type",
        help="Choose migration-type from available choices.",
        required=True,
    )
    args = parser.parse_args()
    cmdargs = {}
    # Define param_key -> param_value pairs
    cmdargs["config_file"] = args.config_file
    cmdargs["type"] = args.type

    return cmdargs


def access_secret_version(project_id, secret_id, version_id="latest"):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = client.secret_version_path(project_id, secret_id, version_id)

    # Access the secret version.
    response = client.access_secret_version(name)

    payload = response.payload.data.decode("UTF-8")
    secret_key = json.loads(payload)

    return secret_key


def check_dataset_exists(dataset_ref=None, client=None):
    """Returns True if dataset exists. Else returns False"""
    try:
        client.get_dataset(dataset_ref)
        return True
    except NotFound:
        return False
    except Exception as error:
        print(
            "Exception occurred at {} function: {}".format(
                "check_dataset_exists", error
            )
        )
        return False


def create_datasets(bq_client=None, datasets=None):
    """
    Creates datasets in Big Query.
    :param bq_client: Target Big Query Client (type:google.cloud.bigquery.client.Client)
    :param datasets: List of Datasets (type:list)
    :return success_criterion: (type:int)
    """
    success_criterion = 0
    try:
        for dataset_id in datasets:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset)
            success_criterion = 0
    except Exception as error:
        print("Exception occurred at {} function: {}".format("create_datasets", error))
        success_criterion = success_criterion + 1
    finally:
        return success_criterion


def bq_copy_table(
    source_client=None,
    source_project_id=None,
    target_project_id=None,
    dataset_table_list=None,
):
    """
    Copying custom project.dataset.table between two projects.
    :param source_project_id: GCP Project-Id (type:str)
    :param target_project_id: GCP Project-Id (type:str)
    :param dataset_table_list: [dataset_id.table_id] (type:list)
    :return table_transfer_criterion {0:SUCCESS, 1:FAIL}.
    """
    table_transfer_criterion = 0
    # Table Copy From Source Project To Target Project
    for table_id in dataset_table_list:
        try:
            source_table_id = "{}.{}".format(source_project_id, table_id)
            destination_table_id = "{}.{}".format(target_project_id, table_id)
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
            job = source_client.copy_table(
                source_table_id,
                destination_table_id,
                location="US",
                job_config=job_config,
            )
            job.result()
            print(
                "Copy successful from {} to {}".format(
                    source_table_id, destination_table_id
                )
            )
            table_transfer_criterion = 0
        except Exception as error:
            print(
                "Exception occurred for {} at function {}: {}".format(
                    table_id, "bq_copy_table", error
                )
            )
            table_transfer_criterion = table_transfer_criterion + 1
    return table_transfer_criterion


def bq_copy_dataset(
    shell_script=None,
    source_project_id=None,
    target_project_id=None,
    copy_datasets_dict=None,
):
    """
    Copying project.dataset between two projects.
    :param source_project_id: GCP Project-Id (type:str)
    :param target_project_id: GCP Project-Id (type:str)
    :param copy_datasets_dict: {source_dataset_id:target_dataset_id} (type:dict)
    :return dataset_transfer_criterion {0:SUCCESS, 1:FAIL}.
    """
    dataset_transfer_criterion = 0
    # Dataset Copy From Source Project To Target Project
    for source_dataset_id, target_dataset_id in copy_datasets_dict.items():
        try:
            print(
                "Copying {}.{} to {}.{}".format(
                    source_project_id,
                    source_dataset_id,
                    target_project_id,
                    target_dataset_id,
                )
            )
            process = subprocess.call(
                [
                    shell_script
                    + " "
                    + target_project_id
                    + " "
                    + target_dataset_id
                    + " "
                    + source_dataset_id
                    + " "
                    + source_project_id
                ],
                shell=True,
            )
            if process == 0:
                dataset_transfer_criterion = 0
                print("Success")
            else:
                dataset_transfer_criterion = dataset_transfer_criterion + 1
                print("Failed")
        except Exception as error:
            print(
                "Exception occurred for {} at function {} : {}".format(
                    source_dataset_id, "bq_copy_dataset", error
                )
            )
            dataset_transfer_criterion = dataset_transfer_criterion + 1
    return dataset_transfer_criterion


class Data_Movement:
    def __init__(self):
        pass

    @staticmethod
    def main(config_file=None, type=None):
        """
        Copies datasets/tables from source_project_id to target_project_id
        :param config_file: Configuration file path(type:str)
        :param type: Data Transfer Type [datasets, tables] (type:str)
        """
        try:
            # Reading migration-parameters from json config
            with open(config_file) as f:
                master_config = json.load(f)
            config = master_config["migration"]

            # Getting service account json-content from secrets manager
            service_account_credentials = access_secret_version(
                config["secret_manager_project"], config["secret_key"]
            )

            # Setting auth for GCP from service account
            bq_credentials = service_account.Credentials.from_service_account_info(
                service_account_credentials
            )

            # Checking the data-transfer type
            dataset_table_list, datasets_list = {}, []
            if type == "tables":
                dataset_table_list = config[type]
            elif type == "datasets":
                datasets_list = config[type]
            else:
                print(
                    "Provide valid set of parameters. Transfer type supported tables, datasets."
                )
                return None

            # Creating Big Query Clients
            source_client = bigquery.Client(
                project=config["source_project_id"], credentials=bq_credentials
            )
            target_client = bigquery.Client(
                project=config["target_project_id"], credentials=bq_credentials
            )

            # Table-level data transfer
            if dataset_table_list:
                datasets = list(set([x.split(".")[0] for x in dataset_table_list]))
                missing_datasets = []
                for dataset_id in datasets:
                    dataset_ref = "{}.{}".format(
                        config["target_project_id"], dataset_id
                    )
                    dataset_exists = check_dataset_exists(
                        dataset_ref=dataset_ref, client=target_client
                    )
                    if not dataset_exists:
                        missing_datasets.append(dataset_ref)
                    else:
                        pass
                if missing_datasets:
                    success_criterion = create_datasets(
                        bq_client=target_client, datasets=missing_datasets,
                    )
                else:
                    success_criterion = 0
                if success_criterion == 0:
                    table_transfer_criterion = 0
                    table_transfer_criterion = bq_copy_table(
                        source_client=source_client,
                        source_project_id=config["source_project_id"],
                        target_project_id=config["target_project_id"],
                        dataset_table_list=dataset_table_list,
                    )
                else:
                    table_transfer_criterion = 1
            else:
                table_transfer_criterion = 0

            # Dataset-level data transfer
            if datasets_list:
                missing_datasets = []
                for dataset_id in datasets_list:
                    dataset_ref = "{}.{}".format(
                        config["target_project_id"], dataset_id
                    )
                    dataset_exists = check_dataset_exists(
                        dataset_ref=dataset_ref, client=target_client
                    )
                    if not dataset_exists:
                        missing_datasets.append(dataset_ref)
                    else:
                        pass
                if missing_datasets:
                    success_criterion = create_datasets(
                        bq_client=target_client, datasets=missing_datasets,
                    )
                else:
                    success_criterion = 0
                if success_criterion == 0:
                    data_sources = {}
                    for dataset in datasets_list:
                        data_sources[dataset] = dataset
                    # Switch to User Account
                    print("Switching to user-level account...")
                    os.system(
                        "gcloud config set account {}".format(
                            config["bq_data_transfer_account"]
                        )
                    )
                    dataset_transfer_criterion = 0
                    dataset_transfer_criterion = bq_copy_dataset(
                        shell_script=config["copy_script"],
                        source_project_id=config["source_project_id"],
                        target_project_id=config["target_project_id"],
                        copy_datasets_dict=data_sources,
                    )
                    # Switch to Default Account
                    print("Switching to default service-account...")
                    os.system(
                        "gcloud config set account {}".format(config["default_account"])
                    )
                else:
                    dataset_transfer_criterion = 1
            else:
                dataset_transfer_criterion = 0

            if dataset_transfer_criterion == 0 and table_transfer_criterion == 0:
                print("Big Query Data Transfer successful.")
                return
            else:
                print("Big Query Data Transfer not successful.")
                return
        except Exception as error:
            print("Exception occurred for at function {} : {}".format("main", error))


if __name__ == "__main__":
    # Reading command line arguments
    cmdargs = cmd_args_parser()
    config_file = cmdargs["config_file"]
    type = cmdargs["type"]
    # Main Function Call
    Data_Movement().main(
        config_file=config_file, type=type,
    )
