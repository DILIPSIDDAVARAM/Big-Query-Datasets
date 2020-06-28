#!/usr/bin/env python
# coding: utf-8
"""Importing Libraries"""
import json
import os
import sys
import subprocess
from google.cloud import bigquery
from google.oauth2 import service_account
import warnings

warnings.filterwarnings("ignore")


def list_all_datasets(bq_client=None):
    """
    Returns a list of datasets in the project.
    :param bq_client: Bigquery Client (type:google.cloud.bigquery.client.Client)
    :return datasets: List of Datasets(type:list)
    """
    datasets = []
    datasets_list = list(bq_client.list_datasets())
    if datasets_list:
        for dataset in datasets_list:
            datasets.append(dataset.dataset_id)
    datasets = [
        dataset
        for dataset in datasets
        if "_bkp" not in dataset.lower() and "_backup" not in dataset.lower()
    ]
    return datasets


def create_datasets(project_id=None, bq_client=None, datasets=None):
    """
    Creates datasets in Big Query.
    :param bq_client: Target Big Query Client (type:google.cloud.bigquery.client.Client)
    :param datasets: List of Datasets (type:list)
    :return dataset_creation_flag: (type:int)
    """
    dataset_creation_flag = 0
    try:
        for dataset_id in datasets:
            if "_bkp" not in dataset_id.lower() and "_backup" not in dataset_id.lower():
                dataset_id = project_id + "." + dataset_id
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = "US"
                dataset = bq_client.create_dataset(dataset)
    except Exception as e:
        print("Exception occurred: {}".format(e))
        dataset_creation_flag = dataset_creation_flag + 1
    finally:
        return dataset_creation_flag


class Dataset_Copy:
    def __init__(self):
        pass

    @staticmethod
    def main(sanity_check=None, source_project_id=None, target_project_id=None):
        """
        Copies datasets from source_project_id to target_project_id
        :param sanity_check: True/False (type:str, bool)
        :param source_project_id: (type:str)
        :param target_project_id: (type:str)
        """
        if str(sanity_check).title() == "True":
            # Reading parameters
            source_project_id = str(source_project_id)
            target_project_id = str(target_project_id)
            # Getting service account path from environment variable
            service_account_key_file = os.getenv("SERVICE_ACCOUNT_PATH")
            # Setting auth for GCP from service account
            with open(service_account_key_file) as source:
                info = json.load(source)
            bq_credentials = service_account.Credentials.from_service_account_info(info)
            # Creating Big Query Clients
            source_client = bigquery.Client(
                project=source_project_id, credentials=bq_credentials
            )
            target_client = bigquery.Client(
                project=target_project_id, credentials=bq_credentials
            )
            # Get list of datasets for source and target project
            source_datasets = list_all_datasets(bq_client=source_client)
            target_datasets = list_all_datasets(bq_client=target_client)
            # Get missing datasets list in target project
            missing_datasets = list(set(source_datasets) - set(target_datasets))
            if missing_datasets:
                dataset_creation_flag = create_datasets(
                    project_id=target_project_id,
                    bq_client=target_client,
                    datasets=missing_datasets,
                )
            else:
                dataset_creation_flag = 0
            # Create dict of datasets {src:tgt}
            if dataset_creation_flag == 0:
                copy_datasets = target_datasets + missing_datasets
                source_datasets.sort()
                copy_datasets.sort()
                copy_datasets_dict = dict(zip(source_datasets, copy_datasets))
                # Switch to User Account
                user_account = os.getenv("BQ_TRANSFER_USER_ACCOUNT")
                os.system("gcloud config set account {}".format(user_account))
                # Dataset Copy From Source Project To Target Project
                for source_dataset_id, target_dataset_id in copy_datasets_dict.items():
                    try:
                        if (
                            "_bkp" not in source_dataset_id.lower()
                            and "_backup" not in source_dataset_id.lower()
                        ):
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
                                    os.path.abspath("bq_copy.sh")
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
                                print("Success")
                            else:
                                print("Failed")
                    except Exception as e:
                        print(e)
                # Switch to Default Account
                default_account = os.getenv("DEFAULT_SERVICE_ACCOUNT")
                os.system("gcloud config set account {}".format(default_account))


if __name__ == "__main__":
    if len(sys.argv) == 4:
        sanity_check = sys.argv[1]
        source_project_id = sys.argv[2]
        target_project_id = sys.argv[3]
        Dataset_Copy().main(
            sanity_check=sanity_check,
            source_project_id=source_project_id,
            target_project_id=target_project_id,
        )
    else:
        print("Enter valid number of arguments!")
