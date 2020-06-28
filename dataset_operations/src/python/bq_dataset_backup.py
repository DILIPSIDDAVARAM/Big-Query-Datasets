#!/usr/bin/env python
# coding: utf-8
"""Importing Libraries"""
import json
import itertools as it
from datetime import datetime
import os
import sys
import subprocess
from google.cloud import bigquery
from google.oauth2 import service_account
import warnings

warnings.filterwarnings("ignore")


def list_all_datasets(bq_client=None, bkp_flag=None):
    """
    Returns a list of datasets in the project.
    :param bq_client: Bigquery Client (type:google.cloud.bigquery.client.Client)
    :param bkp_flag: {1: Backup, 0: Non-Backup} (type:int)
    :return datasets: List of Datasets(type:list)
    """
    datasets = []
    datasets_list = list(bq_client.list_datasets())
    if datasets_list:
        for dataset in datasets_list:
            datasets.append(dataset.dataset_id)
    if bkp_flag == 0:
        # Select Non-Backup Datasets
        datasets = [
            dataset
            for dataset in datasets
            if "_bkp" not in dataset.lower() and "_backup" not in dataset.lower()
        ]
    elif bkp_flag == 1:
        # Select Current Date Backup Datasets
        today = datetime.now().strftime("%Y_%m_%d")
        datasets = [
            dataset
            for dataset in datasets
            if today in dataset.lower() and "_bkp" in dataset.lower()
        ]
    else:
        print("Provide valid bkp_flag.")
    datasets.sort()
    return datasets


def create_datasets(project_id=None, bq_client=None, datasets=None):
    """
    Creates datasets in Big Query.
    :param bq_client: Target Big Query Client (type:google.cloud.bigquery.client.Client)
    :param datasets: List of Datasets (type:list)
    :return dataset_creation_flag, created_datasets: (type:int, list)
    """
    dataset_creation_flag = 0
    created_datasets = []
    try:
        for dataset_id in datasets:
            if "_bkp" not in dataset_id.lower() and "_backup" not in dataset_id.lower():
                today = datetime.now().strftime("%Y_%m_%d")
                dataset_id = dataset_id + "_" + today + "_bkp"
                created_datasets.append(dataset_id)
                dataset_id = "{}.{}".format(project_id, dataset_id)
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = "US"
                dataset = bq_client.create_dataset(dataset)
    except Exception as e:
        print("Exception occurred: {}".format(e))
        dataset_creation_flag = dataset_creation_flag + 1
    finally:
        return dataset_creation_flag, created_datasets


class Dataset_Backup:
    def __init__(self):
        pass

    @staticmethod
    def main(sanity_check=None, project_id=None):
        """
        Creates datasets backup copies.
        :param sanity_check: True/False (type:str, bool)
        :param project_id: (type:str)
        """
        if str(sanity_check).title() == "True":
            # Getting service account path from environment variable
            service_account_key_file = os.getenv("SERVICE_ACCOUNT_PATH")
            # Setting auth for GCP from service account
            with open(service_account_key_file) as source:
                info = json.load(source)
            bq_credentials = service_account.Credentials.from_service_account_info(info)
            # Creating Big Query Client
            bq_client = bigquery.Client(project=project_id, credentials=bq_credentials)
            # Get list of backup & non-backup datasets for project
            src_non_bkp_datasets = list_all_datasets(bq_client=bq_client, bkp_flag=0)
            tgt_bkp_datasets = list_all_datasets(bq_client=bq_client, bkp_flag=1)
            # Create dict of datasets {non_bkp:bkp}
            combo_dict = dict(it.zip_longest(src_non_bkp_datasets, tgt_bkp_datasets))
            # Get missing datasets list in target project
            missing_datasets = [
                src for src, tgt in combo_dict.items() if str(src) not in str(tgt)
            ]
            if missing_datasets:
                dataset_creation_flag, created_datasets = create_datasets(
                    project_id=project_id,
                    bq_client=bq_client,
                    datasets=missing_datasets,
                )
            else:
                dataset_creation_flag = 0
            # Create dict of datasets {non_bkp:bkp}
            if dataset_creation_flag == 0:
                copy_datasets = tgt_bkp_datasets + created_datasets
                src_non_bkp_datasets.sort()
                copy_datasets.sort()
                copy_datasets_dict = dict(zip(src_non_bkp_datasets, copy_datasets))
                # Switch to User Account
                user_account = os.getenv("BQ_TRANSFER_USER_ACCOUNT")
                os.system("gcloud config set account {}".format(user_account))
                # Backing up dataset from original_dataset to backup_dataset
                for source_dataset_id, target_dataset_id in copy_datasets_dict.items():
                    try:
                        print(
                            "Creating backup from {}.{} to {}.{}".format(
                                project_id,
                                source_dataset_id,
                                project_id,
                                target_dataset_id,
                            )
                        )
                        process = subprocess.call(
                            [
                                os.path.abspath("bq_backup_restore.sh")
                                + " "
                                + project_id
                                + " "
                                + target_dataset_id
                                + " "
                                + source_dataset_id
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
    if len(sys.argv) == 3:
        sanity_check = sys.argv[1]
        project_id = sys.argv[2]
        Dataset_Backup().main(
            sanity_check=sanity_check, project_id=project_id,
        )
    else:
        print("Enter valid number of arguments!")
