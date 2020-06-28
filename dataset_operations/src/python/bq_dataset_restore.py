#!/usr/bin/env python
# coding: utf-8
"""Importing Libraries"""
import json

# import itertools as it
# from datetime import datetime
import os
import sys
import subprocess
from google.cloud import bigquery
from google.oauth2 import service_account
import warnings

warnings.filterwarnings("ignore")


def list_all_datasets(bq_client=None, bkp_flag=None, date=None):
    """
    Returns a list of datasets in the project.
    :param bq_client: Bigquery Client (type:google.cloud.bigquery.client.Client)
    :param bkp_flag: {1: Backup, 0: Non-Backup} (type:int)
    :param date: Date to select datasets (type:str)
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
        # Select Given Date Backup Datasets
        datasets = [
            dataset
            for dataset in datasets
            if date in dataset.lower() and "_bkp" in dataset.lower()
        ]
    else:
        print("Provide valid bkp_flag.")
    datasets.sort()
    return datasets


class Dataset_Restore:
    def __init__(self):
        pass

    @staticmethod
    def main(sanity_check=None, project_id=None, date=None):
        """
        Restores datasets from backup copies.
        :param sanity_check: True/False (type:str, bool)
        :param project_id: (type:str)
        :param date: Date to choose backups for restore (type:str)
        """
        if str(sanity_check).title() == "True":
            # Getting service account path from environment variable
            service_account_key_file = os.getenv("SERVICE_ACCOUNT_PATH")
            # Setting auth for GCP from service account
            with open(service_account_key_file) as source:
                info = json.load(source)
            bq_credentials = service_account.Credentials.from_service_account_info(info)
            # Processing date parameter
            date = date.replace("-", "_")
            # Creating Big Query Client
            bq_client = bigquery.Client(project=project_id, credentials=bq_credentials)
            # Get list of backup & non-backup datasets for project
            src_non_bkp_datasets = list_all_datasets(bq_client=bq_client, bkp_flag=0)
            tgt_bkp_datasets = list_all_datasets(
                bq_client=bq_client, bkp_flag=1, date=date
            )
            # Create dict of datasets {bkp:non_bkp}
            copy_datasets_dict = dict(zip(tgt_bkp_datasets, src_non_bkp_datasets))
            # Switch to User Account
            user_account = os.getenv("BQ_TRANSFER_USER_ACCOUNT")
            os.system("gcloud config set account {}".format(user_account))
            # Restoring data from dataset_date_backup to original_dataset
            for source_dataset_id, target_dataset_id in copy_datasets_dict.items():
                try:
                    print(
                        "Restoring {}.{} to {}.{}".format(
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
    if len(sys.argv) == 4:
        sanity_check = sys.argv[1]
        project_id = sys.argv[2]
        date = sys.argv[3]
        Dataset_Restore().main(
            sanity_check=sanity_check, project_id=project_id, date=date
        )
    else:
        print("Enter valid number of arguments!")
