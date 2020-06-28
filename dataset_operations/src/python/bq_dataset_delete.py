#!/usr/bin/env python
# coding: utf-8
"""Importing Libraries"""
import json
import os
import sys
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
    return datasets


def delete_datasets(bq_client=None, project_id=None, datasets=None):
    """
    Deletes the given list of datasets for given project-id.
    :param bq_client: Bigquery Client (type:google.cloud.bigquery.client.Client)
    :param project_id: (type:str)
    :param datasets: (type:list)
    """
    try:
        for dataset in datasets:
            dataset_id = "{}.{}".format(project_id, dataset)
            bq_client.delete_dataset(
                dataset_id, delete_contents=True, not_found_ok=True
            )
            print("Deleted dataset {}".format(dataset_id))
        exec_flag = 1
    except Exception as e:
        print(e)
        exec_flag = 0
    finally:
        return exec_flag


class Dataset_Delete:
    def __init__(self):
        pass

    @staticmethod
    def main(project_id=None, keyword=None):
        """
        Deletes datasets in project-id.
        :param project_id: (type:str)
        :param keyword: To delete specific datasets (type:str)
        """
        # Reading parameters
        project_id = str(project_id)
        if keyword:
            keyword = str(keyword)
        # Getting service account path from environment variable
        service_account_key_file = os.getenv("SERVICE_ACCOUNT_PATH")
        # Setting auth for GCP from service account
        with open(service_account_key_file) as source:
            info = json.load(source)
        bq_credentials = service_account.Credentials.from_service_account_info(info)
        # Creating Big Query Client
        client = bigquery.Client(project=project_id, credentials=bq_credentials)
        # Get list of datasets for project
        datasets = list_all_datasets(bq_client=client)
        # Delete datasets if exist
        if datasets:
            # Switch to User Account
            user_account = os.getenv("BQ_TRANSFER_USER_ACCOUNT")
            os.system("gcloud config set account {}".format(user_account))
            # Delete datasets based on keyword presence
            if keyword:
                datasets = [dataset for dataset in datasets if keyword in dataset]
                print(
                    "Deleting datasets containing keyword:{} in project:{}".format(
                        keyword, project_id
                    )
                )
                exec_flag = delete_datasets(
                    bq_client=client, project_id=project_id, datasets=datasets
                )
            else:
                print("Deleting all datasets in project:{}".format(project_id))
                exec_flag = delete_datasets(
                    bq_client=client, project_id=project_id, datasets=datasets
                )
            print(exec_flag)
            # Switch to Default Account
            default_account = os.getenv("DEFAULT_SERVICE_ACCOUNT")
            os.system("gcloud config set account {}".format(default_account))


if __name__ == "__main__":
    if len(sys.argv) == 2:
        project_id = sys.argv[1]
        Dataset_Delete().main(project_id=project_id)
    elif len(sys.argv) == 3:
        project_id = sys.argv[1]
        keyword = sys.argv[2]
        Dataset_Delete().main(project_id=project_id, keyword=keyword)
    else:
        print("Enter valid number of arguments!")
