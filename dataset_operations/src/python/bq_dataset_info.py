#!/usr/bin/env python
# coding: utf-8
"""Importing python libraries"""
import argparse
import json
import os

"""Importing google cloud libraries """
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import warnings

warnings.filterwarnings("ignore")


def cmd_args_parser():
    """Parsing command-line arguments"""
    parser = argparse.ArgumentParser(
        prog="DatasetInfo", description="Gets datasets-information",
    )
    parser.add_argument(
        "--project_id",
        type=str,
        action="store",
        dest="project_id",
        help="Provide Google Cloud Project-Id.",
        required=True,
    )
    parser.add_argument(
        "--datasets",
        type=str,
        action="store",
        dest="datasets",
        help="Provide list of dataset names separated by comma.",
        required=True,
    )
    args = parser.parse_args()
    cmdargs = {}
    # Define param_key -> param_value pairs
    cmdargs["project_id"] = args.project_id
    cmdargs["datasets"] = args.datasets.split(",")

    return cmdargs


def get_dataset_info(**kwargs):
    """
    Updates datasets in Big Query.
    :param bq_client: Target Big Query Client (type:google.cloud.bigquery.client.Client)
    :param project_id: GCP Project_Id (type:str)
    :param datasets: List of Datasets (type:list)
    :return dataset_info, dataset_information_flag: {0:SUCCESS, 1:FAIL} (type:dict,int)
    """
    bq_client = kwargs.get("bq_client")
    project_id = kwargs.get("project_id")
    datasets = kwargs.get("datasets", [])
    dataset_information_flag = 0
    dataset_info = {}
    try:
        for dataset_id in datasets:
            dataset_id = project_id + "." + dataset_id
            try:
                dataset = bq_client.get_dataset(dataset_id)
                access_entries = []
                for entry in dataset.access_entries:
                    nested_temp = {}
                    nested_temp["role"] = entry.role
                    nested_temp["entity_id"] = entry.entity_id
                    nested_temp["entity_type"] = entry.entity_type
                    access_entries.append(nested_temp)
                temp = {
                    "project_id": dataset.project,
                    "dataset_id": dataset.dataset_id,
                    "creation_time": dataset.created.strftime("%Y-%m-%d"),
                    "modified_time": dataset.modified.strftime("%Y-%m-%d"),
                    "location": dataset.location,
                    "labels": dataset.labels,
                    "access_entries": access_entries,
                    "default_table_expiration_ms": dataset.default_table_expiration_ms,
                    "description": dataset.description,
                    "tables": [
                        table.table_id for table in list(bq_client.list_tables(dataset))
                    ],
                }
                dataset_info[dataset.full_dataset_id] = temp
            except NotFound:
                print("Dataset {} does not exist.".format(dataset_id))
                dataset_information_flag = dataset_information_flag + 1
    except Exception as e:
        print("Exception occurred: {}".format(e))
        dataset_information_flag = dataset_information_flag + 1
    finally:
        return dataset_info, dataset_information_flag


class Dataset_Update:
    def __init__(self):
        pass

    @staticmethod
    def main(project_id=None, datasets=None):
        """
        Updates datasets in project-id.
        :param project_id: Project-Id (type:str)
        :param datasets: Datasets List (type:list)
        """
        # Getting service account path from environment variable
        service_account_key_file = os.getenv("SERVICE_ACCOUNT_PATH")
        # Setting auth for GCP from service account
        with open(service_account_key_file) as key:
            info = json.load(key)
        bq_credentials = service_account.Credentials.from_service_account_info(info)
        # Creating Big Query Client
        bq_client = bigquery.Client(project=project_id, credentials=bq_credentials)
        # Getting information of datasets
        dataset_info, dataset_information_flag = get_dataset_info(
            bq_client=bq_client, project_id=project_id, datasets=datasets,
        )
        print(
            "Dataset information retrieval success criteria is {}.\nHelp: 0-SUCCESS, 1-FAIL".format(
                dataset_information_flag
            )
        )
        print("The dataset information retrieved is \n {}".format(dataset_info))


if __name__ == "__main__":
    cmdargs = cmd_args_parser()
    Dataset_Update().main(
        project_id=cmdargs["project_id"], datasets=cmdargs["datasets"],
    )
