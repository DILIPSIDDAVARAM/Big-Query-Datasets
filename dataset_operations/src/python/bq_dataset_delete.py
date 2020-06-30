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
        prog="DatasetDeleter",
        description="Deletes datasets based on certain conditions",
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
        "--delete_contents",
        type=str.title,
        action="store",
        choices=["True", "False"],
        dest="delete_contents",
        help="Choose True to delete contents and False to preserve contents of dataset.",
        required=True,
    )
    parser.add_argument(
        "--dataset_list",
        type=str.split,
        action="store",
        dest="dataset_list",
        default=[],
        help="Provide list of dataset names separated by whitespace.",
        required=False,
    )
    parser.add_argument(
        "--keyword",
        type=str,
        action="store",
        dest="keyword",
        help="Provide keyword to filter dataset names.",
        required=False,
    )
    parser.add_argument(
        "--filter",
        type=str.lower,
        action="store",
        choices=["ignore", "choose"],
        default="all",
        dest="filter",
        help="Choose from options to select type of filter. Default: %(default)s",
        required=False,
    )
    args = parser.parse_args()
    cmdargs = {}
    # Define param_key -> param_value pairs
    cmdargs["project_id"] = args.project_id
    cmdargs["delete_contents"] = args.delete_contents
    cmdargs["datasets"] = args.dataset_list
    cmdargs["keyword"] = args.keyword
    cmdargs["filter"] = args.filter

    return cmdargs


def list_all_datasets(bq_client=None, keyword=None, filter=None):
    """
    Returns a list of datasets in the project.
    :param bq_client: Bigquery Client (type:google.cloud.bigquery.client.Client)
    :param keyword: Keyword to be used in filter (type:str)
    :param filter: Type of filter, choices[all, ignore, choose] (type:str)
    :return datasets: List of Datasets(type:list)
    """
    datasets = []
    datasets_list = list(bq_client.list_datasets())
    if datasets_list:
        for dataset in datasets_list:
            datasets.append(dataset.dataset_id)
    else:
        print("No dataset exists.")
    if filter == "all" and not keyword:
        datasets = datasets
    elif filter == "ignore" and keyword:
        datasets = [dataset for dataset in datasets if keyword not in dataset]
    elif (filter == "choose" or filter == "all") and keyword:
        datasets = [dataset for dataset in datasets if keyword in dataset]
    else:
        print("Provide valid arguments for keyword and filter.")

    return datasets


def delete_datasets(**kwargs):
    """
    Deletes datasets in Big Query.
    :param bq_client: Target Big Query Client (type:google.cloud.bigquery.client.Client)
    :param project_id: GCP Project_Id (type:str)
    :param datasets: List of Datasets (type:list)
    :param delete_contents: True/False (type:bool)
    :return dataset_deletion_flag: {0:SUCCESS, 1:FAIL} (type:int)
    """
    bq_client = kwargs.get("bq_client")
    project_id = kwargs.get("project_id")
    datasets = kwargs.get("datasets", [])
    delete_contents = kwargs.get("delete_contents")
    dataset_deletion_flag = 0
    try:
        for dataset in datasets:
            dataset_id = "{}.{}".format(project_id, dataset)
            bq_client.delete_dataset(
                dataset_id, delete_contents=delete_contents, not_found_ok=True
            )
    except Exception as e:
        print("Exception occurred: {}".format(e))
        dataset_deletion_flag = dataset_deletion_flag + 1
    finally:
        return dataset_deletion_flag


class Dataset_Delete:
    def __init__(self):
        pass

    @staticmethod
    def main(
        project_id=None, datasets=None, delete_contents=None, keyword=None, filter=None
    ):
        """
        Deletes datasets in project-id.
        :param project_id: Project-Id (type:str)
        :param datasets: Datasets List (type:list)
        :param delete_contents: True/False (type:str)
        :param keyword: Keyword to be used in filter (type:str)
        :param filter: Type of filter, choices[all, ignore, choose] (type:str)
        """
        # Getting service account path from environment variable
        service_account_key_file = os.getenv("SERVICE_ACCOUNT_PATH")
        # Setting auth for GCP from service account
        with open(service_account_key_file) as key:
            info = json.load(key)
        bq_credentials = service_account.Credentials.from_service_account_info(info)
        # Creating Big Query Client
        bq_client = bigquery.Client(project=project_id, credentials=bq_credentials)
        # Delete_Contents Str -> Bool
        if delete_contents == "True":
            delete_contents = True
        else:
            delete_contents = False
        # Check if datasets list provided
        if datasets:
            # Applying filters
            if filter == "all" and not keyword:
                datasets = datasets
            elif filter == "ignore" and keyword:
                datasets = [dataset for dataset in datasets if keyword not in dataset]
            elif (filter == "choose" or filter == "all") and keyword:
                datasets = [dataset for dataset in datasets if keyword in dataset]
            else:
                print("Provide valid arguments for keyword and filter.")
        else:
            # Get list of datasets for project
            datasets = list_all_datasets(
                bq_client=bq_client, keyword=keyword, filter=filter
            )
        # Deleting datasets
        dataset_deletion_flag = delete_datasets(
            bq_client=bq_client,
            project_id=project_id,
            datasets=datasets,
            delete_contents=delete_contents,
        )
        print(
            "Dataset(s) deletion success criteria is {}.\nHelp: 0-SUCCESS, 1-FAIL".format(
                dataset_deletion_flag
            )
        )


if __name__ == "__main__":
    cmdargs = cmd_args_parser()
    Dataset_Delete().main(
        project_id=cmdargs["project_id"],
        datasets=cmdargs["datasets"],
        delete_contents=cmdargs["delete_contents"],
        keyword=cmdargs["keyword"],
        filter=cmdargs["filter"],
    )
