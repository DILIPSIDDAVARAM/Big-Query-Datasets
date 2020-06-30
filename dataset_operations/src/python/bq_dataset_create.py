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
        prog="DatasetCreator",
        description="Creates datasets with properties if do not exist",
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
        "--dataset_list",
        type=str,
        action="store",
        dest="dataset_list",
        help="Provide list of dataset names separated by whitespace.",
        required=True,
    )
    parser.add_argument(
        "--location",
        type=str,
        action="store",
        dest="location",
        default="US",
        help="Provide location for datasets to be created.",
        required=True,
    )
    parser.add_argument(
        "--description",
        type=str,
        action="store",
        dest="description",
        default=None,
        help="Provide a description surrounded by quotes.",
        required=False,
    )
    parser.add_argument(
        "--default_table_expiration_ms",
        type=int,
        action="store",
        dest="default_table_expiration_ms",
        help="Provide default table expiration time (in seconds). Minimum value is 3600000ms",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--labels",
        nargs="*",
        type=str,
        action="store",
        dest="labels",
        default=None,
        help='Format supported: \'{"key":"value"}\'.',
        required=False,
    )
    args = parser.parse_args()
    # Labels str->dict conversion
    if args.labels:
        labels = json.loads(args.labels[0])
    else:
        labels = {}
    cmdargs = {}
    properties = {}
    # Define key -> value property pairs
    properties["location"] = args.location
    properties["description"] = args.description
    properties["default_table_expiration_ms"] = args.default_table_expiration_ms
    properties["labels"] = labels
    # Define param_key -> param_value pairs
    cmdargs["project_id"] = args.project_id
    cmdargs["datasets"] = args.dataset_list.split()
    cmdargs["properties"] = properties

    return cmdargs


def create_datasets(**kwargs):
    """
    Creates datasets in Big Query.
    :param bq_client: Target Big Query Client (type:google.cloud.bigquery.client.Client)
    :param project_id: GCP Project_Id (type:str)
    :param datasets: List of Datasets (type:list)
    :param properties: Dataset Properties (type:dict)
    :return dataset_creation_flag: {0:SUCCESS, 1:FAIL} (type:int)
    """
    bq_client = kwargs.get("bq_client")
    project_id = kwargs.get("project_id")
    datasets = kwargs.get("datasets", [])
    properties = kwargs.get("properties", {})
    dataset_creation_flag = 0
    try:
        for dataset_id in datasets:
            dataset_id = project_id + "." + dataset_id
            try:
                bq_client.get_dataset(dataset_id)
                print("Dataset {} already exists.".format(dataset_id))
            except NotFound:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = properties["location"]
                dataset.description = properties["description"]
                dataset.labels = properties["labels"]
                dataset.default_table_expiration_ms = properties[
                    "default_table_expiration_ms"
                ]
                dataset = bq_client.create_dataset(dataset)
    except Exception as e:
        print("Exception occurred: {}".format(e))
        dataset_creation_flag = dataset_creation_flag + 1
    finally:
        return dataset_creation_flag


class Dataset_Create:
    def __init__(self):
        pass

    @staticmethod
    def main(project_id=None, datasets=None, properties=None):
        """
        Creates datasets in project-id.
        :param project_id: Project-Id (type:str)
        :param datasets: Datasets List (type:list)
        :param properties: Dataset Properties (type:dict)
        """
        # Getting service account path from environment variable
        service_account_key_file = os.getenv("SERVICE_ACCOUNT_PATH")
        # Setting auth for GCP from service account
        with open(service_account_key_file) as key:
            info = json.load(key)
        bq_credentials = service_account.Credentials.from_service_account_info(info)
        # Creating Big Query Client
        bq_client = bigquery.Client(project=project_id, credentials=bq_credentials)
        # Creating datasets
        dataset_creation_flag = create_datasets(
            bq_client=bq_client,
            project_id=project_id,
            datasets=datasets,
            properties=properties,
        )
        print(
            "Dataset creation success criteria is {}.\nHelp: 0-SUCCESS, 1-FAIL".format(
                dataset_creation_flag
            )
        )


if __name__ == "__main__":
    cmdargs = cmd_args_parser()
    Dataset_Create().main(
        project_id=cmdargs["project_id"],
        datasets=cmdargs["datasets"],
        properties=cmdargs["properties"],
    )
