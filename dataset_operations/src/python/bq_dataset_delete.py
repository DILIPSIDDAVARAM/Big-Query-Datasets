#!/usr/bin/env python
# coding: utf-8
"""Importing Libraries"""
import argparse
import sys
import warnings

warnings.filterwarnings("ignore")

"""Importing google cloud libraries """
from google.cloud import bigquery


def cmd_args_parser():
    """Parsing command-line arguments"""
    parser = argparse.ArgumentParser(
        prog="DatasetDeletor", description="Deletes datasets",
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
        "--keyword",
        type=str,
        action="store",
        dest="keyword",
        default=None,
        help="Provide keyword to identify dataset names for deletion.",
        required=False,
    )

    args = parser.parse_args()
    cmdargs = {}

    # Define param_key -> param_value pairs
    cmdargs["project_id"] = args.project_id
    cmdargs["keyword"] = args.keyword

    return cmdargs


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
        # Creating Big Query Client
        client = bigquery.Client(project=project_id)
        # Get list of datasets for project
        datasets = list_all_datasets(bq_client=client)
        # Delete datasets if exist
        if datasets:
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


if __name__ == "__main__":
    cmdargs = cmd_args_parser()
    Dataset_Delete().main(project_id=cmdargs["project_id"], keyword=cmdargs["keyword"])
