#!/usr/bin/env python
# coding: utf-8
"""Importing python libraries"""
import argparse
import json

"""Importing google cloud libraries """
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import warnings

warnings.filterwarnings("ignore")


def cmd_args_parser():
    """Parsing command-line arguments"""
    parser = argparse.ArgumentParser(
        prog="DatasetUpdater", description="Updates datasets-properties",
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
        "--description",
        type=str,
        action="store",
        dest="description",
        help="Provide a description surrounded by quotes.",
        default=None,
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
        help='Format supported: \'{"key":"value"}\'.',
        default=None,
        required=False,
    )
    parser.add_argument(
        "--access_controls",
        nargs="*",
        type=str,
        action="store",
        dest="access_controls",
        help='Format supported: \'{"role":"value","entity_type":"value","entity_id":"value"}\'.',
        default=None,
        required=False,
    )
    args = parser.parse_args()
    # Labels str->dict conversion
    if args.labels:
        labels = json.loads(args.labels[0])
    else:
        labels = {}
    # Access Controls str->dict conversion
    if args.access_controls:
        access_controls = json.loads(args.access_controls[0])
    else:
        access_controls = {}
    cmdargs = {}
    properties = {}
    # Define key -> value property pairs
    properties["description"] = args.description
    properties["default_table_expiration_ms"] = args.default_table_expiration_ms
    properties["labels"] = labels
    properties["access_controls"] = access_controls
    # Define param_key -> param_value pairs
    cmdargs["project_id"] = args.project_id
    cmdargs["datasets"] = args.dataset_list.split()
    cmdargs["properties"] = properties

    return cmdargs


def update_datasets(**kwargs):
    """
    Updates datasets in Big Query.
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
    dataset_updation_flag = 0
    try:
        for dataset_id in datasets:
            dataset_id = project_id + "." + dataset_id
            try:
                dataset = bq_client.get_dataset(dataset_id)
                for k1, v1 in properties.items():
                    if v1:
                        if k1 == "description":
                            dataset.description = v1
                            dataset = bq_client.update_dataset(dataset, ["description"])
                        elif k1 == "default_table_expiration_ms":
                            dataset.default_table_expiration_ms = v1
                            dataset = bq_client.update_dataset(
                                dataset, ["default_table_expiration_ms"]
                            )
                        elif k1 == "labels":
                            dataset.labels = v1
                            dataset = bq_client.update_dataset(dataset, ["labels"])
                        elif k1 == "access_controls":
                            v1_vals = v1.values()
                            if None in v1_vals or True in [
                                str(elem).isspace() for elem in v1_vals
                            ]:
                                pass
                            else:
                                entry = bigquery.AccessEntry(
                                    role=properties["access_controls"]["role"],
                                    entity_type=properties["access_controls"][
                                        "entity_type"
                                    ],
                                    entity_id=properties["access_controls"][
                                        "entity_id"
                                    ],
                                )
                                entries = list(dataset.access_entries)
                                entries.append(entry)
                                dataset.access_entries = entries
                                dataset = bq_client.update_dataset(
                                    dataset, ["access_entries"]
                                )
            except NotFound:
                print("Dataset {} does not exist.".format(dataset_id))
                dataset_updation_flag = dataset_updation_flag + 1
    except Exception as e:
        print("Exception occurred: {}".format(e))
        dataset_updation_flag = dataset_updation_flag + 1
    finally:
        return dataset_updation_flag


class Dataset_Update:
    def __init__(self):
        pass

    @staticmethod
    def main(project_id=None, datasets=None, properties=None):
        """
        Updates datasets in project-id.
        :param project_id: Project-Id (type:str)
        :param datasets: Datasets List (type:list)
        :param properties: Dataset Properties (type:dict)
        """
        # Creating Big Query Client
        bq_client = bigquery.Client(project=project_id)
        # Creating datasets
        dataset_updation_flag = update_datasets(
            bq_client=bq_client,
            project_id=project_id,
            datasets=datasets,
            properties=properties,
        )
        print(
            "Dataset updation success criteria is {}.\nHelp: 0-SUCCESS, 1-FAIL".format(
                dataset_updation_flag
            )
        )


if __name__ == "__main__":
    cmdargs = cmd_args_parser()
    Dataset_Update().main(
        project_id=cmdargs["project_id"],
        datasets=cmdargs["datasets"],
        properties=cmdargs["properties"],
    )
