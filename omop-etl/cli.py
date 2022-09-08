# pylint: disable=unsubscriptable-object
import logging
import logging.config
import os
import sys
import traceback
from argparse import ArgumentParser
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Optional

from etl import Etl
from etl.bigquery import BigQuery


def main() -> None:
    """Main entry point of application"""
    with init_logging():
        try:
            parser = _contstruct_argument_parser()
            args = parser.parse_args()

            if __debug__:
                print(args)

            if args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)

            etl: Optional[Etl] = None
            match args.db_engine:
                case "BigQuery":
                    etl = BigQuery(
                        cdm_folder_path=args.cdm_folder_path,
                        only_omop_table=args.table,
                        skip_usagi_and_custom_concept_upload=args.skip_usagi_and_custom_concept_upload,
                        credentials_file=args.google_credentials_file,
                        project_id=args.google_project_id,
                        location=args.google_location,
                        dataset_id_raw=args.bigquery_dataset_id_raw,
                        dataset_id_work=args.bigquery_dataset_id_work,
                        dataset_id_omop=args.bigquery_dataset_id_omop,
                        bucket_uri=args.google_cloud_storage_bucket_uri,
                    )
                case _:
                    raise ValueError("Not a supported database engine")

            if args.create_db:  # create OMOP CDM DataBase
                etl.create_omop_db()
            elif args.import_vocabularies:  # impoprt OMOP CDM vocabularies
                etl.import_vocabularies(args.import_vocabularies)
            elif args.cleanup:  # cleanup OMOP DB
                etl.cleanup(args.cleanup)
            else:  # run ETL
                etl.run()

        except Exception:
            logging.error(traceback.format_exc())
            if __debug__:
                breakpoint()


def _contstruct_argument_parser() -> ArgumentParser:
    """Constructs the argument parser"""

    # parser for the required named arguments
    init_parser = MyParser(add_help=False)
    required_named = init_parser.add_argument_group("required named arguments")
    required_named.add_argument(
        "-d",
        "--db-engine",
        nargs="?",
        default="BigQuery",
        choices=["BigQuery"],
        type=str,
        help="""The database engine technology the ETL is running on.
        Each database engine has its own legacy SQL dialect, so the generated ETL queries can be different for
        each database engine. For the moment only BigQuery is supported, yet 'Rabbit in a Blender' has an open design,
        so in the future other database engines can be added easily.""",
        metavar="DB-ENGINE",
        required=not bool(set(sys.argv) & {"-h", "--help"}),
    )
    required_named.add_argument(
        "cdm_folder_path",
        metavar="PATH",
        nargs="?",
        type=str,
        help="Path to the folder structure that holds the queries, Usagi CSV's and the custom concept CSV's",
    )
    args, _ = init_parser.parse_known_args()

    # parser for the optional arguments
    parser = MyParser(
        prog="rabbit-in-a-blender",
        description="Rabbit in a Blender: an OMOP CDM ETL tool",
        parents=[init_parser],
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Verbose logging (logs are also writen to a log file in the systems tmp folder)",
        action="store_true",
    )
    parser.add_argument(
        "--create-db", help="Create the OMOP CDM tables", action="store_true"
    )
    parser.add_argument(
        "-s",
        "--skip-usagi-and-custom-concept-upload",
        help="Skips the parsing and uploading of the Usagi and custom concept CSV's",
        action="store_true",
    )
    parser.add_argument(
        "-i",
        "--import-vocabularies",
        nargs="?",
        type=str,
        help="""Extracts the vocabulary zip file (downloaded from the Athena website) and imports it
        into the OMOP CDM database.""",
        metavar="VOCABULARIES_ZIP_FILE",
    )
    parser.add_argument(
        "-c",
        "--cleanup",
        nargs="?",
        const="all",
        choices=[
            "all",
            "metadata",
            "cdm_source",
            "vocabulary",
            "location",
            "care_site",
            "provider",
            "person",
            "observation_period",
            "visit_occurrence",
            "visit_detail",
            "condition_occurrence",
            "drug_exposure",
            "procedure_occurrence",
            "device_exposure",
            "measurement",
            "observation",
            "death",
            "note",
            "note_nlp",
            "specimen",
            "fact_relationship",
            "payer_plan_period",
            "cost",
            "episode",
            "episode_event",
        ],
        type=str,
        help="""Cleanup all the OMOP tables, or just one.
        Be aware that the cleanup of a single table can screw up foreign keys!
        For instance cleaning up only the 'Person' table,
        will result in clicical results being mapped to the wrong persons!!!!""",
        metavar="TABLE",
    )
    parser.add_argument(
        "-t",
        "--table",
        nargs="?",
        choices=[
            "metadata",
            "cdm_source",
            "vocabulary",
            "location",
            "care_site",
            "provider",
            "person",
            "observation_period",
            "visit_occurrence",
            "visit_detail",
            "condition_occurrence",
            "drug_exposure",
            "procedure_occurrence",
            "device_exposure",
            "measurement",
            "observation",
            "death",
            "note",
            "note_nlp",
            "specimen",
            "fact_relationship",
            "payer_plan_period",
            "cost",
            "episode",
            "episode_event",
        ],
        type=str,
        help="""Do only ETL on this specific OMOP CDM table""",
        metavar="TABLE",
    )
    parser.add_argument(
        "--google-credentials-file",
        nargs="?",
        type=str,
        help="""Loads Google credentials from a file""",
        metavar="GOOGLE_CREDENTIALS_FILE",
    )
    parser.add_argument(
        "--google-project-id",
        nargs="?",
        type=str,
        help="""The Google GCP project id""",
        metavar="GOOGLE_PROJECT_ID",
    )
    parser.add_argument(
        "--google-location",
        nargs="?",
        default="EU",
        type=str,
        help="""The google locations to store the data (see https://cloud.google.com/about/locations)""",
        metavar="GOOGLE_LOCATION",
    )
    parser.add_argument(
        "--bigquery-dataset-id-raw",
        nargs="?",
        type=str,
        help="""BigQuery dataset that holds the raw EMR data""",
        required=args.db_engine == "BigQuery",
        metavar="BIGQUERY_DATASET_ID_RAW",
    )
    parser.add_argument(
        "--bigquery-dataset-id-work",
        nargs="?",
        type=str,
        help="""BigQuery dataset that will hold ETL housekeeping tables (ex: swap tablet, etc...)""",
        required=args.db_engine == "BigQuery",
        metavar="BIGQUERY_DATASET_ID_WORK",
    )
    parser.add_argument(
        "--bigquery-dataset-id-omop",
        nargs="?",
        type=str,
        help="""BigQuery dataset that will hold the final OMOP tables""",
        required=args.db_engine == "BigQuery",
        metavar="BIGQUERY_DATASET_ID_OMOP",
    )
    parser.add_argument(
        "--google-cloud-storage-bucket-uri",
        nargs="?",
        type=str,
        help="""Google Cloud Storage bucket uri, that will hold the uploaded Usagi and custom concept files.
        (the uri has format 'gs://{bucket_name}/{bucket_path}')""",
        required=args.db_engine == "BigQuery",
        metavar="GOOGLE_CLOUD_STORAGE_BUCKET_URI",
    )

    return parser


def init_logging() -> _TemporaryFileWrapper:
    """Initialise logging"""
    # get main logger
    main_logger = logging.getLogger()
    main_logger.setLevel(logging.INFO)

    # formatters
    default_formatter = logging.Formatter(
        "%(asctime)s: %(name)s: #%(lineno)d: %(levelname)s - %(message)s"
    )
    detailed_formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(pathname)s#%(lineno)d %(message)s"
    )

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(default_formatter)
    main_logger.addHandler(console_handler)

    # file handler
    tmp_file_handle = NamedTemporaryFile(
        delete=False, prefix="omop_etl_", suffix=".log"
    )
    print(f"Logs are written to {tmp_file_handle.name}")
    file_handler = logging.FileHandler(tmp_file_handle.name)
    file_handler.setFormatter(detailed_formatter)
    main_logger.addHandler(file_handler)

    return tmp_file_handle


class MyParser(ArgumentParser):
    """Custom ArgumentParser class with better error printing"""

    def error(self, message):
        """Prints a help message to stdout, the error message to stderr and
        exits.

        Args:
            message (string): The error message
        """
        self.print_help()
        sys.stderr.write(f"error: {message}{os.linesep}")
        sys.exit(2)


if __name__ == "__main__":
    main()
