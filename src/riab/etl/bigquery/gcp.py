# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""This module holds the Gcp class,
Google Cloud Provider class with usefull methods for ETL"""
# pylint: disable=no-member
import logging
import math
import time
from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

import backoff
import connectorx as cx
import google.cloud.bigquery as bq
import google.cloud.storage as cs
import pyarrow as pa
from google.auth.credentials import Credentials
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator
from google.cloud.exceptions import NotFound

class Gcp:
    """
    Google Cloud Provider class with usefull methods for ETL
    Local Query --> Parquet --> Cloud Storage --> Bigquery
    """

    _MEGA = 1024**2
    _GIGA = 1024**3
    _COST_PER_10_MB = 6 / 1024 / 1024 * 10

    def __init__(self, credentials: Credentials, location: str = "EU"):
        """Constructor

        Args:
            credentials (Credentials): The Google auth credentials (see https://google-auth.readthedocs.io/en/stable/reference/google.auth.credentials.html)
            location (str): The location in GCP (see https://cloud.google.com/about/locations/)
        """  # noqa: E501 # pylint: disable=line-too-long
        self._cs_client = cs.Client(credentials=credentials)
        self._bq_client = bq.Client(credentials=credentials)
        self._location = location
        self._total_cost = 0
        self._lock_total_cost = Lock()

    @property
    def total_cost(self):
        """Gets the total BigQuery cost

        Returns:
            float: total cost in €
        """
        return self._total_cost

    def get_table_names(self, project_id: str, dataset_id: str) -> List[str]:
        """Get all table names from a specific dataset in Big Query

        Args:
            project_id (str): project ID
            dataset_id (str): dataset ID

        Returns:
            List[str]: list of table names
        """
        query = f"""
SELECT DISTINCT table_name
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
ORDER BY table_name"""
        rows = self.run_query_job(query)
        return [row.table_name for row in rows]

    def get_columns(self, project_id: str, dataset_id: str, table_name: str) -> Any:
        """Get metadata from all column in a table in a Big Query dataset

        Args:
            project_id (str): project ID
            dataset_id (str): dataset ID
            table_name (str): table name

        Returns:
            Any: the column metadata
        """
        query = f"""
SELECT *
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = @table_name
ORDER BY ordinal_position"""
        query_parameters = [
            bq.ScalarQueryParameter("table_name", "STRING", table_name),
        ]
        rows = self.run_query_job(query, query_parameters)
        return rows

    def run_query_job(
        self,
        query: str,
        query_parameters: Union[List[bq.ScalarQueryParameter], None] = None,
    ) -> Union[RowIterator, _EmptyRowIterator]:
        """Runs a query with or without parameters on Big Query
        Calculates and logs the billed cost of the query

        Args:
            query (str): the sql query
            query_parameters (List[bigquery.ScalarQueryParameter], optional): the query parameters

        Returns:
            RowIterator: row iterator
        """  # noqa: E501 # pylint: disable=line-too-long
        result, execution_time = self.run_query_job_with_benchmark(
            query, query_parameters
        )
        return result

    def run_query_job_with_benchmark(
        self,
        query: str,
        query_parameters: Union[List[bq.ScalarQueryParameter], None] = None,
    ) -> Tuple[Union[RowIterator, _EmptyRowIterator], float]:
        """Runs a query with or without parameters on Big Query
        Calculates and logs the billed cost of the query

        Args:
            query (str): the sql query
            query_parameters (List[bigquery.ScalarQueryParameter], optional): the query parameters

        Returns:
            RowIterator: row iterator
        """  # noqa: E501 # pylint: disable=line-too-long
        try:
            job_config = bq.QueryJobConfig(
                query_parameters=query_parameters or [],
            )
            logging.debug(
                "Running query: %s\nWith parameters: %s", query, str(query_parameters)
            )
            start = time.time()
            query_job = self._bq_client.query(
                query, job_config=job_config, location=self._location
            )
            result = query_job.result()
            end = time.time()
            # cost berekening $6.00 per TB (afgerond op 10 MB naar boven)
            cost_per_10_mb = 6 / 1024 / 1024 * 10
            total_10_mbs_billed = math.ceil(
                (query_job.total_bytes_billed or 0) / (Gcp._MEGA * 10)
            )
            cost = total_10_mbs_billed * cost_per_10_mb
            execution_time = end - start
            logging.debug(
                "Query processed %.2f MB (%.2f MB billed) in %.2f seconds"
                " (%.2f seconds slot time): %.8f $ billed",
                (query_job.total_bytes_processed or 0) / Gcp._MEGA,
                (query_job.total_bytes_billed or 0) / Gcp._MEGA,
                execution_time,
                (query_job.slot_millis or 0) / 1000,
                cost,
            )
            if execution_time > 60:
                logging.warning(
                    "Long query time (%.2f seconds) for query: %s",
                    execution_time,
                    query,
                )
            return result, execution_time
        except Exception as ex:
            logging.debug(
                "FAILED QUERY: %s\nWith parameters: %s", query, str(query_parameters)
            )
            raise ex

    def set_clustering_fields_on_table(
        self,
        project_id: str,
        dataset_id: str,
        table_name: str,
        clustering_fields: List[str],
    ):
        """Delete a table from Big Query
        see https://cloud.google.com/bigquery/docs/creating-clustered-tables#modifying-cluster-spec

        Args:
            project_id (str): project ID
            dataset_id (str): dataset ID
            table_name (str): table name
            clustering_fields (List[str]): list of fields (ordered!) to cluster in table table_name
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug(
            "Setting cluster fields on BigQuery table '%s.%s.%s'",
            project_id,
            dataset_id,
            table_name,
        )
        table = self._bq_client.get_table(
            bq.DatasetReference(project_id, dataset_id).table(table_name)
        )
        table.clustering_fields = clustering_fields
        self._bq_client.update_table(table, ["clustering_fields"])

    def delete_table(self, project_id: str, dataset_id: str, table_name: str):
        """Delete a table from Big Query
        see https://cloud.google.com/bigquery/docs/samples/bigquery-delete-table#bigquery_delete_table-python

        Args:
            project_id (str): project ID
            dataset_id (str): dataset ID
            table_name (str): table name
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug(
            "Dropping BigQuery table '%s.%s.%s'", project_id, dataset_id, table_name
        )
        table = self._bq_client.dataset(dataset_id, project_id).table(table_name)
        self._bq_client.delete_table(table, not_found_ok=True)

    def delete_from_bucket(self, bucket_uri: str):
        """Delete a blob from a Cloud Storage bucket
        see https://cloud.google.com/storage/docs/deleting-objects#code-samples

        Args
            bucket_uri (str): The bucket uri
        """
        try:
            scheme, netloc, path, params, query, fragment = urlparse(bucket_uri)
            logging.debug("Delete path '%s' from bucket '%s", netloc, path)
            bucket = self._cs_client.bucket(netloc)
            blobs = bucket.list_blobs(prefix=path.lstrip("/"))
            for blob in blobs:
                blob.delete()
        except NotFound:
            pass

    def upload_file_to_bucket(
        self, source_file_path: Union[str, Path], bucket_uri: str
    ):
        """Upload a local file to a Cloud Storage bucket
        see https://cloud.google.com/storage/docs/uploading-objects

        Args:
            source_file_path (Path): Path to the local file
            bucket_uri (str): Name of the Cloud Storage bucket and the path in the bucket (directory) to store the file (with format: 'gs://{bucket_name}/{bucket_path}')
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug(
            "Upload file '%s' to bucket '%s'",
            str(source_file_path),
            bucket_uri,
        )
        scheme, netloc, path, params, query, fragment = urlparse(bucket_uri)
        bucket = self._cs_client.bucket(netloc)
        filename_w_ext = Path(source_file_path).name
        blob = bucket.blob(f"{path.lstrip('/')}/{filename_w_ext}")
        blob.upload_from_filename(str(source_file_path))
        return f"{bucket_uri}/{filename_w_ext}"  # urljoin doesn't work with protocol gs

    def batch_load_from_bucket_into_bigquery_table(
        self,
        uri: str,
        project_id: str,
        dataset_id: str,
        table_name: str,
        write_disposition: str = bq.WriteDisposition.WRITE_APPEND,
        schema: Optional[Sequence[SchemaField]] = None,
    ):
        """Batch load parquet files from a Cloud Storage bucket to a Big Query table
        see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#python

        Args:
            uri (str): the uri of the bucket blob(s) in the form of 'gs://{bucket_name}/{bucket_path}/{blob_name(s)}.parquet'
            project_id (str): project ID
            dataset_id (str): dataset ID
            table_name (str): table name
        """  # noqa: E501 # pylint: disable=line-too-long
        logging.debug(
            "Append bucket files '%s' to BigQuery table '%s.%s.%s'",
            uri,
            project_id,
            dataset_id,
            table_name,
        )
        table = self._bq_client.dataset(dataset_id, project_id).table(table_name)
        job_config = bq.LoadJobConfig(
            write_disposition=write_disposition,
            schema_update_options=bq.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            if write_disposition == bq.WriteDisposition.WRITE_APPEND
            or write_disposition == bq.WriteDisposition.WRITE_TRUNCATE
            else None,
            source_format=bq.SourceFormat.PARQUET,
            schema=schema,
            autodetect=False if schema else True,
        )
        load_job = self._bq_client.load_table_from_uri(
            uri, table, job_config=job_config
        )  # Make an API request.
        load_job.result()  # Waits for the job to complete.

        table = self._bq_client.get_table(
            bq.DatasetReference(project_id, dataset_id).table(table_name)
        )
        logging.debug(
            "Loaded %i rows into '%s.%s.%s'",
            table.num_rows,
            project_id,
            dataset_id,
            table_name,
        )

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
        giveup=lambda e: isinstance(e, RuntimeError) and "Token error" in str(e),
    )
    def load_local_query_result(self, conn: str, query: str) -> Tuple[pa.Table, int]:
        """Executes a local query and loads the results in an Arrow table
        see https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table

        Args:
            conn (str): The connection string
            query (str): The SQL statement of the select query

        Returns:
            Table: Memory efficient Arrow table, with the query results
        """
        logging.debug("Running query '%s", query)
        start = time.time()
        table: pa.Table = cx.read_sql(conn, query, return_type="arrow")
        end = time.time()
        table_size = table.nbytes
        logging.debug(
            "Query returned %i rows with table size %.2f MB in %.2f seconds",
            table.num_rows,
            table.nbytes / Gcp._MEGA,
            end - start,
        )
        return (
            deepcopy(table),
            table_size,
        )  # deepcopy because of https://github.com/sfu-db/connector-x/issues/196
