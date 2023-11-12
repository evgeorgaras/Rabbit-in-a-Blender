# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from ..create_cdm_folders import CreateCdmFolders
from .bigquery_etl_base import BigQueryEtlBase


class BigQueryCreateCdmFolders(CreateCdmFolders, BigQueryEtlBase):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def _generate_sample_etl_query(self, omop_table: str) -> str:
        """Generates an example SQL query to query the raw data.

        Args:
            omop_table (str): The OMOP table
        """
        template = self._template_env.get_template("cdm_folders/sample_etl_query.sql.jinja")

        columns = self._get_omop_column_names(omop_table)
        sql = template.render(
            project_raw="{{project_raw}}",  # self._project_raw,
            omop_table="omop_table",
            columns=columns,
        )
        return sql

    def _generate_sample_usagi_query(self, omop_table: str, concept_column: str) -> str:
        """Generates an example SQL query to generate the Usagi source CSV.

        Args:
            omop_table (str): The OMOP table
            concept_column 'str): The concept column
        """
        template = self._template_env.get_template("cdm_folders/sample_usagi_query.sql.jinja")

        sql = template.render(
            project_raw="{{project_raw}}",  # self._project_raw,
            omop_table="omop_table",
            concept_column=concept_column,
        )
        return sql
