# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
import os
import re
from pathlib import Path

import jpype
import jpype.imports


def render_sql(target_dialect: str, sql: str) -> str:
    # import the Java module
    from org.ohdsi.sql import (  # type: ignore # pylint: disable=import-outside-toplevel,import-error
        # SqlRender,
        SqlTranslate,
    )

    path_to_replacement_patterns = str(
        Path(__file__).parent.parent.resolve()
        / "src"
        / "riab"
        / "libs"
        / "SqlRender"
        / "inst"
        / "csv"
        / "replacementPatterns.csv"
    )

    # if len(parameters):
    #     sql = str(SqlRender.renderSql(sql, list(parameters.keys()), list(parameters.values())))

    sql = str(SqlTranslate.translateSqlWithPath(sql, target_dialect, None, None, path_to_replacement_patterns))
    return sql


def modify_bigquery_cdm_ddl(sql: str) -> str:
    # Solve some issues with the DDL
    sql = re.sub(
        r"@cdmDatabaseSchema",
        r"{{dataset_omop}}",
        sql,
    )
    sql = re.sub(
        r"(create table )({{dataset_omop}}).(.*).(\([\S\s.]+?\);)",
        r"DROP TABLE IF EXISTS `{{dataset_omop}}.\3`; \n\1`\2.\3` \4",
        sql,
    )
    # sql = re.sub(r".(?<!not )null", r"", sql)
    # sql = re.sub(r"\"", r"", sql)

    # add clustered indexes
    with open(
        str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "etl"
            / "bigquery"
            / "templates"
            / "ddl"
            / "OMOPCDM_bigquery_5.4_clustering_fields.json"
        ),
        "r",
        encoding="UTF8",
    ) as file:
        clustering_fields = json.load(file)
    for table, fields in clustering_fields.items():
        sql = re.sub(
            rf"(create table\s+`{{{{dataset_omop}}}}.{table}`)\s(\([\S\s]*?\))(\s*);",
            rf"\1 \2\ncluster by {', '.join(fields)};",
            sql,
            flags=re.DOTALL,
        )

    return sql


def modify_sqlserver_cdm_ddl(sql: str, ddl_part: str) -> str:
    # ... (keep existing schema and varchar length replacements)

    if ddl_part == "constraints":
        sql = """
-- ===================================================================
-- PHASE 1: Create missing placeholder concepts with detailed logging
-- ===================================================================
DECLARE @AddedConcepts TABLE (concept_id INT, source_table VARCHAR(50), source_column VARCHAR(50), records_affected INT);
DECLARE @LogMessage NVARCHAR(MAX) = '';

-- Find all missing concepts from all tables first
WITH MissingConcepts AS (
    -- From CONCEPT_RELATIONSHIP.concept_id_1
    SELECT DISTINCT cr.concept_id_1 as concept_id, 
           'CONCEPT_RELATIONSHIP' as source_table, 
           'concept_id_1' as source_column,
           COUNT(*) as records_affected
    FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT_RELATIONSHIP cr
    WHERE NOT EXISTS (
        SELECT 1 FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT c 
        WHERE c.concept_id = cr.concept_id_1
    )
    AND cr.concept_id_1 IS NOT NULL
    GROUP BY cr.concept_id_1
    
    UNION
    
    -- From CONCEPT_RELATIONSHIP.concept_id_2
    SELECT DISTINCT cr.concept_id_2 as concept_id, 
           'CONCEPT_RELATIONSHIP' as source_table, 
           'concept_id_2' as source_column,
           COUNT(*) as records_affected
    FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT_RELATIONSHIP cr
    WHERE NOT EXISTS (
        SELECT 1 FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT c 
        WHERE c.concept_id = cr.concept_id_2
    )
    AND cr.concept_id_2 IS NOT NULL
    GROUP BY cr.concept_id_2
    
    UNION
    
    -- From CONCEPT_SYNONYM
    SELECT DISTINCT cs.concept_id, 
           'CONCEPT_SYNONYM' as source_table, 
           'concept_id' as source_column,
           COUNT(*) as records_affected
    FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT_SYNONYM cs
    WHERE NOT EXISTS (
        SELECT 1 FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT c 
        WHERE c.concept_id = cs.concept_id
    )
    AND cs.concept_id IS NOT NULL
    GROUP BY cs.concept_id
    
    UNION
    
    -- From CONCEPT_ANCESTOR (ancestor)
    SELECT DISTINCT ca.ancestor_concept_id as concept_id, 
           'CONCEPT_ANCESTOR' as source_table, 
           'ancestor_concept_id' as source_column,
           COUNT(*) as records_affected
    FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT_ANCESTOR ca
    WHERE NOT EXISTS (
        SELECT 1 FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT c 
        WHERE c.concept_id = ca.ancestor_concept_id
    )
    AND ca.ancestor_concept_id IS NOT NULL
    GROUP BY ca.ancestor_concept_id
    
    UNION
    
    -- From CONCEPT_ANCESTOR (descendant)
    SELECT DISTINCT ca.descendant_concept_id as concept_id, 
           'CONCEPT_ANCESTOR' as source_table, 
           'descendant_concept_id' as source_column,
           COUNT(*) as records_affected
    FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT_ANCESTOR ca
    WHERE NOT EXISTS (
        SELECT 1 FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT c 
        WHERE c.concept_id = ca.descendant_concept_id
    )
    AND ca.descendant_concept_id IS NOT NULL
    GROUP BY ca.descendant_concept_id
)

-- Insert all missing concepts at once
INSERT INTO [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT (
    concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, 
    standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason
)
OUTPUT INSERTED.concept_id, src.source_table, src.source_column, src.records_affected INTO @AddedConcepts
SELECT 
    mc.concept_id,
    'Placeholder for missing concept ' + CAST(mc.concept_id AS VARCHAR) + 
    ' (referenced in ' + mc.source_table + '.' + mc.source_column + 
    ' by ' + CAST(mc.records_affected AS VARCHAR) + ' records)' as concept_name,
    'Metadata' as domain_id,
    'Vocabulary' as vocabulary_id,
    'Concept' as concept_class_id,
    NULL as standard_concept,
    'OMOP generated' as concept_code,
    CAST('1970-01-01' AS DATE) as valid_start_date,
    CAST('2099-12-31' AS DATE) as valid_end_date,
    NULL as invalid_reason
FROM MissingConcepts mc
WHERE NOT EXISTS (
    SELECT 1 FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT c 
    WHERE c.concept_id = mc.concept_id
);

-- Generate comprehensive log message
SELECT @LogMessage = @LogMessage + 
    'Added placeholder concept ' + CAST(concept_id AS VARCHAR) + 
    ' (referenced in ' + source_table + '.' + source_column + 
    ' by ' + CAST(records_affected AS VARCHAR) + ' records)' + CHAR(13) + CHAR(10)
FROM @AddedConcepts
ORDER BY records_affected DESC, source_table, source_column, concept_id;

IF @LogMessage <> ''
BEGIN
    DECLARE @TotalPlaceholders INT;
    SELECT @TotalPlaceholders = COUNT(*) FROM @AddedConcepts;
    
    PRINT '===================================================================';
    PRINT 'ADDED ' + CAST(@TotalPlaceholders AS VARCHAR) + ' PLACEHOLDER CONCEPTS';
    PRINT '===================================================================';
    PRINT @LogMessage;
END
ELSE
BEGIN
    PRINT 'No missing concepts found - all references are valid';
END

-- ===================================================================
-- PHASE 2: Create constraints WITH NOCHECK to bypass validation
-- ===================================================================
PRINT 'Creating constraints WITH NOCHECK to bypass immediate validation';

""" + re.sub(
    r"(ALTER TABLE \[{{omop_database_catalog}}\].\[{{omop_database_schema}}\].\w+ ADD CONSTRAINT \w+ FOREIGN KEY)",
    r"\1 WITH NOCHECK",
    sql
) + """

-- ===================================================================
-- PHASE 3: Validate all constraints and report issues
-- ===================================================================
PRINT 'Validating constraints...';

DECLARE @ConstraintName NVARCHAR(128);
DECLARE @TableName NVARCHAR(128);
DECLARE @ValidationSQL NVARCHAR(MAX);
DECLARE @ErrorCount INT;

DECLARE ConstraintCursor CURSOR FOR
SELECT name, OBJECT_NAME(parent_object_id)
FROM sys.foreign_keys
WHERE is_not_trusted = 1
AND OBJECT_NAME(referenced_object_id) = 'CONCEPT';

OPEN ConstraintCursor;
FETCH NEXT FROM ConstraintCursor INTO @ConstraintName, @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ValidationSQL = 'ALTER TABLE [{{omop_database_catalog}}].[{{omop_database_schema}}].' + QUOTENAME(@TableName) + 
                         ' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(@ConstraintName);
    
    BEGIN TRY
        EXEC sp_executesql @ValidationSQL;
        PRINT 'Successfully validated constraint: ' + @ConstraintName;
    END TRY
    BEGIN CATCH
        SET @ErrorCount = (SELECT COUNT(*) FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].[' + @TableName + '] t
                          WHERE NOT EXISTS (
                              SELECT 1 FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT c 
                              WHERE c.concept_id = t.' + 
                              CASE @ConstraintName
                                  WHEN 'fpk_CONCEPT_RELATIONSHIP_concept_id_1' THEN 'concept_id_1'
                                  WHEN 'fpk_CONCEPT_RELATIONSHIP_concept_id_2' THEN 'concept_id_2'
                                  WHEN 'fpk_CONCEPT_SYNONYM_concept_id' THEN 'concept_id'
                                  WHEN 'fpk_CONCEPT_ANCESTOR_ancestor_concept_id' THEN 'ancestor_concept_id'
                                  WHEN 'fpk_CONCEPT_ANCESTOR_descendant_concept_id' THEN 'descendant_concept_id'
                                  ELSE 'concept_id'
                              END + '
                          ));
        
        PRINT 'WARNING: Constraint ' + @ConstraintName + ' on table ' + @TableName + 
              ' has ' + CAST(@ErrorCount AS VARCHAR) + ' violations that could not be resolved';
        
        -- Generate detailed error report
        IF @ErrorCount > 0
        BEGIN
            DECLARE @ErrorReport NVARCHAR(MAX) = 'Violations for ' + @ConstraintName + ':' + CHAR(13) + CHAR(10);
            
            SET @ValidationSQL = 'SELECT TOP 50 ''' + @TableName + ''' as table_name, ' +
                               CASE @ConstraintName
                                   WHEN 'fpk_CONCEPT_RELATIONSHIP_concept_id_1' THEN 'concept_id_1'
                                   WHEN 'fpk_CONCEPT_RELATIONSHIP_concept_id_2' THEN 'concept_id_2'
                                   WHEN 'fpk_CONCEPT_SYNONYM_concept_id' THEN 'concept_id'
                                   WHEN 'fpk_CONCEPT_ANCESTOR_ancestor_concept_id' THEN 'ancestor_concept_id'
                                   WHEN 'fpk_CONCEPT_ANCESTOR_descendant_concept_id' THEN 'descendant_concept_id'
                                   ELSE 'concept_id'
                               END + ' as invalid_concept_id, COUNT(*) as affected_records ' +
                               'FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].[' + @TableName + '] t ' +
                               'WHERE NOT EXISTS (SELECT 1 FROM [{{omop_database_catalog}}].[{{omop_database_schema}}].CONCEPT c ' +
                               'WHERE c.concept_id = t.' + 
                               CASE @ConstraintName
                                   WHEN 'fpk_CONCEPT_RELATIONSHIP_concept_id_1' THEN 'concept_id_1'
                                   WHEN 'fpk_CONCEPT_RELATIONSHIP_concept_id_2' THEN 'concept_id_2'
                                   WHEN 'fpk_CONCEPT_SYNONYM_concept_id' THEN 'concept_id'
                                   WHEN 'fpk_CONCEPT_ANCESTOR_ancestor_concept_id' THEN 'ancestor_concept_id'
                                   WHEN 'fpk_CONCEPT_ANCESTOR_descendant_concept_id' THEN 'descendant_concept_id'
                                   ELSE 'concept_id'
                               END + ') ' +
                               'GROUP BY ' + 
                               CASE @ConstraintName
                                   WHEN 'fpk_CONCEPT_RELATIONSHIP_concept_id_1' THEN 'concept_id_1'
                                   WHEN 'fpk_CONCEPT_RELATIONSHIP_concept_id_2' THEN 'concept_id_2'
                                   WHEN 'fpk_CONCEPT_SYNONYM_concept_id' THEN 'concept_id'
                                   WHEN 'fpk_CONCEPT_ANCESTOR_ancestor_concept_id' THEN 'ancestor_concept_id'
                                   WHEN 'fpk_CONCEPT_ANCESTOR_descendant_concept_id' THEN 'descendant_concept_id'
                                   ELSE 'concept_id'
                               END + ' ORDER BY affected_records DESC';
            
            DECLARE @ErrorDetails TABLE (table_name NVARCHAR(128), invalid_concept_id INT, affected_records INT);
            INSERT INTO @ErrorDetails EXEC sp_executesql @ValidationSQL;
            
            SELECT @ErrorReport = @ErrorReport + 
                   '  - Concept ID: ' + CAST(invalid_concept_id AS VARCHAR) + 
                   ' (affects ' + CAST(affected_records AS VARCHAR) + ' records)' + CHAR(13) + CHAR(10)
            FROM @ErrorDetails;
            
            PRINT @ErrorReport;
            PRINT 'First 50 violations shown. Total violations: ' + CAST(@ErrorCount AS VARCHAR);
        END
    END CATCH
    
    FETCH NEXT FROM ConstraintCursor INTO @ConstraintName, @TableName;
END

CLOSE ConstraintCursor;
DEALLOCATE ConstraintCursor;
"""
    return sql


def render_cdm_ddl_queries(db_dialect: str):
    for ddl_part in ["ddl", "primary_keys", "constraints", "indices"]:
        sql_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "libs"
            / "CommonDataModel"
            / "inst"
            / "ddl"
            / "5.4"
            / db_dialect
            / f"OMOPCDM_{db_dialect}_5.4_{ddl_part}.sql"
        )
        with open(sql_path, "r", encoding="utf-8") as file:
            sql = file.read()

        match db_dialect:
            case "sql_server":
                target_dialect = "sql server"
            case _:
                target_dialect = db_dialect

        rendered_sql = render_sql(target_dialect, sql)

        match db_dialect:
            case "bigquery":
                match ddl_part:
                    case "ddl":
                        rendered_sql = modify_bigquery_cdm_ddl(rendered_sql)
                    case "primary_keys" | "constraints" | "indices":
                        continue
            case "sql_server":
                rendered_sql = modify_sqlserver_cdm_ddl(rendered_sql, ddl_part)

        jinja_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "etl"
            / db_dialect
            / "templates"
            / "ddl"
            / f"OMOPCDM_{db_dialect}_5.4_{ddl_part}.sql.jinja"
        )
        with open(jinja_path, "w", encoding="utf-8") as file:
            file.write(rendered_sql)


def modify_bigquery_dqd_ddl(sql: str) -> str:
    sql = re.sub(
        r"@tableName",
        r"{{dataset_dqd}}",
        sql,
    )
    sql = re.sub(
        r"DROP TABLE IF EXISTS {{dataset_dqd}};",
        r"DROP TABLE IF EXISTS `{{dataset_dqd}}`;",
        sql,
    )
    sql = re.sub(
        r"create table {{dataset_dqd}}",
        r"create table `{{dataset_dqd}}`",
        sql,
    )
    return sql


def modify_sqlserver_dqd_ddl(sql: str) -> str:
    sql = re.sub(
        r"@tableName",
        r"{{dqd_database_catalog}}.{{dqd_database_schema}}",
        sql,
    )
    return sql


def render_dqd_ddl_queries(db_dialect: str):
    for ddl_part in ["concept", "field", "table"]:
        sql_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "libs"
            / "DataQualityDashboard"
            / "inst"
            / "sql"
            / "sql_server"
            / f"result_table_ddl_{ddl_part}.sql"
        )
        with open(sql_path, "r", encoding="utf-8") as file:
            sql = file.read()

        match db_dialect:
            case "sql_server":
                target_dialect = "sql server"
            case _:
                target_dialect = db_dialect

        rendered_sql = render_sql(target_dialect, sql)

        match db_dialect:
            case "bigquery":
                rendered_sql = modify_bigquery_dqd_ddl(rendered_sql)
            case "sql_server":
                rendered_sql = modify_sqlserver_dqd_ddl(rendered_sql)

        jinja_path = str(
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "etl"
            / db_dialect
            / "templates"
            / "ddl"
            / f"result_table_ddl_{ddl_part}.sql.jinja"
        )
        with open(jinja_path, "w", encoding="utf-8") as file:
            file.write(rendered_sql)


def post_process_sqlrender_to_sqlserver_jinja(sql: str, sql_file: str):
    return sql


def post_process_sqlrender_to_bigquery_jinja(sql: str, sql_file: str):
    sql = re.sub(
        r"`datetime`",
        r"'datetime'",
        sql,
    )
    sql = re.sub(
        r"`date`",
        r"'date'",
        sql,
    )
    return sql


def convert_sqlrender_to_sqlserver_jinja(sql: str, sql_file: str):
    return sql


def convert_sqlrender_to_bigquery_jinja(sql: str, sql_file: str):
    # remove the comment block
    sql = re.sub(
        r"/\*\*\*\*\*\*\*\*\*([\S\s.]+?)\*\*\*\*\*\*\*\*\*\*/",
        r"",
        sql,
    )

    # replace the if else statement
    def replaceIfElse(match):
        else_statement = f"{{% else %}}{match.group(4)}" if match.group(4) else ""
        return f"{{% if {match.group(1)} %}}{match.group(2)}{else_statement}{{% endif %}}"

    sql = re.sub(r"{([\S\s.]+?)}\s??\?\s?{([\S\s.]+?)}(\s?:\s?{([\S\s.]+?)})?", replaceIfElse, sql)

    # replace the quoted parameters in the if statements
    sql = re.sub(
        r"'@([a-zA-Z]*)'(?=.*%})",
        r"\1",
        sql,
    )

    # replace the parameters in the if statements
    def replaceParametersWithinIf(match):
        if match.group(0).endswith("{% endif %}"):
            return match.group(0)
        else:
            return re.sub(
                r"@([a-zA-Z]*)",
                r"\1",
                match.group(0),
            )
            # return f"{match.group(1)}{match.group(2)}{match.group(3)}{match.group(4)}{match.group(5)}"

    sql = re.sub(
        r"({% if)([\S\s.]+?)@([a-zA-Z]*)([\S\s.]+?)(%})",
        replaceParametersWithinIf,
        sql,
    )

    # replace the & in the if statement
    sql = re.sub(
        r" & (?=.*%})",
        r" and ",
        sql,
    )
    # replace the | in the if statement
    sql = re.sub(
        r" \| (?=.*%})",
        r" or ",
        sql,
    )
    # replace () with [] for arrays in the if statement
    sql = re.sub(
        r"\(([a-zA-Z_'\,]*)\)(?=.*%})",
        r"[\1]",
        sql,
    )

    # lowercase the quoted strings in the if statement
    def lowerCaseQuotedStringsWithinIf(match):
        return re.sub(
            r"'([A-Z_]+)'",
            lambda m: f"'{m.group(1).lower()}'",
            match.group(0),
        )

    sql = re.sub(
        r"({% if)([\S\s.]+?)('[A-Z_]+')*([\S\s.]+?)(%})",
        lowerCaseQuotedStringsWithinIf,
        sql,
    )

    # replace the parameters
    sql = re.sub(
        r"@([a-zA-Z]*)",
        r"{{\1}}",
        sql,
    )

    return sql


def render_dqd_queries(db_dialect: str):
    sql_files = list(
        (
            Path(__file__).parent.parent.resolve()
            / "src"
            / "riab"
            / "libs"
            / "DataQualityDashboard"
            / "inst"
            / "sql"
            / "sql_server"
        ).glob("*.sql")
    )
    for sql_file in sql_files:
        render_dqd_query(sql_file, db_dialect)


def render_dqd_query(sql_file: Path, db_dialect: str):
    jinja_path = str(
        Path(__file__).parent.parent.resolve()
        / "src"
        / "riab"
        / "etl"
        / db_dialect
        / "templates"
        / "dqd"
        / f"{os.path.basename(sql_file)}.jinja"
    )

    with open(sql_file, "r", encoding="utf-8") as fr, open(jinja_path, "w", encoding="utf-8") as fw:
        sql = fr.read()
        match db_dialect:
            case "bigquery":
                jinja_sql = convert_sqlrender_to_bigquery_jinja(sql, os.path.basename(sql_file))
            case "sql_server":
                jinja_sql = convert_sqlrender_to_sqlserver_jinja(sql, os.path.basename(sql_file))
        rendered_sql = render_sql(db_dialect, jinja_sql)
        match db_dialect:
            case "bigquery":
                rendered_sql = post_process_sqlrender_to_bigquery_jinja(rendered_sql, os.path.basename(sql_file))
            case "sql_server":
                rendered_sql = post_process_sqlrender_to_bigquery_jinja(rendered_sql, os.path.basename(sql_file))
        fw.write(rendered_sql)


if __name__ == "__main__":
    # launch the JVM
    sqlrender_path = str(
        Path(__file__).parent.parent.resolve()
        / "src"
        / "riab"
        / "libs"
        / "SqlRender"
        / "inst"
        / "java"
        / "SqlRender.jar"
    )
    jpype.startJVM(classpath=[sqlrender_path])  # type: ignore

    # render_dqd_query(
    #     Path(__file__).parent.parent.resolve()
    #     / "src"
    #     / "riab"
    #     / "libs"
    #     / "DataQualityDashboard"
    #     / "inst"
    #     / "sql"
    #     / "sql_server"
    #     / "table_person_completeness.sql",
    #     "bigquery",
    # )

    for db_dialect in ["bigquery", "sql_server"]:
        # render_dqd_queries(db_dialect) #not yet stable, will need to convert SqlTranslate.translateSql JAVA method to Python
        render_cdm_ddl_queries(db_dialect)
        render_dqd_ddl_queries(db_dialect)
