{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_parent_orchestration_pipeline_1_pre_Join_1_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH S3Source_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_parent_orchestration_pipeline_1_source', 'prophecy__temp_sanity_parent_orchestration_pipeline_1_pre_ordered_by_year_0') }}

),

ordered_by_year AS (

  SELECT * 
  
  FROM S3Source_1 AS in0
  
  ORDER BY Year ASC, Industry_aggregation_NZSIOC DESC

),

industry_data_by_year AS (

  SELECT 
    Year AS Year,
    Industry_aggregation_NZSIOC AS Industry_aggregation_NZSIOC,
    Industry_code_NZSIOC AS Industry_code_NZSIOC,
    Industry_name_NZSIOC AS Industry_name_NZSIOC,
    Units AS Units,
    Variable_code AS Variable_code,
    Variable_name AS Variable_name,
    Variable_category AS Variable_category,
    Value AS Value,
    Industry_code_ANZSIC06 AS Industry_code_ANZSIC06
  
  FROM ordered_by_year AS in0

),

Aggregate_1 AS (

  SELECT 
    any_value(YEAR) AS Year,
    any_value(Industry_aggregation_NZSIOC) AS Industry_aggregation_NZSIOC,
    any_value(Industry_code_NZSIOC) AS Industry_code_NZSIOC,
    any_value(Industry_name_NZSIOC) AS Industry_name_NZSIOC,
    any_value(Units) AS Units,
    any_value(Variable_code) AS Variable_code,
    any_value(Variable_name) AS Variable_name,
    any_value(Variable_category) AS Variable_category,
    any_value(Value) AS Value,
    any_value(Industry_code_ANZSIC06) AS Industry_code_ANZSIC06
  
  FROM industry_data_by_year AS in0
  
  GROUP BY Year

),

SetOperation_1 AS (

  SELECT * 
  
  FROM Aggregate_1 AS in0
  
  UNION
  
  SELECT * 
  
  FROM Aggregate_1 AS in1

),

SQLStatement_1 AS (

  SELECT *
  
  FROM SetOperation_1

)

SELECT *

FROM SQLStatement_1
