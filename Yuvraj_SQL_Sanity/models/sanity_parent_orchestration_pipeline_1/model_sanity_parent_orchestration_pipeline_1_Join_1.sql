{{
  config({    
    "materialized": "table",
    "alias": "prophecy_tmp__mbkbpoaq__sanity_parent_orchestration_pipeline_1__Join_1",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH S3Source_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy_tmp_source__sanity_parent_orchestration_pipeline_1', 'prophecy_tmp__mbkbpoaq__sanity_parent_orchestration_pipeline_1__S3Source_1') }}

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

),

env_uitesting_main_model_databricks_1_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_main_model_databricks_1')}}

),

Join_1 AS (

  SELECT 
    in0.Year AS Year,
    in0.Industry_aggregation_NZSIOC AS Industry_aggregation_NZSIOC,
    in0.Industry_code_NZSIOC AS Industry_code_NZSIOC,
    in0.Industry_name_NZSIOC AS Industry_name_NZSIOC,
    in0.Units AS Units,
    in0.Variable_code AS Variable_code,
    in0.Variable_name AS Variable_name,
    in0.Variable_category AS Variable_category,
    in0.Value AS Value,
    in0.Industry_code_ANZSIC06 AS Industry_code_ANZSIC06,
    in1.c_array AS c_array,
    in1.c_struct AS c_struct,
    in1.p_string AS p_string,
    in1.c_int AS c_int,
    in1.c_float AS c_float,
    in1.c_boolean AS c_boolean,
    in1.c_double AS c_double,
    in1.c_id AS c_id
  
  FROM SQLStatement_1 AS in0
  INNER JOIN env_uitesting_main_model_databricks_1_1 AS in1
     ON in0.Year != in1.p_int

)

SELECT *

FROM Join_1
