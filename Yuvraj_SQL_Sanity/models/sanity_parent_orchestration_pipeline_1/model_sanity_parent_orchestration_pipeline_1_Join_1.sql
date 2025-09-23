{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_parent_orchestration_pipeline_1_post_Join_1_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH env_uitesting_main_model_databricks_1_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_main_model_databricks_1')}}

),

SQLStatement_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_parent_orchestration_pipeline_1_source', 'prophecy__temp_sanity_parent_orchestration_pipeline_1_pre_Join_1_0') }}

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
