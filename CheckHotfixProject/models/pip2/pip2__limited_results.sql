{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_database"
  })
}}

WITH all_type_csv AS (

  SELECT * 
  
  FROM {{ source('qa_team.qa_database', 'all_type_csv') }}

),

Reformat_1 AS (

  SELECT 
    {{ var('config1') }} AS col1,
    {{ var('config2') }} AS col2
  
  FROM all_type_csv AS in0

),

limited_results AS (

  SELECT * 
  
  FROM Reformat_1 AS in0
  
  LIMIT 1

)

SELECT *

FROM limited_results
