{% snapshot snapshot_parent_main %}
{{
  config({    
    "strategy": "timestamp",
    "target_schema": "qa_db_warehouse",
    "unique_key": "c_int",
    "updated_at": "c_date"
  })
}}

WITH all_type_partitioned AS (

  SELECT *
  
  FROM {{ source('alias_son_hive_metastore_qa_database', 'all_type_partitioned') }}

),

Reformat_1 AS (

  SELECT 
    c_tinyint AS c_tinyint,
    c_smallint AS c_smallint,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_float AS c_float,
    c_double AS c_double,
    c_string AS c_string,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_struct AS c_struct,
    p_int AS p_int,
    p_string AS p_string,
    CURRENT_DATE AS c_date
  
  FROM all_type_partitioned AS in0

)

SELECT *

FROM Reformat_1

{% endsnapshot %}
