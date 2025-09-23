with DAG():
    S3Source_1 = Task(
        task_id = "S3Source_1", 
        component = "Dataset", 
        table = {
          "name": "prophecy__temp_sanity_parent_orchestration_pipeline_1_pre_ordered_by_year_0", 
          "sourceType": "Source", 
          "sourceName": "prophecy__temp_sanity_parent_orchestration_pipeline_1_source", 
          "alias": ""
        }
    )
    model_sanity_parent_orchestration_pipeline_1_SQLStatement_1 = Task(
        task_id = "model_sanity_parent_orchestration_pipeline_1_SQLStatement_1", 
        component = "Model", 
        modelName = "model_sanity_parent_orchestration_pipeline_1_SQLStatement_1"
    )
    S3Source_1 = SourceTask(
        task_id = "S3Source_1", 
        component = "OrchestrationSource", 
        kind = "S3Source", 
        connector = Connection(kind = "s3", id = "s3"), 
        format = CSVFormat(
          header = True, 
          schema = {
            "fields": [{
                          "dataType": {"type" : "Bigint"}, 
                          "description": "The year in which the data was collected.", 
                          "name": "Year"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "The classification of industries based on the New Zealand Standard Industry Output Classification.", 
                          "name": "Industry_aggregation_NZSIOC"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "Code representing the industry classification according to NZSIOC standards", 
                          "name": "Industry_code_NZSIOC"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "Name of the industry as per the NZSIOC classification", 
                          "name": "Industry_name_NZSIOC"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "The measurement units used for the data values", 
                          "name": "Units"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "Code representing the specific variable being measured", 
                          "name": "Variable_code"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "Name of the variable being measured in the dataset", 
                          "name": "Variable_name"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "Category classification of the variable being measured", 
                          "name": "Variable_category"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "The measured value associated with the industry data", 
                          "name": "Value"
                        },                         {
                          "dataType": {"type" : "String"}, 
                          "description": "The code representing the industry classification according to ANZSIC 2006 standards.", 
                          "name": "Industry_code_ANZSIC06"
                        }], 
            "providerType": "arrow"
          }, 
          separator = ","
        ), 
        filePath = "/datasets/orchestration_datasets/csv/valid/9MB_annual-enterprise-survey-2023-financial-year-provisional.csv"
    )
    send_danger_email = Task(
        task_id = "send_danger_email", 
        component = "Email", 
        body = "This is a dangerous email from sanity pipeline for parent databricks project", 
        subject = "This is a dangerous email from sanity pipeline for parent databricks project", 
        includeData = True, 
        to = ["abhisheks@prophecy.io"], 
        bcc = ["abhisheks+bcc@prophecy.io"], 
        cc = ["abhisheks+cc@prophecy.io"], 
        connection = Connection(kind = "smtp", id = "smtp")
    )
    S3Source_1.out0 >> S3Source_1.input_port_0_1
    S3Source_1.output_port_0_1 >> model_sanity_parent_orchestration_pipeline_1_SQLStatement_1.in_1
    model_sanity_parent_orchestration_pipeline_1_SQLStatement_1.out_1 >> send_danger_email.in0
