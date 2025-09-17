Config = {"config1" : "'Pipeline 1 Config 1 Original'", "config2" : "'Pipeline 1 Config 2 Original'"}
Schedule = Schedule(cron = "0 0/4 * * * ? *", timezone = "Asia/Kolkata")
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Config = Config, Schedule = Schedule, SensorSchedule = SensorSchedule):
    email_hotfix_notification = Task(
        task_id = "email_hotfix_notification", 
        component = "Email", 
        body = "test bb8", 
        subject = "Hotfix pip1", 
        includeData = True, 
        fileName = "test.csv", 
        to = ["yuvraj@prophecy.io"], 
        connection = Connection(kind = "smtp", id = "smtp"), 
        fileFormat = "csv", 
        hasTemplate = False
    )
    pip1__limited_results = Task(
        task_id = "pip1__limited_results", 
        component = "Model", 
        modelName = "pip1__limited_results"
    )
    pip1__limited_results.out_0 >> email_hotfix_notification.in0
